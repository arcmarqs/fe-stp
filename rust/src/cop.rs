use std::env;
use std::env::args;
use std::hash::Hasher;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::Relaxed;

use atlas_common::collections::HashMap;
use atlas_common::error::*;
use intmap::IntMap;
use konst::primitive::parse_usize;
use log4rs::append::Append;
use log4rs::append::console::ConsoleAppender;
use log4rs::append::rolling_file::policy::compound::CompoundPolicy;
use log4rs::append::rolling_file::policy::compound::roll::fixed_window::FixedWindowRoller;
use log4rs::append::rolling_file::policy::compound::trigger::Trigger;
use log4rs::append::rolling_file::{LogFile, RollingFileAppender};
use log4rs::Config;
use log4rs::config::{Appender, Logger, Root};
use log4rs::encode::pattern::PatternEncoder;
use log4rs::filter::threshold::ThresholdFilter;
use log::LevelFilter;
use rand::Rng;

use atlas_client::client::ordered_client::Ordered;
use atlas_client::concurrent_client::ConcurrentClient;
use atlas_common::{async_runtime as rt, channel, init, InitConfig};
use atlas_common::crypto::signature::{KeyPair, PublicKey};
use atlas_common::node_id::NodeId;
use atlas_common::peer_addr::PeerAddr;
use atlas_metrics::{MetricLevel, with_metric_level, with_metrics};

use crate::common::*;
use crate::serialize::Action;

#[derive(Debug)]
pub struct InitTrigger {
    has_been_triggered: AtomicBool,
}

impl Trigger for InitTrigger {
    fn trigger(&self, file: &LogFile) -> anyhow::Result<bool> {
        if file.len_estimate() == 0 {
            println!("Not triggering rollover because file is empty");

            self.has_been_triggered.store(true, Relaxed);
            return Ok(false);
        }

        Ok(self.has_been_triggered.compare_exchange(false, true, Relaxed, Relaxed).is_ok())
    }
}

fn format_old_log(id: u32, str: &str) -> String {
    format!("./logs/log_{}/old/febft{}.log.{}", id, str, "{}")
}

fn format_log(id: u32, str: &str) -> String {
    format!("./logs/log_{}/febft{}.log", id, str)
}

fn policy(id: u32, str: &str) -> CompoundPolicy {
    let trigger = InitTrigger { has_been_triggered: AtomicBool::new(false) };

    let roller = FixedWindowRoller::builder().base(1).build(format_old_log(id, str).as_str(), 5).wrapped(ErrorKind::MsgLog).unwrap();

    CompoundPolicy::new(Box::new(trigger), Box::new(roller))
}

fn file_appender(id: u32, str: &str) -> Box<dyn Append> {
    Box::new(RollingFileAppender::builder()
        .encoder(Box::new(PatternEncoder::new("{l} {d} - {m}{n}")))
        .build(format_log(id, str).as_str(), Box::new(policy(id, str)))
        .wrapped_msg(ErrorKind::MsgLog, "Failed to create rolling file appender").unwrap())
}

fn generate_log(id: u32) {
    let console_appender = ConsoleAppender::builder()
        .encoder(Box::new(PatternEncoder::new("{l} {d} - {m}{n}"))).build();

        let config = Config::builder()
        .appender(Appender::builder().build("comm", file_appender(id, "_comm")))
        .appender(Appender::builder().build("reconfig", file_appender(id, "_reconfig")))
        .appender(Appender::builder().build("common", file_appender(id, "_common")))
        .appender(Appender::builder().build("consensus", file_appender(id, "_consensus")))
        .appender(Appender::builder().build("file", file_appender(id, "")))
        .appender(Appender::builder().build("log_transfer", file_appender(id, "_log_transfer")))
        .appender(Appender::builder().build("state_transfer", file_appender(id, "_state_transfer")))
        .appender(Appender::builder().filter(Box::new(ThresholdFilter::new(LevelFilter::Warn))).build("console", Box::new(console_appender)))

        .logger(Logger::builder().appender("comm").build("atlas_communication", LevelFilter::Warn))
        .logger(Logger::builder().appender("common").build("atlas_common", LevelFilter::Warn))
        .logger(Logger::builder().appender("reconfig").build("atlas_reconfiguration", LevelFilter::Warn))
        .logger(Logger::builder().appender("consensus").build("febft_pbft_consensus", LevelFilter::Warn))
        .logger(Logger::builder().appender("log_transfer").build("atlas_log_transfer", LevelFilter::Warn))
        .logger(Logger::builder().appender("state_transfer").build("febft_state_transfer", LevelFilter::Warn))
        .build(Root::builder().appender("file").build(LevelFilter::Warn), ).wrapped(ErrorKind::MsgLog).unwrap();

}

pub fn main() {
    let is_client = std::env::var("CLIENT").map(|x| x == "1").unwrap_or(false);

    let threadpool_threads = parse_usize(
        std::env::var("THREADPOOL_THREADS")
            .unwrap_or(String::from("2"))
            .as_str(),
    )
    .unwrap();
    let async_threads = parse_usize(
        std::env::var("ASYNC_THREADS")
            .unwrap_or(String::from("2"))
            .as_str(),
    )
    .unwrap();

    if !is_client {
        let id: u32 = std::env::args()
            .nth(1)
            .expect("No replica specified")
            .trim()
            .parse()
            .expect("Expected an integer");

        generate_log(id);

        let conf = InitConfig {
            //If we are the client, we want to have many threads to send stuff to replicas
            threadpool_threads,
            async_threads,
            //If we are the client, we don't want any threads to send to other clients as that will never happen
            id: Some(id.to_string()),
        };

        let _guard = unsafe { init(conf).unwrap() };
        let node_id = NodeId::from(id);

        atlas_metrics::initialize_metrics(
            vec![
                with_metrics(febft_pbft_consensus::bft::metric::metrics()),
                with_metrics(atlas_core::metric::metrics()),
                with_metrics(atlas_communication::metric::metrics()),
                with_metrics(atlas_replica::metric::metrics()),
                with_metrics(atlas_log_transfer::metrics::metrics()),
                with_metrics(febft_state_transfer::metrics::metrics()),
                with_metric_level(MetricLevel::Info),
            ],
            influx_db_config(node_id),
        );

        main_(node_id);
    } else {
        let conf = InitConfig {
            //If we are the client, we want to have many threads to send stuff to replicas
            threadpool_threads,
            async_threads,
            //If we are the client, we don't want any threads to send to other clients as that will never happen
            id: None,
        };

        let _guard = unsafe { init(conf).unwrap() };

        let first_id: u32 = env::var("ID")
            .unwrap_or(String::from("1000"))
            .parse()
            .unwrap();

        atlas_metrics::initialize_metrics(
            vec![
                with_metrics(atlas_communication::metric::metrics()),
                with_metrics(atlas_client::metric::metrics()),
                with_metric_level(MetricLevel::Info),
            ],
            influx_db_config(NodeId::from(first_id)),
        );

        client_async_main();
    }
}

fn main_(id: NodeId) {
    let clients_config = parse_config("./config/clients.config").unwrap();
    let replicas_config = parse_config("./config/replicas.config").unwrap();

    println!("Read configurations.");

    let mut secret_keys: IntMap<KeyPair> = sk_stream()
        .take(replicas_config.len())
        .enumerate()
        .map(|(id, sk)| (id as u64, sk))
        .collect();
    let public_keys: IntMap<PublicKey> = secret_keys
        .iter()
        .map(|(id, sk)| (*id, sk.public_key().into()))
        .collect();

    println!("Read keys.");

    println!("Starting replica {:?}", id);

    let addrs = {
        let mut addrs = IntMap::new();

        for other in &replicas_config {
            let id = NodeId::from(other.id);
            let addr = format!("{}:{}", other.ipaddr, other.portno);
            let replica_addr = format!("{}:{}", other.ipaddr, other.rep_portno.unwrap());

            let otherrep_addr = PeerAddr::new_replica(
                crate::addr!(&other.hostname => addr),
                crate::addr!(&other.hostname => replica_addr),
            );

            addrs.insert(id.into(), otherrep_addr);
        }

        for client in &clients_config {
            let id = NodeId::from(client.id);
            let addr = format!("{}:{}", client.ipaddr, client.portno);

            let replica = PeerAddr::new(crate::addr!(&client.hostname => addr));

            addrs.insert(id.into(), replica);
        }

        addrs
    };

    let sk = secret_keys.remove(id.into()).unwrap();

    println!("Setting up replica...");
    let fut = setup_replica(
        replicas_config.len(),
        id,
        sk,
        addrs,
        public_keys.clone(),
        None,
    );

    let mut replica = rt::block_on(async move {
        println!("Bootstrapping replica #{}", u32::from(id));
        let replica = fut.await.unwrap();
        println!("Running replica #{}", u32::from(id));
        replica
    });

    replica.run().unwrap();
    //We will only launch a single OS monitoring thread since all replicas also run on the same system
    // crate::os_statistics::start_statistics_thread(NodeId(0));

    drop((secret_keys, public_keys, clients_config, replicas_config));
}

fn client_async_main() {
    let clients_config = parse_config("./config/clients.config").unwrap();
    let replicas_config = parse_config("./config/replicas.config").unwrap();

    let first_id: u32 = env::var("ID").unwrap_or(String::from("1000")).parse().unwrap();

    let client_count: u32 = env::var("NUM_CLIENTS").unwrap_or(String::from("1")).parse().unwrap();

    //let client_count = 1;

    let mut secret_keys: IntMap<KeyPair> = sk_stream()
        .take(clients_config.len())
        .enumerate()
        .map(|(id, sk)| (first_id as u64 + id as u64, sk))
        .chain(sk_stream()
            .take(replicas_config.len())
            .enumerate()
            .map(|(id, sk)| (id as u64, sk)))
        .collect();
    let public_keys: IntMap<PublicKey> = secret_keys
        .iter()
        .map(|(id, sk)| (*id, sk.public_key().into()))
        .collect();

    let (tx, mut rx) = channel::new_bounded_async(clients_config.len());

    for i in 0..client_count {
        let id = NodeId::from(i + first_id);

        //generate_log(id.0 );

        let addrs = {
            let mut addrs = IntMap::new();

            for replica in &replicas_config {
                let id = NodeId::from(replica.id);
                let addr = format!("{}:{}", replica.ipaddr, replica.portno);
                let replica_addr = format!("{}:{}", replica.ipaddr, replica.rep_portno.unwrap());

                let replica_p_addr = PeerAddr::new_replica(crate::addr!(&replica.hostname => addr),
                                                           crate::addr!(&replica.hostname => replica_addr));

                addrs.insert(id.into(), replica_p_addr);
            }

            for other in &clients_config {
                let id = NodeId::from(other.id);
                let addr = format!("{}:{}", other.ipaddr, other.portno);

                let client_addr = PeerAddr::new(crate::addr!(&other.hostname => addr));

                addrs.insert(id.into(), client_addr);
            }

            addrs
        };

        let sk = secret_keys.remove(id.into()).unwrap();

        let fut = setup_client(
            replicas_config.len(),
            id,
            sk,
            addrs,
            public_keys.clone(),
            None
        );

        let mut tx = tx.clone();

        rt::spawn(async move {
            println!("Bootstrapping client #{}", u32::from(id));
            let client = fut.await.unwrap();
            println!("Done bootstrapping client #{}", u32::from(id));
            tx.send(client).await.unwrap();
        });
    }

    drop((secret_keys, public_keys, replicas_config));

    let mut clients = Vec::with_capacity(client_count as usize);

    for _i in 0..client_count {
        clients.push(rt::block_on(rx.recv()).unwrap());
    }

    //We have all the clients, start the OS resource monitoring thread
    //crate::os_statistics::start_statistics_thread(NodeId(first_cli));

    let mut handles = Vec::with_capacity(client_count as usize);

    for client in clients {
        let id = client.id();

       // generate_log(id.0);

        let h = std::thread::Builder::new()
            .name(format!("Client {:?}", client.id()))
            .spawn(move || { run_client(client) })
            .expect(format!("Failed to start thread for client {:?} ", &id.id()).as_str());

        handles.push(h);

        // Delay::new(Duration::from_millis(5)).await;
    }

    drop(clients_config);

    for h in handles {
        let _ = h.join();
    }
}

fn sk_stream() -> impl Iterator<Item=KeyPair> {
    std::iter::repeat_with(|| {
        // only valid for ed25519!
        let buf = [0; 32];
        KeyPair::from_bytes(&buf[..]).unwrap()
    })
}

fn run_client(client: SMRClient) {
    let id = client.id();
    println!("run client");
    let concurrent_client = ConcurrentClient::from_client(client, get_concurrent_rqs()).unwrap();
    let mut rand = rand::thread_rng();

    for u  in 0..40000000 as u64 {

        let i : u64 = rand.gen_range(1..100000000000);

        let kv ={
             let id = (id.0 as u64).to_be_bytes().to_vec();
             let rest = u.to_be_bytes().to_vec();

             [id,rest].concat()
        };
    
        let request = if u % 3 == 0 && i % 2 == 0 {
            Action::Remove(i.to_be_bytes().to_vec()) 
        } else if i % 5 == 0 {
            Action::Read(i.to_be_bytes().to_vec()) 
        } else {
            Action::Insert(i.to_be_bytes().to_vec(),kv) 
        };


        println!("{:?} // Sending req {:?}...", id, request);

        if let Ok(reply) = rt::block_on(concurrent_client.update::<Ordered>(Arc::from(request))) {
            println!("state: {:?}", reply);
        }
    }
    
    for u in 0..100000 as u64 {
        let kv = format!("{}{}", id.0.to_string(), u.to_string());
        let request = {Action::Read(kv.into_bytes())};

        println!("{:?} // Sending req {:?}...", id.0, request);

        if let Ok(reply) = rt::block_on(concurrent_client.update::<Ordered>(Arc::from(request))) {
            println!("state: {:?}", reply);
        }
    }
   
}