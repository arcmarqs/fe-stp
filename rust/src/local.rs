use std::env;
use std::fs::File;
use std::io::Write;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Barrier};
use std::sync::atomic::Ordering::Relaxed;

use atlas_client::concurrent_client::ConcurrentClient;
use atlas_common::collections::HashMap;
use atlas_common::peer_addr::PeerAddr;

use intmap::IntMap;
use nolock::queues::mpsc::jiffy::{async_queue, AsyncSender};

use atlas_client::client::ordered_client::Ordered;
use atlas_common::crypto::signature::{KeyPair, PublicKey};
use atlas_common::error::*;
use atlas_common::node_id::NodeId;
use atlas_common::{async_runtime as rt, channel, init, InitConfig};
use log::LevelFilter;
use log4rs::append::rolling_file::policy::compound::roll::fixed_window::FixedWindowRoller;
use log4rs::append::rolling_file::policy::compound::trigger::Trigger;
use log4rs::append::rolling_file::policy::compound::CompoundPolicy;
use log4rs::append::rolling_file::{LogFile, RollingFileAppender};
use log4rs::append::Append;
use log4rs::config::{Appender, Logger, Root};
use log4rs::encode::pattern::PatternEncoder;
use log4rs::Config;

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

        Ok(self
            .has_been_triggered
            .compare_exchange(false, true, Relaxed, Relaxed)
            .is_ok())
    }
}

fn format_old_log(id: u32, str: &str) -> String {
    format!("./logs/log_{}/old/febft{}.log.{}", id, str, "{}")
}

fn format_log(id: u32, str: &str) -> String {
    format!("./logs/log_{}/febft{}.log", id, str)
}

fn policy(id: u32, str: &str) -> CompoundPolicy {
    let trigger = InitTrigger {
        has_been_triggered: AtomicBool::new(false),
    };

    let roller = FixedWindowRoller::builder()
        .base(1)
        .build(format_old_log(id, str).as_str(), 5)
        .wrapped(ErrorKind::MsgLog)
        .unwrap();

    CompoundPolicy::new(Box::new(trigger), Box::new(roller))
}

fn file_appender(id: u32, str: &str) -> Box<dyn Append> {
    Box::new(
        RollingFileAppender::builder()
            .encoder(Box::new(PatternEncoder::new("{l} {d} - {m}{n}")))
            .build(format_log(id, str).as_str(), Box::new(policy(id, str)))
            .wrapped_msg(ErrorKind::MsgLog, "Failed to create rolling file appender")
            .unwrap(),
    )
}

pub fn main() {
    let is_client = std::env::var("CLIENT").map(|x| x == "1").unwrap_or(false);

    let single_server = std::env::var("SINGLE_SERVER")
        .map(|x| x == "1")
        .unwrap_or(false);


        let id: u32 = std::env::var("ID")
        .iter()
        .flat_map(|id| id.parse())
        .next()
        .unwrap();

    let config = Config::builder()
        .appender(Appender::builder().build("comm", file_appender(id, "_comm")))
        .appender(Appender::builder().build("reconfig", file_appender(id, "_reconfig")))
        .appender(Appender::builder().build("consensus", file_appender(id, "_consensus")))
        .appender(Appender::builder().build("file", file_appender(id, "")))
        .logger(
            Logger::builder()
                .appender("comm")
                .build("atlas_communication", LevelFilter::Warn),
        )
        .logger(
            Logger::builder()
                .appender("reconfig")
                .build("atlas_reconfiguration", LevelFilter::Debug),
        )
        .logger(
            Logger::builder()
                .appender("consensus")
                .build("febft_pbft_consensus", LevelFilter::Trace),
        )
        .build(
            Root::builder()
                .appender("file")
                .build(LevelFilter::Debug),
        )
        .wrapped(ErrorKind::MsgLog)
        .unwrap();

    let _handle = log4rs::init_config(config)
        .wrapped(ErrorKind::MsgLog)
        .unwrap();

    let conf = InitConfig {
        threadpool_threads: 5,
        async_threads: num_cpus::get() / 1,
        id: None,
    };

    let _guard = unsafe { init(conf).unwrap() };

  

    println!("Starting...");

    if !is_client {
        if !single_server {
            main_();
        } else {
            run_single_server();
        }
    } else {
        client_async_main();
    }
}

fn main_() {
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

    let mut pending_threads = Vec::with_capacity(4);

    let barrier = Arc::new(Barrier::new(replicas_config.len()));

    for replica in &replicas_config {
        let barrier = barrier.clone();
        let id = NodeId::from(replica.id);

        println!("Starting replica {:?}", id);

        let addrs = {
            let mut addrs = IntMap::new();

            for other in &replicas_config {
                let id = NodeId::from(other.id);
                let addr = format!("{}:{}", other.ipaddr, other.portno);
                let replica_addr = format!("{}:{}", other.ipaddr, other.rep_portno.unwrap());

                let client_addr = PeerAddr::new_replica(
                    crate::addr!(&other.hostname => addr),
                    crate::addr!(&other.hostname => replica_addr),
                );

                addrs.insert(id.into(), client_addr);
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

        let thread = std::thread::Builder::new()
            .name(format!("Node {:?}", id))
            .spawn(move || {
                let mut replica = rt::block_on(async move {
                    println!("Bootstrapping replica #{}", u32::from(id));
                    let mut replica = fut.await.unwrap();
                    println!("Running replica #{}", u32::from(id));
                    replica
                });

                barrier.wait();

                println!("{:?} // Let's go", id);

                barrier.wait();

                replica.run().unwrap();
            })
            .unwrap();

        pending_threads.push(thread);
    }

    //We will only launch a single OS monitoring thread since all replicas also run on the same system
    //crate::os_statistics::start_statistics_thread(NodeId(0));

    drop((secret_keys, public_keys, clients_config, replicas_config));

    // run forever
    for mut x in pending_threads {
        x.join();
    }
}

fn run_single_server() {
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

    let replica_id: usize = std::env::args()
        .nth(1)
        .expect("No replica specified")
        .trim()
        .parse()
        .expect("Expected an integer");

    let replica = &replicas_config[replica_id];

    let id = NodeId::from(replica.id);

    println!("Starting replica {:?}", id);

    let addrs = {
        let mut addrs = IntMap::new();

        for other in &replicas_config {
            let id = NodeId::from(other.id);
            let addr = format!("{}:{}", other.ipaddr, other.portno);
            let replica_addr = format!("{}:{}", other.ipaddr, other.rep_portno.unwrap());

            let client_addr = PeerAddr::new_replica(
                crate::addr!(&other.hostname => addr),
                crate::addr!(&other.hostname => replica_addr),
            );

            addrs.insert(id.into(), client_addr);
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

    let mut first_id: u32 = env::var("ID")
        .unwrap_or(String::from("1000"))
        .parse()
        .unwrap();

    let client_count: u32 = env::var("NUM_CLIENTS")
        .unwrap_or(String::from("1"))
        .parse()
        .unwrap();

    let mut secret_keys: IntMap<KeyPair> = sk_stream()
        .take(clients_config.len())
        .enumerate()
        .map(|(id, sk)| (first_id as u64 + id as u64, sk))
        .chain(
            sk_stream()
                .take(replicas_config.len())
                .enumerate()
                .map(|(id, sk)| (id as u64, sk)),
        )
        .collect();
    let public_keys: IntMap<PublicKey> = secret_keys
        .iter()
        .map(|(id, sk)| (*id, sk.public_key().into()))
        .collect();

    let (tx, mut rx) = channel::new_bounded_async(clients_config.len());

    for i in 0..client_count {
        let id = NodeId::from(i + first_id);

        let addrs = {
            let mut addrs = IntMap::new();

            for replica in &replicas_config {
                let id = NodeId::from(replica.id);
                let addr = format!("{}:{}", replica.ipaddr, replica.portno);
                let replica_addr = format!("{}:{}", replica.ipaddr, replica.rep_portno.unwrap());

                let replica_p_addr = PeerAddr::new_replica(
                    crate::addr!(&replica.hostname => addr),
                    crate::addr!(&replica.hostname => replica_addr),
                );

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
            None,
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

    let (mut queue, queue_tx) = async_queue();
    let queue_tx = Arc::new(queue_tx);

    let mut clients = Vec::with_capacity(client_count as usize);

    for _i in 0..client_count {
        clients.push(rt::block_on(rx.recv()).unwrap());
    }

    //We have all the clients, start the OS resource monitoring thread
    //crate::os_statistics::start_statistics_thread(NodeId(first_cli));

    let mut handles = Vec::with_capacity(client_count as usize);

    for client in clients {
        let queue_tx = Arc::clone(&queue_tx);
        let id = client.id();

        let h = std::thread::Builder::new()
            .name(format!("Client {:?}", client.id()))
            .spawn(move || run_client(client, queue_tx))
            .expect(format!("Failed to start thread for client {:?} ", &id.id()).as_str());

        handles.push(h);

        // Delay::new(Duration::from_millis(5)).await;
    }

    drop(clients_config);

    for mut h in handles {
        let _ = h.join();
    }

    let mut file = File::create("./latencies.out").unwrap();

    while let Ok(line) = queue.try_dequeue() {
        file.write_all(line.as_ref()).unwrap();
    }

    file.flush().unwrap();
}

fn sk_stream() -> impl Iterator<Item = KeyPair> {
    std::iter::repeat_with(|| {
        // only valid for ed25519!
        let buf = [0; 32];
        KeyPair::from_bytes(&buf[..]).unwrap()
    })
}

fn run_client(client: SMRClient, _q: Arc<AsyncSender<String>>) {
    let id = client.id();
    println!("run client");
    let concurrent_client = ConcurrentClient::from_client(client, get_concurrent_rqs()).unwrap();
    

    for u in 0..400 {

        let kv = format!("{}{}", id.0.to_string(), u.to_string());

        let mut map: HashMap<String,Vec<u8>> = HashMap::default();

        map.insert(id.0.to_string(),kv.as_bytes().to_vec());

        let request = Action::Insert(kv, map);
        println!("{:?} // Sending req {:?}...", id.0, request);

        if let Ok(reply) = rt::block_on(concurrent_client.update::<Ordered>(Arc::from(request))) {
            println!("state: {:?}", reply);
        }
    }

    for u in 0..400 {
        let kv = format!("{}{}", id.0.to_string(), u.to_string());
        let request = {Action::Read(kv)};

        println!("{:?} // Sending req {:?}...", id.0, request);

        if let Ok(reply) = rt::block_on(concurrent_client.update::<Ordered>(Arc::from(request))) {
            println!("state: {:?}", reply);
        }
    }


   
}
