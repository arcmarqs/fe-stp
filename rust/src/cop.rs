use std::env;
use std::env::args;
use crate::common::*;
use crate::serialize::{MicrobenchmarkData, Request};

use std::fs::File;
use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};

use intmap::IntMap;
use chrono::offset::Utc;
use konst::primitive::{parse_usize};
use nolock::queues::mpsc::jiffy::{
    async_queue,
    AsyncSender,
};
use regex::internal::Inst;

use semaphores::RawSemaphore;
use atlas_client::client::Client;
use atlas_client::client::ordered_client::Ordered;
use atlas_client::concurrent_client::ConcurrentClient;
use atlas_common::crypto::signature::{KeyPair, PublicKey};
use atlas_common::{async_runtime as rt, channel, init, InitConfig};
use atlas_common::node_id::NodeId;
use atlas_communication::tcpip::{PeerAddr};
use atlas_metrics::{MetricLevel, with_metric_level, with_metrics};

pub fn main() {
    let is_client = std::env::var("CLIENT")
        .map(|x| x == "1")
        .unwrap_or(false);

    let threadpool_threads = parse_usize(std::env::var("THREADPOOL_THREADS")
        .unwrap_or(String::from("2")).as_str()).unwrap();
    let async_threads = parse_usize(std::env::var("ASYNC_THREADS")
        .unwrap_or(String::from("2")).as_str()).unwrap();

    if !is_client {
        let replica_id: u32 = std::env::var("ID")
            .iter()
            .flat_map(|id| id.parse())
            .next()
            .unwrap();

        let conf = InitConfig {
            //If we are the client, we want to have many threads to send stuff to replicas
            threadpool_threads,
            async_threads,
            //If we are the client, we don't want any threads to send to other clients as that will never happen
            id: Some(replica_id.to_string()),
        };

        let _guard = unsafe { init(conf).unwrap() };
        let node_id = NodeId::from(replica_id);

        atlas_metrics::initialize_metrics(vec![with_metrics(febft_pbft_consensus::bft::metric::metrics()),
                                               with_metrics(atlas_core::metric::metrics()),
                                               with_metrics(atlas_communication::metric::metrics()),
                                               with_metrics(atlas_replica::metric::metrics()),
                                               with_metrics(atlas_log_transfer::metrics::metrics()),
                                               with_metrics(febft_state_transfer::metrics::metrics()),
                                               with_metric_level(MetricLevel::Info)],
                                          influx_db_config(node_id));

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

        let mut first_id: u32 = env::var("ID").unwrap_or(String::from("1000")).parse().unwrap();

        atlas_metrics::initialize_metrics(vec![with_metrics(atlas_communication::metric::metrics()),
                                               with_metrics(atlas_client::metric::metrics()),
                                               with_metric_level(MetricLevel::Info)],
                                          influx_db_config(NodeId::from(first_id)));

        client_async_main();
    }
}

fn main_(id: NodeId) {
    let mut replica = {
        println!("Started working on the replica");

        //TODO: don't have this hardcoded?
        let first_cli = NodeId::from(1000u32);

        let clients_config = parse_config("./config/clients.config").unwrap();
        let replicas_config = parse_config("./config/replicas.config").unwrap();

        println!("Finished reading replica config.");

        let mut secret_keys: IntMap<KeyPair> = sk_stream()
            .take(clients_config.len())
            .enumerate()
            .map(|(id, sk)| (u64::from(first_cli) + id as u64, sk))
            .chain(sk_stream()
                .take(replicas_config.len())
                .enumerate()
                .map(|(id, sk)| (id as u64, sk)))
            .collect();

        let public_keys: IntMap<PublicKey> = secret_keys
            .iter()
            .map(|(id, sk)| (*id, sk.public_key().into()))
            .collect();

        println!("Finished reading keys.");

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

        /* let comm_stats = Some(Arc::new(CommStats::new(id,
                                                 first_cli,
                                                 MicrobenchmarkData::MEASUREMENT_INTERVAL))); */

        let comm_stats = None;

        let sk = secret_keys.remove(id.into()).unwrap();

        let fut = setup_replica(
            replicas_config.len(),
            id,
            sk,
            addrs,
            public_keys.clone(),
            comm_stats,
        );

        println!("Bootstrapping replica #{}", u32::from(id));
        let replica = rt::block_on(fut).unwrap();
        println!("Running replica #{}", u32::from(id));

        //Here we want to launch a statistics thread for each replica since they are on different machines
        //crate::os_statistics::start_statistics_thread(id);

        replica
    };

    // run forever
    replica.run().unwrap();
}

fn client_async_main() {
    let clients_config = parse_config("./config/clients.config").unwrap();
    let replicas_config = parse_config("./config/replicas.config").unwrap();

    let arg_vec: Vec<String> = args().collect();

    let default = String::from("1000");

    println!("arg_vec: {:?}", arg_vec);

    let mut first_id: u32 = arg_vec.last().unwrap_or(&default).parse().unwrap();

    //let client_count: u32 = env::var("NUM_CLIENTS").unwrap_or(String::from("1")).parse().unwrap();

    let client_count = 1;

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

    let (tx, mut rx) = channel::new_bounded_async(8);

    let comm_stats = None;

    for i in 0..client_count {
        let id = NodeId::from(first_id + i);

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
            comm_stats.clone(),
        );

        let mut tx = tx.clone();
        rt::spawn(async move {
            //We can have this async initializer no problem, just don't want it to be used to actually send
            //The requests/control the clients
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

    let mut handles = Vec::with_capacity(client_count as usize);

    // Get one client to run on this thread
    let our_client = clients.pop();

    for client in clients {
        let id = client.id();

        let h = std::thread::Builder::new()
            .name(format!("Client {:?}", client.id()))
            .spawn(move || { run_client(client) })
            .expect(format!("Failed to start thread for client {:?} ", &id.id()).as_str());

        handles.push(h);
    }

    drop(clients_config);

    run_client(our_client.unwrap());

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

fn run_client(mut client: Client<MicrobenchmarkData, ClientNetworking>) {
    let concurrent_rqs: usize = get_concurrent_rqs();

    let id = u32::from(client.id());

    println!("Warm up...");

    let request = Arc::new({
        let mut r = vec![0; MicrobenchmarkData::REQUEST_SIZE];
        r
    });

    let concurrent_client = ConcurrentClient::from_client(client, concurrent_rqs).unwrap();

    let semaphore = Arc::new(RawSemaphore::new(concurrent_rqs));

    let iterator = 0..(MicrobenchmarkData::OPS_NUMBER / 2);

    let mut ramp_up = 1000;

    for req in iterator {

        //Only allow concurrent_rqs per client at the network
        semaphore.acquire();

        if MicrobenchmarkData::VERBOSE {
            println!("{:?} // Sending req {}...", concurrent_client.id(), req);
        }

        let sem_clone = semaphore.clone();

        concurrent_client.update_callback::<Ordered>(Request::new(MicrobenchmarkData::REQUEST), Box::new(move |reply| {

            //Release another request for this client
            sem_clone.release();

            if MicrobenchmarkData::VERBOSE {
                if req % 1000 == 0 {
                    println!("{} // {} operations sent!", id, req);
                }

                println!(" sent!");
            }
        })).unwrap();

        if MicrobenchmarkData::REQUEST_SLEEP_MILLIS != Duration::ZERO {
            std::thread::sleep(MicrobenchmarkData::REQUEST_SLEEP_MILLIS);
        } else if ramp_up > 0 {
            let to_sleep = fastrand::u32(ramp_up / 2..ramp_up);
            std::thread::sleep(Duration::from_millis(to_sleep as u64));

            ramp_up -= 100;
        }
    }

    println!("Executing experiment for {} ops", MicrobenchmarkData::OPS_NUMBER / 2);

    let iterator = 0..(MicrobenchmarkData::OPS_NUMBER / 2);

    //let mut st = BenchmarkHelper::new(client.id(), MicrobenchmarkData::OPS_NUMBER / 2);

    let start = Instant::now();

    for req in iterator {
        semaphore.acquire();

        if MicrobenchmarkData::VERBOSE {
            print!("Sending req {}...", req);
        }

        let last_send_instant = Utc::now();

        let sem_clone = semaphore.clone();

        concurrent_client.update_callback::<Ordered>(Request::new(MicrobenchmarkData::REQUEST),
                                                     Box::new(move |reply| {
                                                         //Release another request for this client
                                                         sem_clone.release();

                                                         if MicrobenchmarkData::VERBOSE {
                                                             if req % 1000 == 0 {
                                                                 println!("{} // {} operations sent!", id, req);
                                                             }

                                                             println!(" sent!");
                                                         }
                                                     })).unwrap();

        if MicrobenchmarkData::REQUEST_SLEEP_MILLIS != Duration::ZERO {
            std::thread::sleep(MicrobenchmarkData::REQUEST_SLEEP_MILLIS);
        }
    }

    //Wait for all requests to finish
    for _ in 0..concurrent_rqs {
        semaphore.acquire();
    }

    let time_passed = start.elapsed();

    let ops_done = MicrobenchmarkData::OPS_NUMBER / 2;

    println!("{:?} // Done.", concurrent_client.id());

    println!("{:?} // Test done in {:?}. ({} ops/s)", concurrent_client.id(), time_passed, (ops_done * 1_000_000) / time_passed.as_micros() as usize );
}
