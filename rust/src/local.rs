use std::env;
use std::fs::File;
use std::io::Write;
use std::sync::{Arc, Barrier};
use std::time::Duration;

use chrono::offset::Utc;
use futures_timer::Delay;
use intmap::IntMap;
use nolock::queues::mpsc::jiffy::{
    async_queue,
    AsyncSender,
};
use rand_core::{OsRng, RngCore};
use semaphores::RawSemaphore;

use febft_pbft_consensus::bft::{PBFT};
use atlas_client::client::Client;
use atlas_client::client::ordered_client::Ordered;
use atlas_common::crypto::signature::{KeyPair, PublicKey};
use atlas_common::{async_runtime as rt, channel, init, InitConfig};
use atlas_common::node_id::NodeId;
use atlas_communication::tcpip::{PeerAddr, TcpNode};

use crate::common::*;
use crate::serialize::{MicrobenchmarkData, Request};

pub fn main() {
    let is_client = std::env::var("CLIENT")
        .map(|x| x == "1")
        .unwrap_or(false);

    let conf = InitConfig {
        threadpool_threads: 5,
        async_threads: 2,
        id: None
    };

    let _guard = unsafe { init(conf).unwrap() };

    println!("Starting...");

    if !is_client {
        main_();
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

                let client_addr = PeerAddr::new_replica(crate::addr!(&other.hostname => addr),
                                                        crate::addr!(&other.hostname => replica_addr));

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
            None
        );

        let thread = std::thread::Builder::new().name(format!("Node {:?}", id)).spawn(move || {
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
        }).unwrap();

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

fn client_async_main() {
    let clients_config = parse_config("./config/clients.config").unwrap();
    let replicas_config = parse_config("./config/replicas.config").unwrap();

    let mut first_id: u32 = env::var("ID").unwrap_or(String::from("1000")).parse().unwrap();

    let client_count: u32 = env::var("NUM_CLIENTS").unwrap_or(String::from("1")).parse().unwrap();

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
            .spawn(move || { run_client(client, queue_tx) })
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

fn sk_stream() -> impl Iterator<Item=KeyPair> {
    std::iter::repeat_with(|| {
        // only valid for ed25519!
        let buf = [0; 32];
        KeyPair::from_bytes(&buf[..]).unwrap()
    })
}

fn run_client(mut client: Client<MicrobenchmarkData, ClientNetworking>, q: Arc<AsyncSender<String>>) {
    let concurrent_rqs: usize = get_concurrent_rqs();

    let id = u32::from(client.id());

    println!("Warm up...");

    let request = Arc::new({
        let mut r = vec![0; MicrobenchmarkData::REQUEST_SIZE];
        r
    });

    let semaphore = Arc::new(RawSemaphore::new(concurrent_rqs));

    let iterator = 0..(MicrobenchmarkData::OPS_NUMBER / 2);

    let mut ramp_up = 1000;

    for req in iterator {

        //Only allow concurrent_rqs per client at the network
        semaphore.acquire();

        if MicrobenchmarkData::VERBOSE {
            println!("{:?} // Sending req {}...", client.id(), req);
        }

        let last_send_instant = Utc::now();

        let sem_clone = semaphore.clone();

        let q = q.clone();

        client.clone().update_callback::<Ordered>(Request::new(MicrobenchmarkData::REQUEST), Box::new(move |reply| {

            //Release another request for this client
            sem_clone.release();

            let latency = Utc::now()
                .signed_duration_since(last_send_instant)
                .num_nanoseconds()
                .unwrap_or(i64::MAX);

            let time_ms = Utc::now().timestamp_millis();

            /*let _ = q.enqueue(
                format!(
                    "{}\t{}\t{}\n",
                    id,
                    time_ms,
                    latency,
                ),
            );*/

            if MicrobenchmarkData::VERBOSE {
                if req % 1000 == 0 {
                    println!("{} // {} operations sent!", id, req);
                }

                println!(" sent!");
            }
        }));

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

    for req in iterator {
        semaphore.acquire();

        if MicrobenchmarkData::VERBOSE {
            print!("Sending req {}...", req);
        }

        let q = q.clone();

        let last_send_instant = Utc::now();

        let sem_clone = semaphore.clone();

        client.clone().update_callback::<Ordered>(Request::new(MicrobenchmarkData::REQUEST),
                                                  Box::new(move |reply| {

                                                      //Release another request for this client
                                                      sem_clone.release();

                                                      /* let latency = Utc::now()
                                                          .signed_duration_since(last_send_instant)
                                                          .num_nanoseconds()
                                                          .unwrap_or(i64::MAX);

                                                      let time_ms = Utc::now().timestamp_millis();*/

                                                      /*let _ = q.enqueue(
                                                          format!(
                                                              "{}\t{}\t{}\n",
                                                              id,
                                                              time_ms,
                                                              latency,
                                                          ),
                                                      );*/

                                                      //(exec_time, last_send_instant).store(st);

                                                      if MicrobenchmarkData::VERBOSE {
                                                          if req % 1000 == 0 {
                                                              println!("{} // {} operations sent!", id, req);
                                                          }

                                                          println!(" sent!");
                                                      }
                                                  }));

        if MicrobenchmarkData::REQUEST_SLEEP_MILLIS != Duration::ZERO {
            std::thread::sleep(MicrobenchmarkData::REQUEST_SLEEP_MILLIS);
        }
    }

    //Wait for all requests to finish
    for _ in 0..concurrent_rqs {
        semaphore.acquire();
    }

    println!("{:?} // Done.", client.id());

    /*
    if id == 1000 {
        println!("{} // Average time for {} executions (-10%) = {} us",
                 id,
                 MicrobenchmarkData::OPS_NUMBER / 2,
                 st.average(true, false) / 1000.0);

        println!("{} // Standard deviation for {} executions (-10%) = {} us",
                 id,
                 MicrobenchmarkData::OPS_NUMBER / 2,
                 st.standard_deviation(true, true) / 1000.0);

        println!("{} // Average time for {} executions (all samples) = {} us",
                 id,
                 MicrobenchmarkData::OPS_NUMBER / 2,
                 st.average(false, true) / 1000.0);

        println!("{} // Standard deviation for {} executions (all samples) = {} us",
                 id,
                 MicrobenchmarkData::OPS_NUMBER / 2,
                 st.standard_deviation(false, true) / 1000.0);

        println!("{} // Maximum time for {} executions (all samples) = {} us",
                 id,
                 MicrobenchmarkData::OPS_NUMBER / 2,
                 st.max(false) / 1000);
    }
    */
}
