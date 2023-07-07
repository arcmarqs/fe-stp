use std::time::Duration;

use chrono::{DateTime, Utc};
use atlas_common::node_id::NodeId;


pub fn start_statistics_thread(node_id: NodeId) {
    std::thread::Builder::new()
        .name(String::from("OS Statistics thread"))
        .spawn(move || {
            loop {

                take_cpu_measurement(node_id);
                take_ram_measurement(node_id);

                std::thread::sleep(Duration::from_millis(250));
            }
        }).expect("Failed to launch os stats thread");
}

fn take_ram_measurement(node_id: NodeId) {
    let memory_stats = procinfo::pid::statm_self().unwrap();

    let timestamp = Utc::now().timestamp_millis();

    println!("{:?} // {:?} // RAM Usage {}", node_id, timestamp,
             memory_stats.data
    );
}

fn take_cpu_measurement(node_id: NodeId) {
    let result = mprober_lib::cpu::get_all_cpu_utilization_in_percentage(false,
                                                                         Duration::from_millis(100)).unwrap();

    let mut cpu_res = Vec::with_capacity(result.len());

    let mut curr_cpu = 0;

    for usage in result {
        cpu_res.push((curr_cpu, usage));

        curr_cpu += 1;
    }

    let network_speed = mprober_lib::network::get_networks_with_speed(Duration::from_millis(100)).unwrap();

    let mut tx_speed = 0.0;
    let mut rx_speed = 0.0;

    for (_network, speed) in network_speed {
        //Only capture the most used one.
        if speed.receive > rx_speed || speed.transmit > tx_speed {
            rx_speed = speed.receive;
            tx_speed = speed.transmit;
        }
    }

    let timestamp = Utc::now().timestamp_millis();

    for (cpu_id, usage) in cpu_res {
        println!("{:?} // {:?} // CPU {} utilization: {}", node_id, timestamp, cpu_id, usage);
    }

    println!("{:?} // {:?} // Network TX Usage {} Network RX Usage {}", node_id, timestamp, tx_speed, rx_speed);
}