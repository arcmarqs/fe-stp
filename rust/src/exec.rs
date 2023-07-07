use std::sync::{Arc, Weak};

use chrono::DateTime;
use chrono::offset::Utc;
use atlas_common::error::*;
use atlas_common::node_id::NodeId;
use atlas_execution::app::{Application, BatchReplies, Reply, Request, UpdateBatch};
use atlas_metrics::benchmarks::{BenchmarkHelperStore, Measurements};

use crate::serialize;

use crate::serialize::{MicrobenchmarkData, State};

pub struct Microbenchmark {
    id: NodeId,
    max_tp: f32,
    batch_count: f32,
    max_tp_time: DateTime<Utc>,
    last_measurement: usize,
    iterations: usize,
    reply: Arc<Vec<u8>>,
    measurements: Measurements,
}

impl Microbenchmark {
    pub fn new(id: NodeId) -> Self {
        let reply = Arc::new((0..)
            .into_iter()
            .take(MicrobenchmarkData::REPLY_SIZE)
            .map(|x| (x & 0xff) as u8)
            .collect());

        Self {
            id: id.clone(),
            reply,
            max_tp: -1.0,
            batch_count: 0.0,
            max_tp_time: Utc::now(),
            last_measurement: 0,
            iterations: 0,
            measurements: Measurements::new(id),
        }
    }
}

impl Application<State> for Microbenchmark {
    type AppData = MicrobenchmarkData;

    fn initial_state() -> Result<serialize::State> {
        Ok(serialize::State::new(MicrobenchmarkData::STATE))
    }

    fn unordered_execution(&self, state: &State, request: Request<Self, State>) -> Reply<Self, State> {
        todo!()
    }

    fn update(&mut self, state: &mut State, request: Request<Self, State>) -> Reply<Self, State> {

        let reply = serialize::Reply::new(MicrobenchmarkData::REPLY);

        // increase iter count
        self.iterations += 1;

        if self.iterations % MicrobenchmarkData::MEASUREMENT_INTERVAL == 0 {
            println!("{:?} // --- Measurements after {} ops ({} samples) ---",
                     self.id, self.iterations, MicrobenchmarkData::MEASUREMENT_INTERVAL);

            let diff = Utc::now()
                .signed_duration_since(self.max_tp_time)
                .num_microseconds().expect("Need micro seconds");


            let tp = ((self.iterations - self.last_measurement) as f32 * 1000.0 * 1000.0) / (diff as f32);

            if tp > self.max_tp {
                self.max_tp = tp;
            }

            self.last_measurement = self.iterations;

            println!("{:?} // Throughput = {} operations/sec (Maximum observed: {} ops/sec)",
                     self.id, tp, self.max_tp);

            //This gives us the amount of batches per micro seconds, since the diff is in microseconds
            let mut batches_per_second = self.batch_count / diff as f32;

            //This moves the amount of batches per microsecond to the amount of batches per second
            batches_per_second *= 1000.0 * 1000.0;

            println!("{:?} // Batch throughput = {} batches/sec",
                     self.id, batches_per_second);

            //Reset the amount of batches
            self.batch_count = 0.0;

            self.measurements.total_latency.log_latency("Total");
            self.measurements.consensus_latency.log_latency("Consensus");
            self.measurements.pre_cons_latency.log_latency("Pre-consensus");
            self.measurements.pos_cons_latency.log_latency("Pos-consensus");
            self.measurements.pre_prepare_latency.log_latency("Propose / PrePrepare");
            self.measurements.prepare_latency.log_latency("Write / Prepare");
            self.measurements.commit_latency.log_latency("Accept / Commit");
            self.measurements.prepare_msg_latency.log_latency("Prepare msg");
            self.measurements.propose_time_latency.log_latency("Propose time");
            self.measurements.prepare_time_taken.log_latency("Prepare time taken");
            self.measurements.commit_time_taken.log_latency("Commit time taken");
            self.measurements.batch_size.log_batch();

            self.max_tp_time = Utc::now();
        }

        reply
    }


    fn update_batch(
        &mut self,
        _state: &mut State,
        mut batch: UpdateBatch<serialize::Request>,
    ) -> BatchReplies<serialize::Reply> {
        let batch_len = batch.len();

        let meta = batch.take_meta();

        let mut reply_batch = BatchReplies::with_capacity(batch.len());

        for update in batch.into_inner() {
            let (peer_id, sess, opid, _req) = update.into_inner();
            reply_batch.add(peer_id, sess, opid, serialize::Reply::new(MicrobenchmarkData::REPLY));
        }

        self.batch_count += 1.0;

        if let Some(mut meta) = meta {
            meta.execution_time = Utc::now();
            // take measurements
            meta.batch_size.store(&mut self.measurements.batch_size);
            (meta.consensus_decision_time, meta.consensus_start_time).store(&mut self.measurements.consensus_latency);
            (meta.reception_time, meta.consensus_start_time).store(&mut self.measurements.pre_cons_latency);
            (meta.execution_time, meta.consensus_decision_time).store(&mut self.measurements.pos_cons_latency);
            (meta.prepare_sent_time, meta.consensus_start_time).store(&mut self.measurements.pre_prepare_latency);
            (meta.commit_sent_time, meta.prepare_sent_time).store(&mut self.measurements.prepare_latency);
            (meta.consensus_decision_time, meta.commit_sent_time).store(&mut self.measurements.commit_latency);
            (meta.prepare_sent_time, meta.pre_prepare_received_time).store(&mut self.measurements.prepare_msg_latency);
            (meta.done_propose, meta.started_propose).store(&mut self.measurements.propose_time_latency);
            (meta.commit_sent_time, meta.first_prepare_received).store(&mut self.measurements.prepare_time_taken);
            (meta.consensus_decision_time, meta.first_commit_received).store(&mut self.measurements.commit_time_taken);
        }

        for _ in 0..batch_len {
            // increase iter count
            self.iterations += 1;

            if self.iterations % MicrobenchmarkData::MEASUREMENT_INTERVAL == 0 && batch_len < MicrobenchmarkData::MEASUREMENT_INTERVAL {
                println!("{:?} // --- Measurements after {} ops ({} samples) ---",
                         self.id, self.iterations, MicrobenchmarkData::MEASUREMENT_INTERVAL);

                let diff = Utc::now()
                    .signed_duration_since(self.max_tp_time)
                    .num_microseconds().expect("Need micro seconds");


                let tp = ((self.iterations - self.last_measurement) as f32 * 1000.0 * 1000.0) / (diff as f32);

                if tp > self.max_tp {
                    self.max_tp = tp;
                }

                self.last_measurement = self.iterations;

                println!("{:?} // Throughput = {} operations/sec (Maximum observed: {} ops/sec)",
                         self.id, tp, self.max_tp);

                //This gives us the amount of batches per micro seconds, since the diff is in microseconds
                let mut batches_per_second = self.batch_count / diff as f32;

                //This moves the amount of batches per microsecond to the amount of batches per second
                batches_per_second *= 1000.0 * 1000.0;

                println!("{:?} // Batch throughput = {} batches/sec",
                         self.id, batches_per_second);

                //Reset the amount of batches
                self.batch_count = 0.0;

                self.measurements.total_latency.log_latency("Total");
                self.measurements.consensus_latency.log_latency("Consensus");
                self.measurements.pre_cons_latency.log_latency("Pre-consensus");
                self.measurements.pos_cons_latency.log_latency("Pos-consensus");
                self.measurements.pre_prepare_latency.log_latency("Propose / PrePrepare");
                self.measurements.prepare_latency.log_latency("Write / Prepare");
                self.measurements.commit_latency.log_latency("Accept / Commit");
                self.measurements.prepare_msg_latency.log_latency("Prepare msg");
                self.measurements.propose_time_latency.log_latency("Propose time");
                self.measurements.prepare_time_taken.log_latency("Prepare time taken");
                self.measurements.commit_time_taken.log_latency("Commit time taken");
                self.measurements.batch_size.log_batch();

                self.max_tp_time = Utc::now();
            }

        }

        reply_batch
    }
}