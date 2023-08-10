use std::ops::Deref;
use std::sync::{Arc, Weak};

use atlas_common::async_runtime::spawn;
use atlas_divisible_state::state_orchestrator::{StateOrchestrator, self, monitor_changes};
use atlas_execution::state::divisible_state::DivisibleState;
use chrono::DateTime;
use chrono::offset::Utc;
use atlas_common::error::*;
use atlas_common::node_id::NodeId;
use atlas_execution::app::{Application, BatchReplies, Reply, Request, UpdateBatch};
use atlas_metrics::benchmarks::{BenchmarkHelperStore, Measurements};

use crate::serialize;

use crate::serialize::KvData;

#[derive(Default)]
pub struct KVApp;

impl Application<StateOrchestrator> for KVApp {
   type AppData = KvData;

fn initial_state() -> Result<StateOrchestrator> {
    // create the state

    let id: u32 = std::env::args()
    .nth(1).expect("No replica specified")
    .trim().parse().expect("Expected an integer");

    let path = format!("{}{}", "./appdata_",id);

    let state = StateOrchestrator::new(&path);   

    let _ = spawn(
        monitor_changes(
            state.updates.clone(),
            state.get_subscriber()));
            
    Ok(state)
}

fn unordered_execution(&self, state: &StateOrchestrator, request: Request<Self, StateOrchestrator>) -> Reply<Self, StateOrchestrator> {
        todo!()
    }

fn update(&mut self, state: &mut StateOrchestrator, request: Request<Self, StateOrchestrator>) -> Reply<Self, StateOrchestrator> {

       let ivec =  match request.as_ref() {
        serialize::Action::Get(k) => state.db.get(k),
        serialize::Action::Set(k, v) => state.db.insert(k, v),
        serialize::Action::Remove(k) => state.db.remove(k),
    }.unwrap();


        let reply_inner = ivec.map(|x| String::from_utf8(x.to_vec()).unwrap());
        println!("request {:?} reply {:?}",request, reply_inner);

        Arc::new(reply_inner)
    }

fn unordered_batched_execution(
        &self,
        state: &StateOrchestrator,
        requests: atlas_execution::app::UnorderedBatch<Request<Self, StateOrchestrator>>,
    ) -> BatchReplies<Reply<Self, StateOrchestrator>> {
        let mut reply_batch = BatchReplies::with_capacity(requests.len());

        for unordered_req in requests.into_inner() {
            let (peer_id, sess, opid, req) = unordered_req.into_inner();
            let reply = self.unordered_execution(&state, req);
            reply_batch.add(peer_id, sess, opid, reply);
        }

        reply_batch
    }

fn update_batch(
        &mut self,
        state: &mut StateOrchestrator,
        batch: UpdateBatch<Request<Self, StateOrchestrator>>,
    ) -> BatchReplies<Reply<Self, StateOrchestrator>> {
        let mut reply_batch = BatchReplies::with_capacity(batch.len());

        for update in batch.into_inner() {
            let (peer_id, sess, opid, req) = update.into_inner();
            let reply = self.update(state, req);
            reply_batch.add(peer_id, sess, opid, reply);
        }

        reply_batch
    }


}