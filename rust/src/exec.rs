use std::sync::{Arc, Weak};

use atlas_divisible_state::state_orchestrator::{StateOrchestrator, self};
use atlas_execution::state::divisible_state::DivisibleState;
use chrono::DateTime;
use chrono::offset::Utc;
use atlas_common::error::*;
use atlas_common::node_id::NodeId;
use atlas_execution::app::{Application, BatchReplies, Reply, Request, UpdateBatch};
use atlas_metrics::benchmarks::{BenchmarkHelperStore, Measurements};

use crate::serialize;

use crate::serialize::KvData;

pub struct KVApp;

impl Application<StateOrchestrator> for KVApp {
   type AppData = KvData;

fn initial_state() -> Result<StateOrchestrator> {
    // create the state
    let state = StateOrchestrator::new("../database_NODE", );

    //launch the monitoring system
    state_orchestrator::monitor_changes(state.descriptor.clone(), state.get_subscriber());

    Ok(state)
}

fn unordered_execution(&self, state: &StateOrchestrator, request: Request<Self, StateOrchestrator>) -> Reply<Self, StateOrchestrator> {
        todo!()
    }

fn update(&mut self, state: &mut StateOrchestrator, request: Request<Self, StateOrchestrator>) -> Reply<Self, StateOrchestrator> {
    match request.as_ref() {
        serialize::Action::Get(_) => todo!(),
        serialize::Action::Set(_, _) => todo!(),
        serialize::Action::Remove(_) => todo!(),
    }
    
    self.1 += 1;
    let reply = Arc::new(*state);

    reply
}
}