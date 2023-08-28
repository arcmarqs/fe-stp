use std::ops::Deref;
use std::sync::{Arc, Weak};

use atlas_common::async_runtime::spawn;
use atlas_common::collections::HashMap;
use atlas_execution::state::divisible_state::DivisibleState;
use chrono::DateTime;
use chrono::offset::Utc;
use atlas_common::error::*;
use atlas_common::ordering::SeqNo;
use atlas_common::node_id::NodeId;
use atlas_execution::app::{Application, BatchReplies, Reply, Request, UpdateBatch};
use atlas_metrics::benchmarks::{BenchmarkHelperStore, Measurements};

use crate::serialize::{self, State};

use crate::serialize::KvData;

#[derive(Default)]
pub struct KVApp;

impl Application<State> for KVApp {
    type AppData = KvData;

fn initial_state() -> Result<State> {
 
    let state = State::new();   
            
    Ok(state)
}

fn unordered_execution(&self, state: &State, request: Request<Self, State>) -> Reply<Self, State> {
        todo!()
    }

    fn update(
        &mut self,
        state: &mut State,
        request: Request<Self, State>,
    ) -> Reply<Self, State> {
        let reply_inner = match request.as_ref() {
            serialize::Action::Read(key) => {
                let ret = state.db.get(key);

                match ret {
                    Some(vec) => {
                        let map: HashMap<String, Vec<u8>> = bincode::deserialize(vec.as_ref()).expect("deserialize");

                        serialize::Reply::Single(map)
                    },
                    None => serialize::Reply::None,
                }

            }
            serialize::Action::Insert(key, value) => {
                let serialized_map = bincode::serialize(value).expect("failed to serialize");
                state.db.insert(key.clone(), serialized_map);

                serialize::Reply::None
            }
            serialize::Action::Remove(key) => { 
                let ret = state.db.remove(key);
                
                match ret {
                    Some(vec) => {
                        let map: HashMap<String, Vec<u8>> = bincode::deserialize(vec.as_ref()).expect("deserialize");

                        serialize::Reply::Single(map)
                    },
                    None => serialize::Reply::None,
                }
            }
        };

        Arc::new(reply_inner)
    }

fn unordered_batched_execution(
        &self,
        state: &State,
        requests: atlas_execution::app::UnorderedBatch<Request<Self, State>>,
    ) -> BatchReplies<Reply<Self, State>> {
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
        state: &mut State,
        batch: UpdateBatch<Request<Self, State>>,
    ) -> BatchReplies<Reply<Self, State>> {
        let mut reply_batch = BatchReplies::with_capacity(batch.len());

        for update in batch.into_inner() {
            let (peer_id, sess, opid, req) = update.into_inner();
            let reply = self.update(state, req);
            reply_batch.add(peer_id, sess, opid, reply);
        }

        reply_batch
    }
}
