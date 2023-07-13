#[cfg(feature = "serialize_capnp")]
mod capnp;

use std::marker::PhantomData;
use std::time::Instant;
use atlas_execution::{serialize::ApplicationData, state::divisible_state::DivisibleState};
use atlas_core::serialize::{OrderingProtocolMessage, StatefulOrderProtocolMessage, StateTransferMessage};
use atlas_divisible_state::state_orchestrator::StateOrchestrator;

use super::StMessage;

pub struct STMsg<S: DivisibleState>(PhantomData<S>);

impl<S: DivisibleState> StateTransferMessage for STMsg<S> {

    type StateTransferMessage = StMessage<S>;

    #[cfg(feature = "serialize_capnp")]
    fn serialize_capnp(builder: atlas_capnp::cst_messages_capnp::cst_message::Builder, msg: &Self::StateTransferMessage) -> atlas_common::error::Result<()> {
        todo!()
    }

    #[cfg(feature = "serialize_capnp")]
    fn deserialize_capnp(reader: atlas_capnp::cst_messages_capnp::cst_message::Reader) -> atlas_common::error::Result<Self::StateTransferMessage> {
        todo!()
    }
}

unsafe impl<S: DivisibleState> Send for STMsg<S> {
    
}