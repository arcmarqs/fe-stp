#[cfg(feature = "serialize_capnp")]
mod capnp;

use std::{marker::PhantomData, sync::Arc};
use std::time::Instant;
use atlas_communication::message_signing::NetworkMessageSignatureVerifier;
use atlas_communication::reconfiguration_node::NetworkInformationProvider;
use atlas_communication::{serialize::Serializable, message::Header};
use atlas_execution::{serialize::ApplicationData, state::divisible_state::{DivisibleState, StatePart}};
use atlas_core::serialize::{StateTransferMessage, InternallyVerifiable};
use atlas_divisible_state::{state_orchestrator::StateOrchestrator, SerializedState};
use serde::{Serialize, Deserialize};

use super::StMessage;

pub struct STMsg<S> (PhantomData<S>);

impl<S: DivisibleState> InternallyVerifiable<StMessage<S>> for STMsg<S> {
    fn verify_internal_message<M, SV, NI>(network_info: &Arc<NI>, header: &Header, msg: &StMessage<S>) -> atlas_common::error::Result<bool>
        where M: Serializable,
              SV: NetworkMessageSignatureVerifier<M, NI>,
              NI: NetworkInformationProvider {
        Ok(true)
    }
}

impl<S: DivisibleState + for<'a> Deserialize<'a> + Serialize> StateTransferMessage for STMsg<S> {

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

