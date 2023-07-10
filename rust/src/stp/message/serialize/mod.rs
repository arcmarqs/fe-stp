#[cfg(feature = "serialize_capnp")]
mod capnp;

use std::marker::PhantomData;
use std::time::Instant;
use atlas_execution::serialize::ApplicationData;
use atlas_core::serialize::{OrderingProtocolMessage, StatefulOrderProtocolMessage, StateTransferMessage};
use atlas_core::state_transfer::{StateTransferProtocol};
use atlas_execution::state::divisible_state::DivisibleState;
use febft_pbft_consensus::bft::message::ObserveEventKind::CollabStateTransfer;

