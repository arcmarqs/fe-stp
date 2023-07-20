use std::fmt::{Debug, Formatter};

use atlas_divisible_state::state_orchestrator::StateOrchestrator;
use atlas_execution::state::divisible_state::{AppStateMessage, DivisibleState};
#[cfg(feature = "serialize_serde")]
use serde::{Deserialize, Serialize};

use atlas_common::{ordering::{Orderable, SeqNo}, crypto::hash::Digest};

use super::RecoveryState;



pub mod serialize;

#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub struct StMessage<S:DivisibleState> {
    // NOTE: not the same sequence number used in the
    // consensus layer to order client requests!
    seq: SeqNo,
    kind: MessageKind<S> ,
}

impl<S: DivisibleState> Debug for StMessage<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.kind {
            MessageKind::RequestLatestSeq => {
                write!(f, "Request consensus ID")
            }
            MessageKind::ReplyLatestSeq(opt ) => {
                write!(f, "Reply consensus seq {:?}", opt.as_ref().map(|(seq, _)| *seq).unwrap_or(SeqNo::ZERO))
            }
            MessageKind::ReqState => {
                write!(f, "Request state message")
            }
            MessageKind::ReplyState(_) => {
                write!(f, "Reply with state message")
            }
        }

    }
}

#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub enum MessageKind<S:DivisibleState> {
    RequestLatestSeq,
    ReplyLatestSeq(Option<(SeqNo, Digest)>),
    ReqState,
    ReplyState(RecoveryState<S>),
}

impl<S>  Orderable for StMessage<S> where S: DivisibleState {
    /// Returns the sequence number of this state transfer message.
    fn sequence_number(&self) -> SeqNo {
        self.seq
    }
}

impl<S> StMessage<S> where S: DivisibleState {
    /// Creates a new `CstMessage` with sequence number `seq`,
    /// and of the kind `kind`.
    pub fn new(seq: SeqNo, kind: MessageKind<S>) -> Self {
        Self { seq, kind }
    }

    /// Returns a reference to the state transfer message kind.
    pub fn kind(&self) -> &MessageKind<S> {
        &self.kind
    }

    /// Takes the recovery state embedded in this cst message, if it is available.
    pub fn take_state(&mut self) -> Option<RecoveryState<S>> {
        let kind = std::mem::replace(&mut self.kind, MessageKind::ReqState);
        match kind {
            MessageKind::ReplyState(state) => Some(state),
            _ => {
                self.kind = kind;
                None
            }
        }
    }
}