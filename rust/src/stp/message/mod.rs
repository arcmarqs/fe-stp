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
pub struct StMessage {
    // NOTE: not the same sequence number used in the
    // consensus layer to order client requests!
    seq: SeqNo,
    kind: MessageKind,
}

impl Debug for CstMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.kind {
            MessageKind::RequestLatestConsensusSeq => {
                write!(f, "Request consensus ID")
            }
            MessageKind::ReplyLatestConsensusSeq(opt ) => {
                write!(f, "Reply consensus seq {:?}", opt.as_ref().map(|(seq, _)| *seq).unwrap_or(SeqNo::ZERO))
            }
            MessageKind::RequestState => {
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
pub enum MessageKind{
    RequestLatestConsensusSeq,
    ReplyLatestConsensusSeq(Option<(SeqNo, Digest)>),
    RequestState,
    ReplyState(RecoveryState<StateOrchestrator>),
}

impl Orderable for CstMessage {
    /// Returns the sequence number of this state transfer message.
    fn sequence_number(&self) -> SeqNo {
        self.seq
    }
}

impl CstMessage {
    /// Creates a new `CstMessage` with sequence number `seq`,
    /// and of the kind `kind`.
    pub fn new(seq: SeqNo, kind: MessageKind) -> Self {
        Self { seq, kind }
    }

    /// Returns a reference to the state transfer message kind.
    pub fn kind(&self) -> &MessageKind {
        &self.kind
    }

    /// Takes the recovery state embedded in this cst message, if it is available.
    pub fn take_state(&mut self) -> Option<AppStateMessage<StateOrchestrator>> {
        let kind = std::mem::replace(&mut self.kind, MessageKind::RequestState);
        match kind {
            MessageKind::ReplyState(state) => Some(state),
            _ => {
                self.kind = kind;
                None
            }
        }
    }
}