use std::fmt::{Debug, Formatter};

use atlas_execution::state::divisible_state::{AppStateMessage, DivisibleState};
#[cfg(feature = "serialize_serde")]
use serde::{Deserialize, Serialize};

use atlas_common::{ordering::{Orderable, SeqNo}, crypto::hash::Digest};



pub mod serialize;

#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub struct CstMessage<S> {
    // NOTE: not the same sequence number used in the
    // consensus layer to order client requests!
    seq: SeqNo,
    kind: MessageKind<S>,
}

impl<S> Debug for CstMessage<S> {
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
pub enum MessageKind<S>{
    RequestLatestConsensusSeq,
    ReplyLatestConsensusSeq(Option<(SeqNo, Digest)>),
    RequestState,
    ReplyState(AppStateMessage<S>),
}

impl<S> Orderable for CstMessage<S> {
    /// Returns the sequence number of this state transfer message.
    fn sequence_number(&self) -> SeqNo {
        self.seq
    }
}

impl<S> CstMessage<S> {
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
    pub fn take_state(&mut self) -> Option<AppStateMessage<S>> {
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