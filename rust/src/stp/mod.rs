#![feature(inherent_associated_types)]

use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::{Duration, Instant};

use atlas_core::state_transfer::log_transfer::StatefulOrderProtocol;
use atlas_execution::state::divisible_state::{DivisibleState, AppStateMessage, InstallStateMessage};
use log::{debug, error, info};
#[cfg(feature = "serialize_serde")]
use serde::{Deserialize, Serialize};
use atlas_common::channel::ChannelSyncTx;

use atlas_common::collections;
use atlas_common::collections::HashMap;
use atlas_common::crypto::hash::Digest;
use atlas_common::error::*;
use atlas_common::globals::ReadOnly;
use atlas_common::node_id::NodeId;
use atlas_common::ordering::{Orderable, SeqNo};
use atlas_communication::Node;
use atlas_communication::message::{Header, NetworkMessageKind, StoredMessage};
use atlas_execution::app::{Application, Reply, Request};
use atlas_execution::ExecutorHandle;
use atlas_execution::serialize::ApplicationData;
use atlas_core::messages::{StateTransfer, SystemMessage};
use atlas_core::ordering_protocol::{ExecutionResult, OrderingProtocol, SerProof, View};
use atlas_core::persistent_log::{DivisibleStateLog, PersistableStateTransferProtocol, OperationMode};
use atlas_core::serialize::{LogTransferMessage, NetworkView, OrderingProtocolMessage, ServiceMsg, StatefulOrderProtocolMessage, StateTransferMessage};
use atlas_core::state_transfer::{Checkpoint, CstM, StateTransferProtocol, STResult, STTimeoutResult};
use atlas_core::state_transfer::divisible_state::*;
use atlas_divisible_state::*;
use atlas_core::timeouts::{RqTimeout, TimeoutKind, Timeouts};
use atlas_metrics::metrics::metric_duration;

use self::message::CstMessage;
use self::message::serialize::STMsg;

pub mod message;



pub struct StateTransferConfig {
    pub timeout_duration: Duration
}
/// The state of the checkpoint
pub enum CheckpointState<D> {
    // no checkpoint has been performed yet
    None,
    // we are calling this a partial checkpoint because we are
    // waiting for the application state from the execution layer
    Partial {
        // sequence number of the last executed request
        seq: SeqNo,
    },
    PartialWithEarlier {
        // sequence number of the last executed request
        seq: SeqNo,
        // save the earlier checkpoint, in case corruption takes place
        earlier: Arc<ReadOnly<Checkpoint<D>>>,
    },
    // application state received, the checkpoint state is finalized
    Complete(Arc<ReadOnly<Checkpoint<D>>>),
}

enum ProtoPhase<S> {
    Init,
    WaitingCheckpoint(Vec<StoredMessage<CstMessage<S>>>),
    ReceivingCid(usize),
    ReceivingState(usize),
}

impl<S> Debug for ProtoPhase<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ProtoPhase::Init => {
                write!(f, "Init Phase")
            }
            ProtoPhase::WaitingCheckpoint(header) => {
                write!(f, "Waiting for checkpoint {}", header.len())
            }
            ProtoPhase::ReceivingCid(size) => {
                write!(f, "Receiving CID phase {} responses", size)
            }
            ProtoPhase::ReceivingState(size) => {
                write!(f, "Receiving state phase {} responses", size)
            }
        }
    }
}

pub struct StFragment<S> {

}

pub struct RecoveryState<S,V,O> {
    pub st_frag: Arc<ReadOnly<StFragment<S>>>,
}

enum STStatus {
    Nil,
    Running,
    ReqLatestCid,
    ReqStateUpdate
}
// NOTE: in this module, we may use cid interchangeably with
// consensus sequence number
pub struct BtStateTransfer<S, NT, PL>
    where S: DivisibleState + 'static {
    curr_seq: SeqNo,
    StStatus: STStatus,
    largest_cid: SeqNo,
    latest_cid_count: usize,
    base_timeout: Duration,
    curr_timeout: Duration,
    timeouts: Timeouts,

    node: Arc<NT>,
    orchestrator: Arc<S>,

    phase: ProtoPhase<S>,

    install_channel: ChannelSyncTx<InstallStateMessage<S>>,

    /// Persistent logging for the state transfer protocol.
    persistent_log: PL,
}


pub enum CstProgress<S> {
    // TODO: Timeout( some type here)
    /// This value represents null progress in the CST code's state machine.
    Nil,
    /// We have a fresh new message to feed the CST state machine, from
    /// the communication layer.
    Message(Header, CstMessage<S>),
}

macro_rules! getmessage {
    ($progress:expr, $status:expr) => {
        match $progress {
            CstProgress::Nil => return $status,
            CstProgress::Message(h, m) => (h, m),
        }
    };
    // message queued while waiting for exec layer to deliver app state
    ($phase:expr) => {{
        let phase = std::mem::replace($phase, ProtoPhase::Init);
        match phase {
            ProtoPhase::WaitingCheckpoint(h, m) => (h, m),
            _ => return CstStatus::Nil,
        }
    }};
}

impl<S, NT, PL> StateTransferProtocol<S, NT, PL> for BtStateTransfer<S, NT, PL>
where
    S: DivisibleState + 'static,
    PL: DivisibleStateLog<S> + 'static,
{
    type Serialization = STMsg<S>;

    fn request_latest_state<D, OP, LP, V>(&mut self, view: V) -> Result<()>
    where
        D: ApplicationData + 'static,
        OP: OrderingProtocolMessage + 'static,
        LP: LogTransferMessage + 'static,
        NT: Node<ServiceMsg<D, OP, Self::Serialization, LP>>,
        V: NetworkView,
    {

        
    }

    fn handle_off_ctx_message<D, OP, LP, V>(
        &mut self,
        view: V,
        message: StoredMessage<StateTransfer<CstM<Self::Serialization>>>,
    ) -> Result<()>
    where
        D: ApplicationData + 'static,
        OP: OrderingProtocolMessage + 'static,
        LP: LogTransferMessage + 'static,
        NT: Node<ServiceMsg<D, OP, Self::Serialization, LP>>,
        V: NetworkView,
    {
        let (header, message) = message.into_inner();

        let message = message.into_inner();

        debug!("{:?} // Off context Message {:?} from {:?} with seq {:?}", self.node.id(), message, header.from(), message.sequence_number());

        match &message.kind() {
            CstMessageKind::RequestStateCid => {
                self.process_request_seq(header, message);

                return Ok(());
            }
            CstMessageKind::RequestState => {
                self.process_request_state(header, message);

                return Ok(());
            }
            _ => {}
        }

        let status = self.process_message(
            view,
            CstProgress::Message(header, message),
        );

        match status {
            CstStatus::Nil => (),
// should not happen...
            _ => {
                return Err(format!("Invalid state reached while state transfer processing message! {:?}", status)).wrapped(ErrorKind::CoreServer);
            }
        }

        Ok(())
    }

    fn process_message<D, OP, LP, V>(
        &mut self,
        view: V,
        message: StoredMessage<StateTransfer<CstM<Self::Serialization>>>,
    ) -> Result<STResult>
    where
        D: ApplicationData + 'static,
        OP: OrderingProtocolMessage + 'static,
        LP: LogTransferMessage + 'static,
        NT: Node<ServiceMsg<D, OP, Self::Serialization, LP>>,
        V: NetworkView,
    {
        let (header, message) = message.into_inner();

        let message = message.into_inner();

        debug!("{:?} // Message {:?} from {:?} while in phase {:?}", self.node.id(), message, header.from(), self.phase);

        match &message.kind() {}
    }

    fn handle_app_state_requested<D, OP, LP, V>(
        &mut self,
        view: V,
        seq: SeqNo,
    ) -> Result<ExecutionResult>
    where
        D: ApplicationData + 'static,
        OP: OrderingProtocolMessage + 'static,
        LP: LogTransferMessage + 'static,
        NT: Node<ServiceMsg<D, OP, Self::Serialization, LP>>,
        V: NetworkView,
    {
        let earlier = std::mem::replace(&mut self.current_checkpoint_state, CheckpointState::None);

        self.current_checkpoint_state = match earlier {
            CheckpointState::None => CheckpointState::Partial { seq },
            CheckpointState::Complete(earlier) => {
                CheckpointState::PartialWithEarlier { seq, earlier }
            }
            // FIXME: this may not be an invalid state after all; we may just be generating
            // checkpoints too fast for the execution layer to keep up, delivering the
            // hash digests of the appstate
            _ => {
                error!("Invalid checkpoint state detected");

                self.current_checkpoint_state = earlier;

                return Ok(ExecutionResult::Nil);
            }
        };

        Ok(ExecutionResult::BeginCheckpoint)
    }

    fn handle_timeout<D, OP, LP, V>(
        &mut self,
        view: V,
        timeout: Vec<RqTimeout>,
    ) -> Result<STTimeoutResult>
    where
        D: ApplicationData + 'static,
        OP: OrderingProtocolMessage + 'static,
        LP: LogTransferMessage + 'static,
        NT: Node<ServiceMsg<D, OP, Self::Serialization, LP>>,
        V: NetworkView,
    {
        for cst_seq in timeout {
            if let TimeoutKind::Cst(cst_seq) = cst_seq.timeout_kind() {
                if self.cst_request_timed_out(cst_seq.clone(), view.clone()) {
                    return Ok(STTimeoutResult::RunCst);
                }
            }
        }

        Ok(STTimeoutResult::CstNotNeeded)
    }
}

type Ser<ST: StateTransferProtocol<S, NT, PL>, S, NT, PL> = <ST as StateTransferProtocol<S, NT, PL>>::Serialization;

// TODO: request timeouts
impl<S,NT, PL> BtStateTransfer<S,NT,PL>
    where
        S: DivisibleState + 'static,
        PL: DivisibleStateLog<S> + 'static,
{
    /// Create a new instance of `BtStateTransfer`.
    pub fn new(node: Arc<NT>, base_timeout: Duration, timeouts: Timeouts, persistent_log: PL, install_channel: ChannelSyncTx<InstallStateMessage<S>>) -> Self {
        Self {
            base_timeout,
            curr_timeout: base_timeout,
            timeouts,
            node,
            phase: ProtoPhase::Init,
            largest_cid: SeqNo::ZERO,
            latest_cid_count: 0,
            curr_seq: SeqNo::ZERO,
            persistent_log,
            install_channel,
            cur_root_digest: todo!(),
        }
    }

}

impl<S, NT, PL> DivisibleStateTransfer<S, NT, PL> for BtStateTransfer<S, NT, PL>
where
    S: DivisibleState + 'static,
    PL: DivisibleStateLog<S> + 'static,
{
    type Config = StateTransferConfig;

    fn initialize(config: Self::Config, timeouts: Timeouts, node: Arc<NT>, log: PL,
                  executor_state_handle: ChannelSyncTx<InstallStateMessage<S>>) -> Result<Self>
        where Self: Sized {
        todo!()
    }

    fn handle_state_received_from_app<D, OP, LP>(&mut self,
                                                 descriptor: <S as DivisibleState>::StateDescriptor,
                                                 state: Vec<<S as DivisibleState>::StatePart>) -> Result<()>
        where D: ApplicationData + 'static,
              OP: OrderingProtocolMessage + 'static,
              LP: LogTransferMessage + 'static,
              NT: Node<ServiceMsg<D, OP, Self::Serialization, LP>> {
        todo!()
    }
}
