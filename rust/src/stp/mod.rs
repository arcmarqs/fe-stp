#![feature(inherent_associated_types)]

use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::{Duration, Instant};

use atlas_core::state_transfer::log_transfer::StatefulOrderProtocol;
use atlas_divisible_state::state_orchestrator::StateOrchestrator;
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

use crate::stp::message::MessageKind;

use self::message::StMessage;
use self::message::serialize::STMsg;

pub mod message;


enum OrchestratorState<S> where S:DivisibleState {
    // We haven't had a flush yet
    None,
    // the current state is outdated, requires syncing with either the Orchestrator 
    // or with other replicas, the seqno here is the StateOrchestrator SeqNo
    Old(SeqNo,Digest),
    // The current state of the KVDb can be summarized by this descriptor
    Current(S::StateDescriptor)
}


pub struct StateTransferConfig {
    pub timeout_duration: Duration
}
/// The state of the checkpoint

enum ProtoPhase<S> where S: DivisibleState {
    Init,
    WaitingCheckpoint(Vec<StoredMessage<StMessage<S>>>),
    ReceivingCid(usize),
    ReceivingState(usize),
}

impl<S> Debug for ProtoPhase<S> where S: DivisibleState {
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

#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub struct ReqStateParts<S> where S:DivisibleState {
    descriptors: Vec<S::PartDescription>,
}

#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub struct StFragment<S> where S: DivisibleState {
    parts: Vec<S::StatePart>,
}


#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub struct RecoveryState<S> where S: DivisibleState{
    pub st_frag: Arc<ReadOnly<StFragment<S>>>,
}

enum StStatus<S> where S:DivisibleState {
    Nil,
    Running,
    ReqLatestCid,
    SeqNo(SeqNo),
    ReqState,
    State(RecoveryState<S>)
}
// NOTE: in this module, we may use cid interchangeably with
// consensus sequence number
pub struct BtStateTransfer<S, NT, PL>
    where S: DivisibleState + 'static {
    curr_seq: SeqNo,
    st_status: StStatus<S>,
    curr_state: OrchestratorState<S>,
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


pub enum StProgress<S> where S: DivisibleState {
    // TODO: Timeout( some type here)
    /// This value represents null progress in the CST code's state machine.
    Nil,
    /// We have a fresh new message to feed the CST state machine, from
    /// the communication layer.
    Message(Header, StMessage<S>),
}

macro_rules! getmessage {
    ($progress:expr, $status:expr) => {
        match $progress {
            StProgress::Nil => return $status,
            SProgress::Message(h, m) => (h, m),
        }
    };
    // message queued while waiting for exec layer to deliver app state
    ($phase:expr) => {{
        let phase = std::mem::replace($phase, ProtoPhase::Init);
        match phase {
            ProtoPhase::WaitingCheckpoint(h, m) => (h, m),
            _ => return StStatus::Nil,
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

        todo!()
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
        let (header, inner_message) = message.into_inner();

        let st_message = inner_message.into_inner();

        debug!("{:?} // Off context Message {:?} from {:?} with seq {:?}", self.node.id(), st_message, header.from(), st_message.sequence_number());

        match &inner_message.kind() {
            MessageKind::RequestLatestConsensusSeq => {
                self.process_request_seq(header, inner_message);

                return Ok(());
            }
            MessageKind::RequestState => {
                self.process_request_state(header, inner_message);

                return Ok(());
            }
            _ => {}
        }

        let status = self.process_message(
            view,
            message,
        );

        match status {
            StStatus::Nil => (),
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

        match message.kind() {
            MessageKind::RequestLatestConsensusSeq => todo!(),
            MessageKind::ReplyLatestConsensusSeq(_) => todo!(),
            MessageKind::RequestState => todo!(),
            MessageKind::ReplyState(_) => todo!(),
        }
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
        let earlier = std::mem::replace(&mut self.curr_state, OrchestratorState::None);

        self.curr_state = match earlier {
            OrchestratorState::None => OrchestratorState::Current(self.orchestrator.prepare_checkpoint()?),
            OrchestratorState::Old(seq, digest) => {
                if seq < self.orchestrator.get_seqno() {
                    OrchestratorState::Current(self.orchestrator.prepare_checkpoint()?)
                }
            },
            OrchestratorState::Current(c) => OrchestratorState::Current(c),
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
    pub fn new(orchestrator: Arc<S>, node: Arc<NT>, base_timeout: Duration, timeouts: Timeouts, persistent_log: PL, install_channel: ChannelSyncTx<InstallStateMessage<S>>) -> Self {
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
            st_status: StStatus::Nil,
            curr_state: OrchestratorState::None,
            orchestrator,
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
