#![feature(inherent_associated_types)]

use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::{Duration, Instant};

use atlas_core::state_transfer::log_transfer::StatefulOrderProtocol;
use atlas_divisible_state::state_orchestrator::{StateOrchestrator, StateDescriptor};
use atlas_execution::state::divisible_state::{DivisibleState, AppStateMessage, InstallStateMessage, DivisibleStateDescriptor};
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
use atlas_execution::serialize::ApplicationData;
use atlas_core::messages::{StateTransfer, SystemMessage};
use atlas_core::ordering_protocol::{ExecutionResult, OrderingProtocol, SerProof, View};
use atlas_core::persistent_log::{DivisibleStateLog, PersistableStateTransferProtocol, OperationMode};
use atlas_core::serialize::{LogTransferMessage, NetworkView, OrderingProtocolMessage, ServiceMsg, StatefulOrderProtocolMessage, StateTransferMessage};
use atlas_core::state_transfer::{Checkpoint, CstM, StateTransferProtocol, STResult, STTimeoutResult, divisible_state};
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
    //Old(SeqNo,Digest),
    // The current state of the KVDb can be summarized by this descriptor
    Current(Arc<ReadOnly<S::StateDescriptor>>)
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
struct ReceivedState<S> where S: DivisibleState {
    count: usize,
    state: RecoveryState<S>,
}

struct ReceivedStateCid {
    cid: SeqNo,
    count: usize,
}

#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub struct RecoveryState<S> where S: DivisibleState{
    seq: SeqNo,
    pub st_frag: Arc<ReadOnly<Vec<S::StatePart>>>,
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
    phase: ProtoPhase<S>,
    received_state_ids: HashMap<Digest, ReceivedStateCid>,    
    received_states: HashMap<Digest, ReceivedState<S>>,
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
            StProgress::Message(h, m) => (h, m),
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
        self.request_latest_consensus_seq_no::<D, OP, LP, V>(view);   

        Ok(())
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
            MessageKind::RequestLatestSeq => {
                self.process_request_seq(header, st_message);

                return Ok(());
            }
            MessageKind::ReqState => {
                self.process_request_state(header, st_message);

                return Ok(());
            }
            _ => {}
        }

        let status = self.process_message(
            view,
            message,
        )?;

        match status {
            STResult::RunStateTransfer => todo!(),
            STResult::StateTransferNotNeeded(_) => todo!(),
            STResult::StateTransferRunning => todo!(),
            STResult::StateTransferReady => todo!(),
            STResult::StateTransferFinished(_) => todo!(),
           
            // should not happen...
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

        debug!(
            "{:?} // Message {:?} from {:?} while in phase {:?}",
            self.node.id(),
            message,
            header.from(),
            self.phase
        );

        match message.kind() {
            MessageKind::RequestLatestSeq => {
                self.process_request_seq(header, message);

                return Ok(STResult::StateTransferRunning);
            }
            MessageKind::ReqState => {
                self.process_request_state(header, message);

                return Ok(STResult::StateTransferRunning);
            }
            _ => {}
        }

        // Notify timeouts that we have received this message
        self.timeouts
            .received_cst_request(header.from(), message.sequence_number());

        let status = self.process_message_inner(view.clone(), StProgress::Message(header, message));

        match status {
            StStatus::Nil => (),
            StStatus::Running => (),
            StStatus::ReqLatestCid => self.request_latest_consensus_seq_no(view),
            StStatus::SeqNo(seq) => {
                if self.curr_state.into() < seq {
                    debug!("{:?} // Requesting state {:?}", self.node.id(), seq);

                    self.request_latest_state(view);
                } else {
                    debug!("{:?} // Not installing sequence number nor requesting state ???? {:?} {:?}", self.node.id(), self.curr_state.into(), seq);
                    return Ok(STResult::StateTransferNotNeeded(seq));
                }
            },
            StStatus::ReqState => self.request_latest_state(view)?,
            StStatus::State(state) =>{
                let start = Instant::now();

                self.install_channel.send(InstallStateMessage::StatePart(state.st_frag.to_vec())).unwrap();

                return Ok(STResult::StateTransferFinished(state.seq));
            },
        }

        Ok(STResult::StateTransferRunning)
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
impl<S, NT, PL> BtStateTransfer<S, NT, PL>
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
            st_status: StStatus::Nil,
            curr_state: OrchestratorState::None,
            received_state_ids: HashMap::default(),
            received_states: HashMap::default(),
        }
    }

    /// Checks if the CST layer is waiting for a local checkpoint to
    /// complete.
    ///
    /// This is used when a node is sending state to a peer.
    pub fn needs_checkpoint(&self) -> bool {
        matches!(self.phase, ProtoPhase::WaitingCheckpoint(_))
    }

    /// Handle a timeout received from the timeouts layer.
    /// Returns a bool to signify if we must move to the Retrieving state
    /// If the timeout is no longer relevant, returns false (Can remain in current phase)
    pub fn cst_request_timed_out<D, OP, LP, V>(&mut self, seq: SeqNo, view: V) -> bool
    where
        D: ApplicationData + 'static,
        OP: OrderingProtocolMessage + 'static,
        LP: LogTransferMessage + 'static,
        V: NetworkView,
    {
        let status = self.timed_out(seq);

        match status {
            StStatus::ReqLatestCid => {
                self.request_latest_consensus_seq_no(view);

                true
            }
            StStatus::ReqState => {
                self.request_latest_state(view);

                true
            }
            // nothing to do
            _ => false,
        }
    }

    fn timed_out(&mut self, seq: SeqNo) -> StStatus<S> {
        if seq != self.curr_seq {
            // the timeout we received is for a request
            // that has already completed, therefore we ignore it
            //
            // TODO: this check is probably not necessary,
            // as we have likely already updated the `ProtoPhase`
            // to reflect the fact we are no longer receiving state
            // from peer nodes
            return StStatus::Nil;
        }

        self.next_seq();

        match self.phase {
            // retry requests if receiving state and we have timed out
            ProtoPhase::ReceivingCid(_) => {
                self.curr_timeout *= 2;
                StStatus::ReqLatestCid
            }
            ProtoPhase::ReceivingState(_) => {
                self.curr_timeout *= 2;
                StStatus::ReqState
            }
            // ignore timeouts if not receiving any kind
            // of state from peer nodes
            _ => StStatus::Nil,
        }
    }

    fn process_request_seq<D, OP, LP>(
        &mut self,
        header: Header,
        message: StMessage<S>)
        where D: ApplicationData + 'static,
              OP: OrderingProtocolMessage + 'static,
              LP: LogTransferMessage + 'static,
    {
        let seq = match &self.curr_state {
            OrchestratorState::Current(state) => {
                Some((state.sequence_number(),state.get_digest().clone()))
            }
            _ => {
                None
            }
        };

        let kind = MessageKind::ReplyLatestSeq(seq.clone());

        let reply = StMessage::new(message.sequence_number(), kind);

        let network_msg = SystemMessage::from_state_transfer_message(reply);

        debug!("{:?} // Replying to {:?} seq {:?} with seq no {:?}", self.node.id(),
            header.from(), message.sequence_number(), seq);

        self.node.send(network_msg, header.from(), true);
    }

    /// Process the entire list of pending state transfer requests
    /// This will only reply to the latest request sent by each of the replicas
    fn process_pending_state_requests<D, OP, LP>(&mut self)
    where
        D: ApplicationData + 'static,
        OP: OrderingProtocolMessage + 'static,
        LP: LogTransferMessage + 'static,
    {
        let waiting = std::mem::replace(&mut self.phase, ProtoPhase::Init);

        if let ProtoPhase::WaitingCheckpoint(reqs) = waiting {
            let mut map: HashMap<NodeId, StoredMessage<StMessage<S>>> = collections::hash_map();

            for request in reqs {
                // We only want to reply to the most recent requests from each of the nodes
                if map.contains_key(&request.header().from()) {
                    map.entry(request.header().from()).and_modify(|x| {
                        if x.message().sequence_number() < request.message().sequence_number() {
                            //Dispose of the previous request
                            let _ = std::mem::replace(x, request);
                        }
                    });

                    continue;
                } else {
                    map.insert(request.header().from(), request);
                }
            }

            map.into_values().for_each(|req| {
                let (header, message) = req.into_inner();

                self.process_request_state::<D, OP, LP>(header, message);
            });
        }
    }

    fn process_request_state<D, OP, LP>(&mut self, header: Header, message: StMessage<S>)
    where
        D: ApplicationData + 'static,
        OP: OrderingProtocolMessage + 'static,
        LP: LogTransferMessage + 'static,
        
    {
        match &mut self.phase {
            ProtoPhase::Init => {}
            ProtoPhase::WaitingCheckpoint(waiting) => {
                waiting.push(StoredMessage::new(header, message));

                return;
            }
            _ => {
                // We can't reply to state requests when requesting state ourselves
                return;
            }
        }

        let state = match &self.curr_state {
            OrchestratorState::Current(state) => state.clone(),
            _ => {
                if let ProtoPhase::WaitingCheckpoint(waiting) = &mut self.phase {
                    waiting.push(StoredMessage::new(header, message));
                } else {
                    self.phase =
                        ProtoPhase::WaitingCheckpoint(vec![StoredMessage::new(header, message)]);
                }

                return;
            }
        };

        let reply = StMessage::new(
            message.sequence_number(),
            MessageKind::ReplyState(RecoveryState {
                seq: state.sequence_number(),
                st_frag: Arc::new(ReadOnly::new(state.parts().clone())),
            }),
        );

        let network_msg = SystemMessage::from_state_transfer_message(reply);

        self.node.send(network_msg, header.from(), true).unwrap();
    }

    fn process_message_inner<D, OP, LP, V>(
        &mut self,
        view: V,
        progress: StProgress<S>,
    ) -> StStatus<S>
    where
        D: ApplicationData + 'static,
        OP: OrderingProtocolMessage + 'static,
        LP: LogTransferMessage + 'static,
        V: NetworkView,
    {
        match self.phase {
            ProtoPhase::WaitingCheckpoint(_) => {
                self.process_pending_state_requests();

                StStatus::Nil
            }
            ProtoPhase::Init => {
                let (header, message) = getmessage!(progress, StStatus::Nil);

                match message.kind() {
                    MessageKind::RequestLatestSeq=> {
                        self.process_request_seq(header, message);
                    }
                    MessageKind::ReqState => {
                        self.process_request_state(header, message);
                    }
                    // we are not running cst, so drop any reply msgs
                    //
                    // TODO: maybe inspect cid msgs, and passively start
                    // the state transfer protocol, by returning
                    // CstStatus::RequestState
                    _ => (),
                }

                StStatus::Nil
            }
            ProtoPhase::ReceivingCid(i) => {
                let (header, message) = getmessage!(progress, StStatus::ReqLatestCid);

                debug!("{:?} // Received Cid with {} responses from {:?} for CST Seq {:?} vs Ours {:?}", self.node.id(),
                   i, header.from(), message.sequence_number(), self.curr_seq);

                // drop cst messages with invalid seq no
                if message.sequence_number() != self.curr_seq {
                    debug!(
                        "{:?} // Wait what? {:?} {:?}",
                        self.node.id(),
                        self.curr_seq,
                        message.sequence_number()
                    );
                    // FIXME: how to handle old or newer messages?
                    // BFT-SMaRt simply ignores messages with a
                    // value of `queryID` different from the current
                    // `queryID` a replica is tracking...
                    // we will do the same for now
                    //
                    // TODO: implement timeouts to fix cases like this
                    return StStatus::Running;
                }

                match message.kind() {
                    MessageKind::RequestLatestSeq => {
                        self.process_request_seq(header, message);

                        return StStatus::Running;
                    }
                    MessageKind::ReplyLatestSeq(state_cid) => {
                        if let Some((cid, digest)) = state_cid {
                            let received_state_cid = self
                                .received_state_ids
                                .entry(digest.clone())
                                .or_insert_with(|| ReceivedStateCid {
                                    cid: *cid,
                                    count: 0,
                                });

                            if *cid > received_state_cid.cid {
                                info!("{:?} // Received newer state for old cid {:?} vs new cid {:?} with digest {:?}.",
                                    self.node.id(), received_state_cid.cid, *cid, digest);

                                received_state_cid.cid = *cid;
                                received_state_cid.count = 1;
                            } else if *cid == received_state_cid.cid {
                                info!("{:?} // Received matching state for cid {:?} with digest {:?}. Count {}",
                                self.node.id(), received_state_cid.cid, digest, received_state_cid.count + 1);

                                received_state_cid.count += 1;
                            }
                        }
                    }
                    MessageKind::ReqState => {
                        self.process_request_state(header, message);

                        return StStatus::Running;
                    },
                    _ => return StStatus::Running ,
                }

                // check if we have gathered enough cid
                // replies from peer nodes
                //
                // TODO: check for more than one reply from the same node
                let i = i + 1;

                debug!("{:?} // Quorum count {}, i: {}, cst_seq {:?}. Current Latest Cid: {:?}. Current Latest Cid Count: {}",
                        self.node.id(), view.quorum(), i,
                        self.curr_seq, self.largest_cid, self.latest_cid_count);

                if i == view.quorum() {
                    self.phase = ProtoPhase::Init;

                    // reset timeout, since req was successful
                    self.curr_timeout = self.base_timeout;

                    info!("{:?} // Identified the latest state seq no as {:?} with {} votes.",
                            self.node.id(),
                            self.largest_cid, self.latest_cid_count);

                    // we don't need the latest cid to be available in at least
                    // f+1 replicas since the replica has the proof that the system
                    // has decided
                    StStatus::SeqNo(self.largest_cid)
                } else {
                    self.phase = ProtoPhase::ReceivingCid(i);

                    StStatus::Running
                }
            }
            ProtoPhase::ReceivingState(i) => {
                let (header, mut message) = getmessage!(progress, StStatus::ReqState);

                if message.sequence_number() != self.curr_seq {
                    // NOTE: check comment above, on ProtoPhase::ReceivingCid
                    return StStatus::Running;
                }

                let state = match message.take_state() {
                    Some(state) => state,
                    // drop invalid message kinds
                    None => return StStatus::Running,
                };

                let state_digest = state.into_state().0.get_digest();

                debug!("{:?} // Received state with digest {:?}, is contained in map? {}", self.node.id(),
                state_digest, self.received_states.contains_key(&state_digest));

                if self.received_states.contains_key(&state_digest) {
                    let current_state = self.received_states.get_mut(&state_digest).unwrap();

                    let current_state_seq: SeqNo = current_state.state.seq.clone();
                    let recv_state_seq: SeqNo = state.sequence_number();

                    match recv_state_seq.cmp(&current_state_seq) {
                        Ordering::Less | Ordering::Equal => {
                            // we have just verified that the state is the same, but the decision log is
                            // smaller than the one we have already received
                            current_state.count += 1;
                        }
                        Ordering::Greater => {
                            current_state.state = state;
                            // We have also verified that the state is the same but the decision log is
                            // Larger, so we want to store the newest one. However we still want to increment the count
                            // We can do this since to be in the decision log, a replica must have all of the messages
                            // From at least 2f+1 replicas, so we know that the log is valid
                            current_state.count += 1;
                        }
                    }
                } else {
                    self.received_states.insert(state_digest.clone(), ReceivedState { count: 1, state });
                }

                // check if we have gathered enough state
                // replies from peer nodes
                //
                // TODO: check for more than one reply from the same node
                let i = i + 1;

                if i <= view.f() {
                    self.phase = ProtoPhase::ReceivingState(i);
                    return StStatus::Running;
                }

                // NOTE: clear saved states when we return;
                // this is important, because each state
                // may be several GBs in size

                // check if we have at least f+1 matching states
                let digest = {
                    let received_state = self.received_states.iter().max_by_key(|(_, st)| st.count);

                    match received_state {
                        Some((digest, _)) => digest.clone(),
                        None => {
                            return if i >= view.quorum() {
                                self.received_states.clear();

                                debug!("{:?} // No matching states found, clearing", self.node.id());
                                StStatus::ReqState
                            } else {
                                StStatus::Running
                            };
                        }
                    }
                };

                let received_state = {
                    let received_state = self.received_states.remove(&digest);
                    self.received_states.clear();
                    received_state
                };

                // reset timeout, since req was successful
                self.curr_timeout = self.base_timeout;

                // return the state
                let f = view.f();

                match received_state {
                    Some(ReceivedState { count, state }) if count > f => {
                        self.phase = ProtoPhase::Init;

                        StStatus::State(state)
                    }
                    _ => {
                        debug!("{:?} // No states with more than f {} count", self.node.id(), f);

                        StStatus::ReqState
                    }
                }
            },
        }
    }

    pub fn request_latest_consensus_seq_no<D, OP, LP, V>(&mut self, view: V)
    where
        D: ApplicationData + 'static,
        OP: OrderingProtocolMessage + 'static,
        LP: LogTransferMessage + 'static,
        V: NetworkView,
    {
         // reset state of latest seq no. request
         self.largest_cid = SeqNo::ZERO;
         self.latest_cid_count = 0;
 
         self.next_seq();
 
         let cst_seq = self.curr_seq;
 
         info!("{:?} // Requesting latest state seq no with seq {:?}", self.node.id(), cst_seq);
 
         self.timeouts.timeout_cst_request(self.curr_timeout,
                                           view.quorum() as u32,
                                           cst_seq);
 
         self.phase = ProtoPhase::ReceivingCid(0);
 
         let message = StMessage::new(
             cst_seq,
             MessageKind::RequestLatestSeq,
         );
 
         let targets = view;
 
         self.node.broadcast(SystemMessage::from_state_transfer_message(message), targets);
    }

    fn next_seq(&mut self) -> SeqNo {
        self.curr_seq = self.curr_seq.next();

        self.curr_seq
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
            let StateTransferConfig {
                timeout_duration
            } = config;
    
            Ok(Self::new(node, timeout_duration, timeouts, log, executor_state_handle))
    }

    fn handle_state_received_from_app<D, OP, LP>(&mut self,
                                                 descriptor: <S as DivisibleState>::StateDescriptor,
                                                 state: Vec<<S as DivisibleState>::StatePart>) -> Result<()>
        where D: ApplicationData + 'static,
              OP: OrderingProtocolMessage + 'static,
              LP: LogTransferMessage + 'static,
              NT: Node<ServiceMsg<D, OP, Self::Serialization, LP>> {
               
        if self.needs_checkpoint() {
            // This will make the state transfer protocol aware of the latest state
            self.curr_state = OrchestratorState::Current(Arc::new(ReadOnly::new(descriptor)));
        }

        Ok(())
    }
}
