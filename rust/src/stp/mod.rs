#![feature(inherent_associated_types)]

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::fmt::{write, Debug, Formatter};
use std::fs::{File, OpenOptions};
use std::io::{prelude::*, Seek, SeekFrom, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};

use atlas_common::channel::ChannelSyncTx;
use atlas_core::state_transfer::networking::StateTransferSendNode;
use atlas_divisible_state::state_orchestrator::{StateDescriptor, StateOrchestrator};
use atlas_divisible_state::state_tree::LeafNode;
use atlas_execution::state::divisible_state::{
    AppStateMessage, DivisibleState, DivisibleStateDescriptor, InstallStateMessage,
    PartDescription, StatePart,
};
use log::{debug, error, info};
use serde::{Deserialize, Serialize};

use atlas_common::collections::HashMap;
use atlas_common::collections::{self, HashSet, OrderedMap};
use atlas_common::crypto::hash::Digest;
use atlas_common::error::*;
use atlas_common::globals::ReadOnly;
use atlas_common::node_id::NodeId;
use atlas_common::ordering::{Orderable, SeqNo};
use atlas_communication::message::{Header, NetworkMessage, NetworkMessageKind, StoredMessage};
use atlas_core::messages::{StateTransfer, SystemMessage};
use atlas_core::ordering_protocol::{ExecutionResult, OrderingProtocol, SerProof, View};
use atlas_core::persistent_log::{
    DivisibleStateLog, OperationMode, PersistableStateTransferProtocol,
};
use atlas_core::serialize::{
    LogTransferMessage, NetworkView, OrderingProtocolMessage, ServiceMsg, StateTransferMessage,
    StatefulOrderProtocolMessage,
};
use atlas_core::state_transfer::divisible_state::*;
use atlas_core::state_transfer::{
    divisible_state, Checkpoint, CstM, STResult, STTimeoutResult, StateTransferProtocol,
};
use atlas_core::timeouts::{RqTimeout, TimeoutKind, Timeouts};
use atlas_divisible_state::*;
use atlas_execution::app::{Application, Reply, Request};
use atlas_execution::serialize::ApplicationData;

use crate::stp::message::MessageKind;

use self::message::serialize::STMsg;
use self::message::StMessage;

pub mod message;

pub struct StateTransferConfig {
    pub timeout_duration: Duration,
}

type PartRef = (File, u64);
#[derive(Debug, Default)]
struct PersistentCheckpoint<S: DivisibleState> {
    descriptor: Option<S::StateDescriptor>,
    path: String,
    // used to track the state part's position in the persistent checkpoint
    // K = pageId V= Length
    parts: HashMap<u64, PartRef>,
    // list of parts we need in orget to recover the state
    req_parts: Vec<S::PartDescription>,
}

impl<S: DivisibleState> PersistentCheckpoint<S> {
    pub fn new() -> Self {
        Self {
            descriptor: None,
            path: String::from("./checkpoint/page"),
            parts: HashMap::default(),
            req_parts: Vec::default(),
        }
    }

    pub fn descriptor(&self) -> &Option<S::StateDescriptor> {
        &self.descriptor
    }
    pub fn get_digest(&self) -> Option<&Digest> {
        match &self.descriptor {
            Some(descriptor) => Some(descriptor.get_digest()),
            None => None,
        }
    }

    pub fn get_seqno(&self) -> Option<SeqNo> {
        match &self.descriptor {
            Some(descriptor) => Some(descriptor.sequence_number()),
            None => None,
        }
    }

    fn distribute_req_parts(
        &mut self,
        targets: impl Iterator<Item = NodeId>,
        num_targets: usize,
    ) -> HashMap<NodeId, Vec<S::PartDescription>> {
        let mut ret = HashMap::default();

        let _ = self.req_parts
            .chunks(num_targets)
            .zip(targets)
            .map(|(chunk, id)| ret.insert(id, chunk.to_vec()));

        ret
    }

    pub fn update(&mut self, descriptor: S::StateDescriptor, new_parts: Vec<S::StatePart>) {
        self.descriptor = Some(descriptor);
  
        for part in new_parts {
            let part_path = format!("{}{}", self.path, part.id().to_string());
            if let Ok(mut f) = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(part_path)
            {
                f.seek(SeekFrom::Start(0)).unwrap();
                let buf = bincode::serialize(&part).unwrap();
                f.write_all(buf.as_slice()).unwrap();
                self.parts.insert(part.id(), (f, buf.len() as u64));
            }
        }
    }

    fn get_file(&mut self, id: &u64) -> Option<&mut (File, u64)> {
        self.parts.get_mut(id)
    }

    pub fn get_parts(&mut self, parts_desc: &Vec<S::PartDescription>) -> Result<Vec<S::StatePart>> {
        let mut ret = Vec::new();
        let mut buf = Vec::new();
        for part_desc in parts_desc.iter() {
            if let Some((file, len)) = self.get_file(part_desc.id()) {
                assert_eq!(len.clone(), file.read_to_end(&mut buf)? as u64);
                let part = bincode::deserialize::<S::StatePart>(buf.as_slice())
                    .wrapped(ErrorKind::CommunicationSerialize)?;
                ret.push(part);
            }
        }

        Ok(ret)
    }
}

enum ProtoPhase<S: DivisibleState> {
    Init,
    WaitingCheckpoint(Vec<StoredMessage<StMessage<S>>>),
    ReceivingCid(usize),
    ReceivingStateDescriptor,
    ReceivingState(usize),
}

impl<S: DivisibleState> Debug for ProtoPhase<S> {
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
            ProtoPhase::ReceivingStateDescriptor => {
                write!(f, "Receiving state descriptor")
            }
            ProtoPhase::ReceivingState(size) => {
                write!(f, "Receiving state phase responses")
            }
        }
    }
}

struct ReceivedStateCid {
    cid: SeqNo,
    count: usize,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct RecoveryState<S: DivisibleState> {
    seq: SeqNo,
    pub st_frag: Arc<ReadOnly<Vec<S::StatePart>>>,
}

enum StStatus<S: DivisibleState> {
    Nil,
    Running,
    ReqLatestCid,
    SeqNo(SeqNo),
    RequestStateDescriptor,
    StateDescriptor(S::StateDescriptor),
    ReqState,
    State(RecoveryState<S>),
    PartiallyComplete(RecoveryState<S>),
}

impl<S: DivisibleState> Debug for StStatus<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            StStatus::Nil => {
                write!(f, "Nil")
            }
            StStatus::Running => {
                write!(f, "Running")
            }
            StStatus::ReqLatestCid => {
                write!(f, "Request latest CID")
            }
            StStatus::ReqState => {
                write!(f, "Request latest state")
            }
            StStatus::SeqNo(seq) => {
                write!(f, "Received seq no {:?}", seq)
            }
            StStatus::State(_) => {
                write!(f, "Received state")
            }
            StStatus::RequestStateDescriptor => write!(f, "Request State Descriptor"),
            StStatus::StateDescriptor(_) => write!(f, "Received State Descriptor"),
            StStatus::PartiallyComplete(_) => {
                write!(f, "Some Parts Installed, Requesting Remaining parts")
            }
        }
    }
}

// NOTE: in this module, we may use cid interchangeably with
// consensus sequence number
pub struct BtStateTransfer<S, NT, PL>
where
    S: DivisibleState + 'static,
{
    curr_seq: SeqNo,
    st_status: StStatus<S>,
    checkpoint: PersistentCheckpoint<S>,
    largest_cid: SeqNo,
    latest_cid_count: usize,
    base_timeout: Duration,
    curr_timeout: Duration,
    timeouts: Timeouts,

    node: Arc<NT>,
    phase: ProtoPhase<S>,
    received_state_ids: HashMap<Digest, ReceivedStateCid>,
    accepted_state_parts: Vec<S::StatePart>,
    install_channel: ChannelSyncTx<InstallStateMessage<S>>,

    /// Persistent logging for the state transfer protocol.
    persistent_log: PL,
}

pub enum StProgress<S: DivisibleState> {
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
    S: DivisibleState + 'static + Send + Clone + for<'a> Deserialize<'a> + Serialize,
    PL: DivisibleStateLog<S> + 'static,
    NT: StateTransferSendNode<STMsg<S>> + 'static,
{
    type Serialization = STMsg<S>;

    fn request_latest_state<V>(&mut self, view: V) -> Result<()>
    where
        V: NetworkView,
    {
        self.request_latest_consensus_seq_no::<V>(view);

        Ok(())
    }

    fn handle_off_ctx_message<V>(
        &mut self,
        view: V,
        message: StoredMessage<StateTransfer<CstM<Self::Serialization>>>,
    ) -> Result<()>
    where
        V: NetworkView,
    {
        let (header, inner_message) = message.into_inner();
        let kind = inner_message.kind().clone();
        let st_message = inner_message.into_inner();

        debug!(
            "{:?} // Off context Message {:?} from {:?} with seq {:?}",
            self.node.id(),
            st_message,
            header.from(),
            st_message.sequence_number()
        );

        match kind {
            MessageKind::RequestLatestSeq => {
                self.process_request_seq(header, st_message);

                return Ok(());
            }
            MessageKind::RequestStateDescriptor => {
                self.process_request_descriptor(header, st_message);

                return Ok(());
            }
            MessageKind::ReqState(_) => {
                self.process_request_state(header, st_message);

                return Ok(());
            }
            _ => {}
        }

        let status = self.process_message_inner(view, StProgress::Message(header, st_message));

        match status {
            StStatus::Nil => (),
            _ => {
                return Err(format!(
                    "Invalid state reached while state transfer processing message! {:?}",
                    status
                ))
                .wrapped(ErrorKind::CoreServer)
            }
        }
        Ok(())
    }

    fn process_message<V>(
        &mut self,
        view: V,
        message: StoredMessage<StateTransfer<CstM<Self::Serialization>>>,
    ) -> Result<STResult>
    where
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
                println!("ReqSeq");
                self.process_request_seq(header, message);

                return Ok(STResult::StateTransferRunning);
            }
            MessageKind::RequestStateDescriptor => {
                println!("ReqStateDesc");

                self.process_request_descriptor(header, message);

                return Ok(STResult::StateTransferRunning);
            }
            MessageKind::ReqState(_) => {
                println!("ReqState");

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
                if self.checkpoint.get_seqno() < Some(seq) {
                    debug!("{:?} // Requesting state {:?}", self.node.id(), seq);

                    self.request_state_descriptor(view);
                } else {
                    //debug!("{:?} // Not installing sequence number nor requesting state ???? {:?} {:?}", self.node.id(), self.curr_state.into(), seq);
                    return Ok(STResult::StateTransferNotNeeded(seq));
                }
            }
            StStatus::ReqState => self.request_latest_state(view)?,
            StStatus::State(state) => {
                self.install_channel
                    .send(InstallStateMessage::StatePart(state.st_frag.to_vec()))
                    .unwrap();

                return Ok(STResult::StateTransferFinished(state.seq));
            }
            StStatus::RequestStateDescriptor => self.request_state_descriptor(view),
            StStatus::StateDescriptor(descriptor) => {
                let my_descriptor = self.checkpoint.descriptor().as_ref();
                if my_descriptor != Some(&descriptor) {
                    self.checkpoint.req_parts = match &self.checkpoint.descriptor {
                        Some(descriptor) => descriptor.compare_descriptors(descriptor),
                        None => descriptor.parts().clone(),
                    };
                    self.request_latest_state(view)?;
                } else {
                    return Ok(STResult::StateTransferNotNeeded(
                        descriptor.sequence_number(),
                    ));
                }
            }
            StStatus::PartiallyComplete(state) => {
                self.install_channel
                    .send(InstallStateMessage::StatePart(state.st_frag.to_vec()))
                    .unwrap();

                self.request_latest_state(view)?;
            }
        }

        Ok(STResult::StateTransferRunning)
    }

    fn handle_app_state_requested<V>(&mut self, view: V, seq: SeqNo) -> Result<ExecutionResult>
    where
        V: NetworkView,
    {
        if self.checkpoint.descriptor.is_none() {
            Ok(ExecutionResult::BeginCheckpoint)
        } else {
            Ok(ExecutionResult::Nil)
        }
    }

    fn handle_timeout<V>(&mut self, view: V, timeout: Vec<RqTimeout>) -> Result<STTimeoutResult>
    where
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

type Ser<ST: StateTransferProtocol<S, NT, PL>, S, NT, PL> =
    <ST as StateTransferProtocol<S, NT, PL>>::Serialization;

// TODO: request timeouts
impl<S, NT, PL> BtStateTransfer<S, NT, PL>
where
    S: DivisibleState + 'static + for<'a> Deserialize<'a> + Serialize,
    PL: DivisibleStateLog<S> + 'static,
    NT: StateTransferSendNode<STMsg<S>> + 'static,
{
    /// Create a new instance of `BtStateTransfer`.
    pub fn new(
        node: Arc<NT>,
        base_timeout: Duration,
        timeouts: Timeouts,
        persistent_log: PL,
        install_channel: ChannelSyncTx<InstallStateMessage<S>>,
    ) -> Self {
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
            checkpoint: PersistentCheckpoint::new(),

            received_state_ids: HashMap::default(),
            accepted_state_parts: Vec::new(),
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
    pub fn cst_request_timed_out<V>(&mut self, seq: SeqNo, view: V) -> bool
    where
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

    fn process_request_seq(&mut self, header: Header, message: StMessage<S>) {
        let seq = match &self.checkpoint.descriptor {
            Some(descriptor) => Some((
                descriptor.sequence_number(),
                descriptor.get_digest().clone(),
            )),
            None => None,
        };

        let kind = MessageKind::ReplyLatestSeq(seq.clone());

        let reply = StMessage::new(message.sequence_number(), kind);

        self.node.send(reply, header.from(), true);
    }

    pub fn request_state_descriptor<V>(&mut self, view: V)
    where
        V: NetworkView,
    {
        self.next_seq();

        let cst_seq = self.curr_seq;

        //info!("{:?} // Requesting latest state with cst msg seq {:?}", self.node.id(), cst_seq);

        self.timeouts
            .timeout_cst_request(self.curr_timeout, 1, cst_seq);

        self.phase = ProtoPhase::ReceivingStateDescriptor;

        //TODO: Maybe attempt to use followers to rebuild state and avoid
        // Overloading the replicas
        let message = StMessage::new(cst_seq, MessageKind::RequestStateDescriptor);
        let targets = view
            .quorum_members()
            .clone()
            .into_iter()
            .filter(|id| *id != self.node.id());

        self.node.broadcast(
            message,
            targets,
        );
    }
    /// Process the entire list of pending state transfer requests
    /// This will only reply to the latest request sent by each of the replicas
    fn process_pending_state_requests(&mut self) {
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

                self.process_request_state(header, message);
            });
        }
    }

    fn process_request_state(&mut self, header: Header, message: StMessage<S>) {
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

        let state = match self.checkpoint.descriptor() {
            Some(state) => state,
            _ => {
                if let ProtoPhase::WaitingCheckpoint(waiting) = &mut self.phase {
                    waiting.push(StoredMessage::new(header, message));
                } else {
                    self.phase =
                        ProtoPhase::WaitingCheckpoint(vec![StoredMessage::new(header, message)]);
                }

                return;
            }
        }
        .clone();

        let st_frag = match message.kind() {
            MessageKind::ReqState(req_parts) => self.checkpoint.get_parts(req_parts).unwrap(),
            _ => {
                return;
            }
        };

        let reply = StMessage::new(
            message.sequence_number(),
            MessageKind::ReplyState(RecoveryState {
                seq: state.sequence_number(),
                st_frag: Arc::new(ReadOnly::new(st_frag)),
            }),
        );

        self.node.send(reply, header.from(), true).unwrap();
    }

    pub fn process_request_descriptor(&mut self, header: Header, message: StMessage<S>) {


        match &mut self.phase {
            ProtoPhase::Init => {println!("phase Init");
        }
            ProtoPhase::WaitingCheckpoint(waiting) => {
                println!("phase Checkpoint");

                waiting.push(StoredMessage::new(header, message));

                return;
            }
            _ => {
                // We can't reply to state requests when requesting state ourselves
                return;
            }
        }

        let state = match &self.checkpoint.descriptor {
            Some(state) => state,
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

        println!("replying");

        let reply = StMessage::new(
            message.sequence_number(),
            MessageKind::ReplyStateDescriptor(Some(state.clone())),
        );

        self.node.send(reply, header.from(), true).unwrap();
    }

    fn process_message_inner<V>(&mut self, view: V, progress: StProgress<S>) -> StStatus<S>
    where
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
                    MessageKind::RequestLatestSeq => {
                        self.process_request_seq(header, message);
                    }
                    MessageKind::RequestStateDescriptor => {
                        self.process_request_descriptor(header, message)
                    }
                    MessageKind::ReqState(_) => {
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

                // debug!("{:?} // Received Cid with {} responses from {:?} for CST Seq {:?} vs Ours {:?}", self.node.id(),
                //    i, header.from(), message.sequence_number(), self.curr_seq);

                // drop cst messages with invalid seq no
                if message.sequence_number() != self.curr_seq {
                    //  debug!(
                    //      "{:?} // Wait what? {:?} {:?}",
                    //      self.node.id(),
                    //      self.curr_seq,
                    //      message.sequence_number()
                    //  );
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
                                //   info!("{:?} // Received newer state for old cid {:?} vs new cid {:?} with digest {:?}.",
                                //        self.node.id(), received_state_cid.cid, *cid, digest);

                                received_state_cid.cid = *cid;
                                received_state_cid.count = 1;
                            } else if *cid == received_state_cid.cid {
                                //   info!("{:?} // Received matching state for cid {:?} with digest {:?}. Count {}",
                                //   self.node.id(), received_state_cid.cid, digest, received_state_cid.count + 1);

                                received_state_cid.count += 1;
                            }
                        }
                    }
                    MessageKind::ReqState(_) => {
                        self.process_request_state(header, message);

                        return StStatus::Running;
                    }
                    _ => return StStatus::Running,
                }

                // check if we have gathered enough cid
                // replies from peer nodes
                //
                // TODO: check for more than one reply from the same node
                let i = i + 1;

                // debug!("{:?} // Quorum count {}, i: {}, cst_seq {:?}. Current Latest Cid: {:?}. Current Latest Cid Count: {}",
                //         self.node.id(), view.quorum(), i,
                //         self.curr_seq, self.largest_cid, self.latest_cid_count);

                if i == view.quorum() {
                    self.phase = ProtoPhase::Init;

                    // reset timeout, since req was successful
                    self.curr_timeout = self.base_timeout;

                    //  info!("{:?} // Identified the latest state seq no as {:?} with {} votes.",
                    //          self.node.id(),
                    //          self.largest_cid, self.latest_cid_count);

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

                let mut accepted_descriptor = Vec::new();
                for received_part in state.st_frag.iter() {
                    if self
                        .checkpoint
                        .req_parts
                        .contains(received_part.descriptor())
                    {
                        accepted_descriptor.push(received_part.descriptor().clone());
                        self.accepted_state_parts.push(received_part.clone());
                    }
                }

                let i = i + 1;

                if !self.checkpoint.req_parts.is_empty() {
                    //remove the parts that we accepted
                    self.checkpoint
                        .req_parts
                        .retain(|part| !accepted_descriptor.contains(part));
                    self.phase = ProtoPhase::ReceivingState(i);
                    return StStatus::Running;
                }

                // reset timeout, since req was successful
                self.curr_timeout = self.base_timeout;

                let st_frag = self.accepted_state_parts.drain(0..).collect();
                let st: RecoveryState<S> = RecoveryState {
                    seq: state.seq,
                    st_frag: Arc::new(ReadOnly::new(st_frag)),
                };

                self.accepted_state_parts.clear();

                if self.checkpoint.req_parts.is_empty() {
                    self.phase = ProtoPhase::Init;

                    StStatus::State(st)
                } else {
                    StStatus::PartiallyComplete(st)
                }
            }
            ProtoPhase::ReceivingStateDescriptor => {
                let (header, mut message) = getmessage!(progress, StStatus::RequestStateDescriptor);

                let descriptor = match message.take_descriptor() {
                    Some(descriptor) => descriptor,
                    None => return StStatus::Running,
                };

                return StStatus::StateDescriptor(descriptor);
            }
        }
    }

    pub fn request_latest_consensus_seq_no<V>(&mut self, view: V)
    where
        V: NetworkView,
    {
        // reset state of latest seq no. request
        self.largest_cid = SeqNo::ZERO;
        self.latest_cid_count = 0;

        self.next_seq();

        let cst_seq = self.curr_seq;

        //info!("{:?} // Requesting latest state seq no with seq {:?}", self.node.id(), cst_seq);

        self.timeouts
            .timeout_cst_request(self.curr_timeout, view.quorum() as u32, cst_seq);

        self.phase = ProtoPhase::ReceivingCid(0);

        let message = StMessage::new(cst_seq, MessageKind::RequestLatestSeq);

        let targets = view
            .quorum_members()
            .clone()
            .into_iter()
            .filter(|id| *id != self.node.id());

        self.node.broadcast(
            message,
            targets,
        );
    }

    fn request_latest_state<V>(&mut self, view: V) -> Result<()>
    where
        V: NetworkView,
    {
        //info!("{:?} // Requesting latest state with cst msg seq {:?}", self.node.id(), cst_seq);

        self.accepted_state_parts.clear();

        self.next_seq();

        let cst_seq = self.curr_seq;

        self.timeouts
            .timeout_cst_request(self.curr_timeout, view.quorum() as u32, cst_seq);

        self.phase = ProtoPhase::ReceivingState(0);

        //TODO: Maybe attempt to use followers to rebuild state and avoid
        // Overloading the replicas
        let mut message;
        let targets = view
            .quorum_members()
            .clone()
            .into_iter()
            .filter(|id| *id != self.node.id());
        let parts_map = self
            .checkpoint
            .distribute_req_parts(targets.clone(), view.n() - 1);

        for target in targets {
            message = StMessage::new(
                cst_seq,
                MessageKind::ReqState(parts_map.get(&target).unwrap().clone()),
            );

            self.node.send(
                message,
                target,
                true,
            );
        }

        Ok(())
    }

    fn next_seq(&mut self) -> SeqNo {
        self.curr_seq = self.curr_seq.next();

        self.curr_seq
    }
}

impl<S, NT, PL> PersistableStateTransferProtocol for BtStateTransfer<S, NT, PL>
where
    S: DivisibleState + 'static + Send + Clone,
    PL: DivisibleStateLog<S> + 'static,
{
}

impl<S, NT, PL> DivisibleStateTransfer<S, NT, PL> for BtStateTransfer<S, NT, PL>
where
    S: DivisibleState + 'static + Send + Clone + for<'a> Deserialize<'a> + Serialize,
    PL: DivisibleStateLog<S> + 'static,
    NT: StateTransferSendNode<STMsg<S>> + 'static,
{
    type Config = StateTransferConfig;

    fn initialize(
        config: Self::Config,
        timeouts: Timeouts,
        node: Arc<NT>,
        log: PL,
        executor_state_handle: ChannelSyncTx<InstallStateMessage<S>>,
    ) -> Result<Self>
    where
        Self: Sized,
    {
        let StateTransferConfig { timeout_duration } = config;

        Ok(Self::new(
            node,
            timeout_duration,
            timeouts,
            log,
            executor_state_handle,
        ))
    }

    fn handle_state_received_from_app<V>(
        &mut self,
        view: V,
        descriptor: S::StateDescriptor,
        state: Vec<S::StatePart>,
    ) -> Result<()>
    where
        V: NetworkView,
    {

        if self.needs_checkpoint() || self.checkpoint.get_digest() != Some(descriptor.get_digest())
        {
            println!("CHECKPOINTING");
            self.checkpoint.update(descriptor, state);
            //TODO: WRITE PARTS TO PERSISTENT LOG
            if let StStatus::Nil = self.process_message_inner(view, StProgress::Nil) {
            } else {
                return Err(
                    "Process message while needing checkpoint returned something else than nil",
                )
                .wrapped(ErrorKind::Cst);
            }
        }

        Ok(())
    }
}
