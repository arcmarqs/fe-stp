use std::sync::Weak;
use std::time::Duration;
use std::default::Default;
use std::io::{Read, Write};
use serde::{Serialize, Deserialize, Deserializer};

use konst::{
    primitive::{
        parse_usize,
        parse_bool,
        parse_u64,
    },
    option::unwrap_or,
    unwrap_ctx,
};
use serde::ser::SerializeStruct;

use atlas_common::error::*;
use atlas_execution::serialize::ApplicationData;
use atlas_execution::state::monolithic_state::MonolithicState;

pub struct MicrobenchmarkData;

impl MicrobenchmarkData {
    pub const REQUEST_SIZE: usize = {
        let result = parse_usize(env!("REQUEST_SIZE"));
        unwrap_ctx!(result)
    };

    pub const REPLY_SIZE: usize = {
        let result = parse_usize(env!("REPLY_SIZE"));
        unwrap_ctx!(result)
    };

    pub const STATE_SIZE: usize = {
        let result = parse_usize(env!("STATE_SIZE"));
        unwrap_ctx!(result)
    };

    pub const MEASUREMENT_INTERVAL: usize = {
        let result = parse_usize(env!("MEASUREMENT_INTERVAL"));
        unwrap_ctx!(result)
    };

    pub const OPS_NUMBER: usize = {
        let result = parse_usize(env!("OPS_NUMBER"));
        unwrap_ctx!(result)
    };

    pub const REQUEST_SLEEP_MILLIS: Duration = {
        let result = parse_u64(unwrap_or!(option_env!("REQUEST_SLEEP_MILLIS"), "0"));
        Duration::from_millis(unwrap_ctx!(result))
    };

    pub const VERBOSE: bool = {
        let result = parse_bool(unwrap_or!(option_env!("VERBOSE"), "false"));
        unwrap_ctx!(result)
    };

    pub(crate) const REQUEST: [u8; Self::REQUEST_SIZE] = [0; Self::REQUEST_SIZE];
    pub(crate) const REPLY: [u8; Self::REPLY_SIZE] = [0; Self::REPLY_SIZE];
    pub(crate) const STATE: [u8; Self::STATE_SIZE] = [0; Self::STATE_SIZE];
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Request {
    inner: Vec<u8>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Reply {
    inner: Vec<u8>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct State {
    inner: Vec<u8>,
}

impl Request {
    pub fn new(inner: [u8; MicrobenchmarkData::REQUEST_SIZE]) -> Self {
        Self { inner: Vec::from(inner) }
    }
}

impl Reply {
    pub fn new(inner: [u8; MicrobenchmarkData::REPLY_SIZE]) -> Self {
        Self { inner: Vec::from(inner) }
    }
}

impl State {
    pub fn new(inner: [u8; MicrobenchmarkData::STATE_SIZE]) -> Self {
        Self { inner: Vec::from(inner) }
    }
}

impl MonolithicState for State {

    fn serialize_state<W>(mut w: W, request: &Self) -> Result<()> where W: Write {
        w.write_all(&MicrobenchmarkData::STATE).wrapped_msg(ErrorKind::CommunicationSerialize, "Failed to serialize state")
    }

    fn deserialize_state<R>(r: R) -> Result<Self> where R: Read, Self: Sized {
        Ok(State { inner: Vec::from(MicrobenchmarkData::STATE) })
    }
}

impl ApplicationData for MicrobenchmarkData {
    type Request = Request;
    type Reply = Reply;

    fn serialize_request<W>(w: W, request: &Self::Request) -> Result<()> where W: Write {
        let mut root = capnp::message::Builder::new(capnp::message::HeapAllocator::new());

        let mut rq_msg: messages_capnp::benchmark_request::Builder = root.init_root();

        rq_msg.set_data(&request.inner);

        capnp::serialize::write_message(w, &root)
            .wrapped_msg(ErrorKind::CommunicationSerialize, "Failed to serialize request")
    }

    fn deserialize_request<R>(r: R) -> Result<Self::Request> where R: Read {
        let reader = capnp::serialize::read_message(r, Default::default()).wrapped_msg(ErrorKind::CommunicationSerialize,
                                                                                       "Failed to read message")?;

        let request_msg: messages_capnp::benchmark_request::Reader = reader.get_root()
            .wrapped_msg(ErrorKind::CommunicationSerialize, "Failed to read request message")?;

        let _data = request_msg.get_data().wrapped_msg(ErrorKind::CommunicationSerialize, "Failed to get data from request message?");

        Ok(Request {
            inner: Vec::from(MicrobenchmarkData::REQUEST)
        })
    }

    fn serialize_reply<W>(w: W, reply: &Self::Reply) -> Result<()> where W: Write {
        let mut root = capnp::message::Builder::new(capnp::message::HeapAllocator::new());

        let mut rq_msg: messages_capnp::benchmark_reply::Builder = root.init_root();

        rq_msg.set_data(&reply.inner);

        capnp::serialize::write_message(w, &root)
            .wrapped_msg(ErrorKind::CommunicationSerialize, "Failed to serialize reply")
    }

    fn deserialize_reply<R>(r: R) -> Result<Self::Reply> where R: Read {
        let reader = capnp::serialize::read_message(r, Default::default()).wrapped_msg(ErrorKind::CommunicationSerialize,
                                                                                       "Failed to read message")?;

        let request_msg: messages_capnp::benchmark_reply::Reader = reader.get_root()
            .wrapped_msg(ErrorKind::CommunicationSerialize, "Failed to read reply message")?;

        let _data = request_msg.get_data().wrapped_msg(ErrorKind::CommunicationSerialize, "Failed to get data from reply message?");

        Ok(Reply {
            inner: Vec::from(MicrobenchmarkData::REPLY)
        })
    }
}

mod messages_capnp {
    #![allow(unused)]
    include!(concat!(env!("OUT_DIR"), "/messages_capnp.rs"));
}