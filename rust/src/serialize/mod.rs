use std::mem::size_of;
use std::sync::{Weak, Arc};
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
use atlas_divisible_state::*;

pub struct KvData;


#[derive(Debug,Clone,Serialize,Deserialize)]
pub enum Action {
    Get(String),
    Set(String, u64),
    Remove(String),
}

impl ApplicationData for KvData{
    type Request = Arc<Action>;
    type Reply = Arc<Option<u64>>;

    fn serialize_request<W>(w: W, request: &Self::Request) -> Result<()> where W: Write {
        let mut root = capnp::message::Builder::new(capnp::message::HeapAllocator::new());

        let mut rq_msg: messages_capnp::request::Builder = root.init_root();
    
        rq_msg.set_data();

        capnp::serialize::write_message(w, &root)
            .wrapped_msg(ErrorKind::CommunicationSerialize, "Failed to serialize request")
    }

    fn deserialize_request<R>(r: R) -> Result<Self::Request> where R: Read {

        let reader = capnp::serialize::read_message(r, Default::default()).wrapped_msg(ErrorKind::CommunicationSerialize,
        "Failed to read message")?;

        let request_msg : messages_capnp::request::Reader = reader.get_root()
            .wrapped_msg(ErrorKind::CommunicationSerialize, "Failed to read request message")?;

            let _data = request_msg.get_data().wrapped_msg(ErrorKind::CommunicationSerialize, "Failed to get data")?;
    
            Ok(Arc::new(_data))
        
    }

    fn serialize_reply<W>(w: W, reply: &Self::Reply) -> Result<()> where W: Write {
        let mut root = capnp::message::Builder::new(capnp::message::HeapAllocator::new());

        let mut rq_msg: messages_capnp::reply::Builder = root.init_root();
        rq_msg.set_data(**reply);
      
        capnp::serialize::write_message(w, &root)
            .wrapped_msg(ErrorKind::CommunicationSerialize, "Failed to serialize reply")
    }

    fn deserialize_reply<R>(r: R) -> Result<Self::Reply> where R: Read {

        let reader = capnp::serialize::read_message(r, Default::default()).wrapped_msg(ErrorKind::CommunicationSerialize,
                                                                                       "Failed to read message")?;

        let request_msg : messages_capnp::reply::Reader = reader.get_root()
            .wrapped_msg(ErrorKind::CommunicationSerialize, "Failed to read reply message")?;

        let _data = request_msg.get_data();

        Ok(Arc::new(_data))
    }
}

mod messages_capnp {
    #![allow(unused)]
    include!(concat!(env!("OUT_DIR"), "/messages_capnp.rs"));
}