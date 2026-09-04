use std::collections::HashMap;
use std::io::ErrorKind;
use std::sync::Arc;
use std::thread;
use bsread::{Bsread, EndpointEvent, EndpointState, EndpointDiag, Message, Pool, IOResult, IOError, ChannelConfig, ChannelData, SocketType};
use crate::Arguments;
use std::sync::{Mutex, RwLock};
use std::sync::atomic::{AtomicU32, Ordering};
use futures::future::join_all;
use std::io::{self, Write};
use serde::Serialize;
use crate::channel_processor::ChannelProcessor;



#[derive(Debug, Clone, Serialize)]
pub struct ChannelInfo{
    name: String,
    kind: String,
    shape: Vec<u32>
}

#[derive(Debug, Clone, Serialize)]
pub struct SourceInfo{
    last_id: u64,
    last_received: i64,
    age: i64,
    channels: Vec<ChannelInfo>,
}

impl SourceInfo {
    fn new() -> SourceInfo {
        SourceInfo{last_id: 0, last_received: 0, age: 0, channels: Vec::new()}
    }
}
pub struct Processor {
    arguments:Arguments,
    sources_info:Arc<RwLock<HashMap<String, SourceInfo>>>,
    channel_processor: Arc<ChannelProcessor>,
}


impl Processor {
    pub fn new(arguments:Arguments, channel_processor: Arc<ChannelProcessor>) -> Self {
        Self {
            arguments,
            sources_info: Arc::new(RwLock::new(HashMap::new())),
            channel_processor,
        }
    }

    pub async fn process(&self, endpoint: Option<String>, message: Message){
        if let Err(err) = self.check_msg(&endpoint, &message){
            log::error!("Message check failed: {}", err);
        } else {
            let id = message.id();
            let tm = message.timestamp();
            let header_changed = message.header_changed();
            let (channels, data) = message.into_parts();
            let mut index = 0;
            //println!("Message {} from {:?} changed {}", id, &endpoint, header_changed);

            //for (channel, (_key, value)) in channels.into_iter().zip(data) {
            //    self.process_channel(id,tm,channel.into_config(),value,header_changed,).await;
            //}


            if self.arguments.join_channels {
                let futures = channels.into_iter().zip(data)
                    .filter_map(|(channel, (_key, value))| {
                        let config = channel.into_config();
                        match Self::as_bytes(&config, value) {
                            Ok(data) => Some(
                                self.channel_processor.process(id, tm, config, data,header_changed,)
                            ),
                            Err(err) => {None}
                        }
                    });
                join_all(futures).await;
            } else {
                for (channel, (_key, value)) in channels.into_iter().zip(data) {
                    let channel_processor = Arc::clone(&self.channel_processor);
                    tokio::spawn(async move {
                        let config = channel.into_config();
                        if let Ok(data) = Self::as_bytes(&config, value) {
                            channel_processor.process(id, tm, config, data, header_changed).await;
                        }
                    });
                }
            }
        }
        //tokio::time::sleep(Duration::from_millis(500)).await;
    }

    fn check_msg(&self, endpoint: &Option<String>, message: &Message) -> IOResult<()> {
        let id = message.id() ;
        //let endpoint = endpoint.clone().unwrap_or(String::from(""));
        let endpoint = match endpoint {
            Some(endpoint) => endpoint,
            None => {""}
        };

        let mut sources_info = self.sources_info.write().unwrap();
        let mut source_info = match sources_info.get_mut(endpoint) {
            Some(map) => map,
            None => {
                let source_info = SourceInfo::new();
                sources_info.entry(endpoint.to_owned()).or_insert(source_info)
            },

        };

        if source_info.last_id > 0 {
            if source_info.last_id >= id {
                return Err(IOError::new(ErrorKind::Other, format!("Received unordered message for {:?} last: {:?}, id: {}", endpoint, source_info.last_id, id)));
            } else if id != source_info.last_id + 1 {
                //TODO: Remove
                log::warn!("Missed ID from  {:?} last: {:?}, id: {}", endpoint, source_info.last_id, id);
            }
        }

        if message.header_changed() {
            let mut channels = Vec::new();
            for channel in message.channels().iter() {
                let config  = channel.config();
                channels.push(ChannelInfo{name: config.name(), kind: config.kind(), shape: config.shape().unwrap_or(Vec::new())});
            }
            source_info.channels = channels;
        }

        let now = chrono::Local::now();
        source_info.age = if  now.timestamp() > source_info.last_received {
            now.timestamp() - source_info.last_received
        } else {
            0
        };
        source_info.last_received = now.timestamp();
        source_info.last_id = id;
        Ok(())
    }

    fn as_bytes(config: &ChannelConfig, data: Option<ChannelData>) -> IOResult<Option<Vec<u8>>> {
         match data {
            Some(data) => match data.into_value().into_bytes() {
                Some(arr) => {
                    if arr.len() != config.size() {
                        if config.kind() != "string" { //What to do for variable-lenght strings?
                            return Err(IOError::new(ErrorKind::Other, format!("Channel {} data lenght {} is different from configuration size {}", config.name(), arr.len(), config.size())));
                        }
                    }
                    Ok(Some(arr))
                },
                None => {
                    Err(IOError::new(ErrorKind::Other, format!("Channel {} data is not u8 array: {} raw={}", config.name(), config.kind(), config.is_raw())))
                }
            },
            None => Ok(None)
        }
    }

    pub async fn on_endpoint_state(&self, endpoint: String, state: EndpointState) {
        let timestamp = chrono::Local::now().format("%H:%M:%S");
        println!("{} - Endpoint {} state: {:?}", timestamp, endpoint, state);
    }

    pub async fn on_endpoint_diag(&self, endpoint: String, diag: EndpointDiag, id: Option<u64>) {
        let timestamp = chrono::Local::now().format("%H:%M:%S");
        println!("{} - Endpoint {} id {:?} diag: {:?}", timestamp, endpoint, id, diag);
    }


    pub async fn sources_info(&self) -> IOResult<HashMap<String, SourceInfo>> {
        Ok(self.sources_info.read().unwrap().clone())
    }
}