use std::collections::HashMap;
use std::io::ErrorKind;
use std::sync::Arc;
use std::thread;
use bsread::{Bsread, EndpointEvent, EndpointState, EndpointDiag, Message, Pool, IOResult, IOError, ChannelConfig, ChannelData};
use crate::Arguments;
use std::sync::{Mutex, RwLock};
use std::sync::atomic::{AtomicU32, Ordering};
use futures::future::join_all;
use std::io::{self, Write};
use crate::channel_processor::ChannelProcessor;

pub struct Processor {
    arguments:Arguments,
    last_ids:Arc<Mutex<HashMap<String, u64>>>,
    channel_processor: Arc<ChannelProcessor>,
}


impl Processor {
    pub fn new(arguments:Arguments, channel_processor: Arc<ChannelProcessor>) -> Self {
        Self {
            arguments,
            last_ids: Arc::new(Mutex::new(HashMap::new())),
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
        //if id % 2 ==0{thread::sleep(Duration::from_millis(50)); }
        let endpoint = endpoint.clone().unwrap_or(String::from(""));
        //println!("{:?} {:?}, {}", thread::current().id(), endpoint, id);
        let mut last_ids = self.last_ids.lock().unwrap();
        let last_id = last_ids.get(endpoint.as_str()).unwrap_or(&(0)).clone();
        if last_id > 0 {
            if last_id >= id {
                return Err(IOError::new(ErrorKind::Other, format!("Received unordered message for {:?} last: {:?}, id: {}", endpoint, last_id, id)));
            } else if id != last_id + 1 {
                log::warn!("Missed ID from  {:?} last: {:?}, id: {}", endpoint, last_id, id);
            }
        }
  last_ids.insert(endpoint.to_string(), id);
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
}