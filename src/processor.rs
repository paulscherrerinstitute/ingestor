use std::collections::HashMap;
use std::io::ErrorKind;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use bsread::{Bsread, EndpointEvent, EndpointState, EndpointDiag, Message, Pool, IOResult, IOError, ChannelConfig, ChannelData};
use tokio::runtime::Handle;
use crate::Arguments;
use std::sync::{Mutex, RwLock};
use std::sync::atomic::{AtomicU32, Ordering};
use bsread::debug::print_channel_data;
use futures::future::join_all;
use std::fs::OpenOptions;
use std::io::{self, Write};
use std::path::PathBuf;
use chrono::Local;
use std::fs;

pub struct Processor {
    last_ids:Arc<Mutex<HashMap<String, u64>>>,
    data_path: PathBuf,
}


impl Processor {
    pub fn new() -> Self {
        Self {
            last_ids: Arc::new(Mutex::new(HashMap::new())),
            data_path: PathBuf::from(std::env::var("HOME").unwrap()).join("data"),
        }
    }

    pub async fn process(&self, endpoint: Option<String>, message: Message) {
        //Limit scope of mutex, so tokio::time::sleep, othewise future needs to be Future<Output = ()> + Send + 'static
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

            let futures = channels.into_iter().zip(data)
                .map(|(channel, (_key, value))| {
                    self.process_channel(id, tm, channel.into_config(), value, header_changed)
                });

            join_all(futures).await;
        }
        //tokio::time::sleep(Duration::from_millis(500)).await;
    }

    pub async fn process_channel(&self, id: u64, tm: (u64, u64), config: ChannelConfig, data: Option<ChannelData>, header_changed: bool) {
        let arr = match data {
            Some(data) => match data.into_value().into_bytes() {
                Some(arr) => {
                    if arr.len() != config.size() {
                        if config.kind() != "string" { //What to do for variable-lenght strings?
                            log::error!("Channel {} data lenght {} is different from configuration size {}", config.name(), arr.len(), config.size());
                            self.print_channel(id, tm, config.name(), Some(arr), config.kind(), config.shape(), config.size());
                            return;
                        }
                    }
                    Some(arr)
                },
                None => {
                    log::error!("Channel {} data is not u8 array: {} raw={}", config.name(), config.kind(), config.is_raw());
                    return;
                }
            },
            None => None,
        };
        //self.save_channel(id, tm, config.name(), arr, config.kind(), config.shape(), config.size());
    }

    pub fn print_channel(&self, id: u64, tm: (u64, u64), name:String, data:Option<Vec<u8>>, kind:String, shape:Option<Vec<u32>>, size:usize) {
        println!("Channel {} id:{} data:{:?} type:{} shape:{:?} size:{}", name, id, data, kind, shape, size);
    }
    pub fn save_channel(&self, id: u64, tm: (u64, u64), name:String, data:Option<Vec<u8>>, kind:String, shape:Option<Vec<u32>>, size:usize) {
        let filename = format!("{}.bin",  Local::now().format("%Y%m%d").to_string());
        let path = self.data_path.join(&name).join(size.to_string()).join(filename);

        if let Err(err) = self.append_record(path, id, tm, data, size){
            log::error!("Failed to save channel {} id {}: {}", name, id, err);
        }
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
                log::error!("Received unordered message for {:?} last: {:?}, id: {}", endpoint, last_id, id);
                return Err(IOError::new(ErrorKind::Other, format!("Received unordered message for {:?} last: {:?}, id: {}", endpoint, last_id, id)));
            } else if id != last_id + 1 {
                log::warn!("Missed ID from  {:?} last: {:?}, id: {}", endpoint, last_id, id);
            }
        }
  last_ids.insert(endpoint.to_string(), id);
        Ok(())
    }


    pub async fn on_endpoint_state(&self, endpoint: String, state: EndpointState) {
        let timestamp = chrono::Local::now().format("%H:%M:%S");
        println!("{} - Endpoint {} state: {:?}", timestamp, endpoint, state);
    }

    pub async fn on_endpoint_diag(&self, endpoint: String, diag: EndpointDiag, id: Option<u64>) {
        let timestamp = chrono::Local::now().format("%H:%M:%S");
        println!("{} - Endpoint {} id {:?} diag: {:?}", timestamp, endpoint, id, diag);
    }


    fn append_record(&self, path:PathBuf, id: u64, tm: (u64, u64), data:Option<Vec<u8>>, size:usize) -> io::Result<()> {
        //println!("Appending record for {:?} id:{} data:{:?}", path, id, data);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut file = OpenOptions::new().create(true).append(true).open(path)?;
        // Choose an explicit byte order.
        file.write_all(&id.to_le_bytes())?;
        file.write_all(&tm.0.to_le_bytes())?;
        file.write_all(&tm.1.to_le_bytes())?;
        if let Some(data) = data {
            file.write_all(&[1])?;
            file.write_all(&data)?;
        } else {
            file.write_all(&[0])?;
            file.write_all(&vec![0u8; size])?;
        }
        Ok(())
    }
}