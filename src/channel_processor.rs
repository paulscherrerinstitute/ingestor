use bsread::{Bsread, EndpointEvent, EndpointState, EndpointDiag, Message, Pool, IOResult, IOError, ChannelConfig, ChannelData};
use crate::Arguments;
use std::sync::{Mutex, RwLock};
use std::sync::atomic::{AtomicU32, Ordering};
use std::fs::OpenOptions;
use std::io::{self, Write};
use std::path::PathBuf;
use chrono::Local;
use std::fs;

pub struct ChannelProcessor {
    arguments:Arguments,
    data_path: PathBuf,
}


impl ChannelProcessor {
    pub fn new(arguments:Arguments) -> Self {
        Self {
            arguments,
            data_path: PathBuf::from(std::env::var("HOME").unwrap()).join("data"),
        }
    }

    pub async fn process(&self, id: u64, tm: (u64, u64), config: ChannelConfig, data: Option<ChannelData>, header_changed: bool) {
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
        self.save_channel(id, tm, config.name(), arr, config.kind(), config.shape(), config.size());
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
