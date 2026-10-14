use std::collections::{HashMap, HashSet};
use crate::Arguments;
use crate::ingestor::{Ingestor};
use bsread::{ChannelConfig, IOResult, ScalarType};
use chrono::Local;
use log::LevelFilter;
use std::fs;
use std::fs::OpenOptions;
use std::io::{self, Write, ErrorKind};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock, atomic::{AtomicU64, Ordering}};
use crate::app::State::Paused;

struct ChannelError {
    kind: ErrorKind,
    suppressed: AtomicU64,
}

pub struct ChannelProcessor {
    arguments: Arc<Arguments>,
    ingestor: Arc<Ingestor>,
    channel_errors: RwLock<HashMap<String, ChannelError>>,
}

const METADATA_ERRORS_NAME: &str = "_metadata_";

impl ChannelProcessor {
    pub fn new(arguments: Arc<Arguments>, ingestor: Arc<Ingestor>) -> Self {
        Self {arguments, ingestor, channel_errors:RwLock::new(HashMap::new())}
    }

    pub async fn process(&self, id: u64, tm: (u64, u64), config: ChannelConfig, data: Option<Vec<u8>>, header_changed: bool) {
        if self.arguments.debug {
            Self::print_channel(id, tm, config.name(), &data, config.kind(), config.shape(), config.size());
        }
        if let Some(path) = &self.arguments.output_path {
            Self::save_channel(path, id, tm, config.name(), &data, config.kind(), config.shape(), config.size());
        }
        if log::max_level() == LevelFilter::Trace {
            log::trace!("Processing Channel name:{} type:{:?} shape:{:?} [ID:{}]", config.name(), config.kind(), config.shape(), id);
        }

        if header_changed {
            let result = self.ingestor.on_header_change(&config.name(), config.kind(), &config.shape(), config.elements(), config.size(), id, tm).await;
            self.check_append_result(id, METADATA_ERRORS_NAME, result); // Should always be ok, errors handled in metadata ingestion
        }

        let (name, kind, shape, elements) = config.into_parts();
        let result = self.ingestor.append_record(&name,kind, &shape, elements, id, tm, data).await;
        self.check_append_result(id, &name, result);
    }


    fn check_append_result(&self, id: u64, name:&str, result:IOResult<()>){
        if let Err(e) = result{
            if self.channel_errors.read().unwrap().get(name).map(|error| {error.suppressed.fetch_add(1, Ordering::Relaxed);}).is_none() {
                self.channel_errors.write().unwrap().insert(name.to_string(), ChannelError{ kind: e.kind(), suppressed: AtomicU64::new(0)});
                log::error!("Error appending record id {} for channel {}: {}",id, name, e);
            }
        } else {
            if  self.channel_errors.read().unwrap().contains_key(name) {
                if self.ingestor.enabled() {
                    if let Some(error) = self.channel_errors.write().unwrap().remove(name) {
                    log::info!("Channel {} recovered on id {}: suppressed {} errors", name,  id, error.suppressed.load(Ordering::Relaxed)); }
                }
            }
        }
    }

 
    fn print_channel(id: u64, tm: (u64, u64), name:String, data:&Option<Vec<u8>>, kind:ScalarType, shape:Option<Vec<u32>>, size:usize) {
        println!("Channel {} id:{} data:{:?} type:{} shape:{:?} size:{}", name, id, data, kind, shape, size);
    }

    fn save_channel(path: &PathBuf, id: u64, tm: (u64, u64), name:String, data:&Option<Vec<u8>>, kind:ScalarType, shape:Option<Vec<u32>>, size:usize) {
        let filename = format!("{}.bin", Local::now().format("%Y%m%d").to_string());
        let path = path.join(&name).join(size.to_string()).join(filename);
        if let Err(err) = Self::append_record(path, id, tm, data, size) {
            log::error!("Failed to save channel {} id {}: {}", name, id, err);
        }
    }

    fn append_record(path:PathBuf, id: u64, tm: (u64, u64), data:&Option<Vec<u8>>, size:usize) -> io::Result<()> {
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
