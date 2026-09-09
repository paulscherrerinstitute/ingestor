use crate::Arguments;
use crate::ingestor::Ingestor;
use bsread::ChannelConfig;
use chrono::Local;
use log::LevelFilter;
use std::fs;
use std::fs::OpenOptions;
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::Arc;

pub struct ChannelProcessor {
    arguments: Arc<Arguments>,
    ingestor: Arc<Ingestor>,
}


impl ChannelProcessor {
    pub fn new(arguments: Arc<Arguments>, ingestor: Arc<Ingestor>) -> Self {
        Self {arguments, ingestor}
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
            self.ingestor.create_table(config.name(), config.kind(), config.shape(), config.size()).await;
        }
        let (name, kind, shape) = config.into_parts();
        self.ingestor.append_record(name, data).await;
    }

 
    fn print_channel(id: u64, tm: (u64, u64), name:String, data:&Option<Vec<u8>>, kind:String, shape:Option<Vec<u32>>, size:usize) {
        println!("Channel {} id:{} data:{:?} type:{} shape:{:?} size:{}", name, id, data, kind, shape, size);
    }

    fn save_channel(path: &String, id: u64, tm: (u64, u64), name:String, data:&Option<Vec<u8>>, kind:String, shape:Option<Vec<u32>>, size:usize) {
        let path = if let Some(rest) = path.strip_prefix("~/") {
            PathBuf::from(std::env::var("HOME").unwrap()).join(rest)
        } else {
            PathBuf::from(path)
        };
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
