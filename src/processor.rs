use crate::Arguments;
use crate::channel_processor::ChannelProcessor;
use bsread::{ChannelConfig, ChannelData, EndpointDiag, EndpointEvent, EndpointState, IOError, IOResult, Message, Pool, Receiver, ScalarType, SocketType};
use futures::future::join_all;
use serde::Serialize;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::ErrorKind;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU32, Ordering, AtomicUsize, AtomicU64};
use std::sync::{RwLock};
use tokio::runtime::Handle;
use crate::arguments::{ChannelProcessing, MetadataProcessing};

#[derive(Debug, Clone, Serialize)]
pub struct ChannelInfo{
    name: String,
    kind: ScalarType,
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

pub struct ProcessorStats {
    pub pending: AtomicU64,
    pub treated: AtomicU64,
    pub dropped: AtomicU64,
}

struct ChannelMessage{
    id:u64,
    tm:(u64,u64),
    config:ChannelConfig,
    data: Option<Vec<u8>>,
    header_changed:bool,
}

pub struct Processor {
    arguments:Arc<Arguments>,
    sources_info:Arc<RwLock<HashMap<String, SourceInfo>>>,
    channel_processor: Arc<ChannelProcessor>,
    stats: Arc<ProcessorStats>,
    channel_buffers:Arc<RwLock<HashMap<String,tokio::sync::mpsc::Sender<ChannelMessage>>>>,
    channel_shards: OnceLock<Vec<tokio::sync::mpsc::Sender<ChannelMessage>>>,
}

pub enum MetadataCommand {
    Insert {
        name:String,  id: u64, kind:ScalarType, elements:usize
    },
}


impl Processor {
    pub fn new(arguments:Arc<Arguments>, channel_processor: Arc<ChannelProcessor>) -> Self {
        let sources_info = Arc::new(RwLock::new(HashMap::new()));
        let stats = Arc::new(ProcessorStats {pending:AtomicU64::new(0), treated:AtomicU64::new(0), dropped:AtomicU64::new(0)});
        let channel_buffers =Arc::new(RwLock::new(HashMap::new()));
        let channel_shards = OnceLock::new();
        Self {arguments, sources_info, channel_processor, stats,channel_buffers, channel_shards}
    }

    pub async fn process(&self, endpoint: Option<String>, message: Message) {
        if let Err(err) = self.check_msg(&endpoint, &message){
            log::debug!("Message check failed: {}", err);
        } else {
            self.check_metadata(&message, &Handle::current());
            let handle = Handle::current();
            match self.arguments.channel_processing {
                ChannelProcessing::Sync => self.process_sync(message).await,
                ChannelProcessing::Async => self.process_async(message, &handle),
                ChannelProcessing::Joined => self.process_joined(message).await,
                ChannelProcessing::Buffered => self.process_buffered(message, &Handle::current()),
                ChannelProcessing::Sharded => self.process_sharded(message, &handle),
            }
        }
    }

    pub fn process_direct(&self, endpoint: Option<String>, message: Message, handle: &Handle) {
        if let Err(err) = self.check_msg(&endpoint, &message){
            log::debug!("Message check failed: {}", err);
        } else {
            self.check_metadata(&message, handle);

            match self.arguments.channel_processing {
                ChannelProcessing::Async => self.process_async(message, handle),
                ChannelProcessing::Buffered => self.process_buffered(message, handle),
                ChannelProcessing::Sharded => self.process_sharded(message, &handle),
                _ => {
                    log::error!(
                    "Channel processing mode {:?} cannot use direct message processing",
                    self.arguments.channel_processing
                );
                }
            }
        }
    }

    fn check_metadata(&self, message: &Message, handle: &Handle){
        if message.header_changed()  {
            let id = message.id();
            for channel in message.channels() {
                let config = channel.config();
                self.channel_processor.process_metadata(config.name(), id, config.kind(),config.elements(), &handle);
            }
        }
    }

    async fn process_sync(&self, message: Message){
        let (id, tm, header_changed, channels, data) = message.into_parts();
        for (channel, (_key, value)) in channels.into_iter().zip(data) {
            let config = channel.into_config();
            if let Ok(data) = Self::as_bytes(&config, value) {
                self.channel_processor.process_data(id, tm, config, data, header_changed).await;
            }
            self.stats.treated.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn process_async(&self, message: Message, handle: &Handle){
        let (id, tm, header_changed, channels, data) = message.into_parts();
        for (channel, (_key, value)) in channels.into_iter().zip(data) {
            let channel_processor = Arc::clone(&self.channel_processor);
            let stats = Arc::clone(&self.stats);
            stats.pending.fetch_add(1, Ordering::Relaxed);
            handle.spawn(async move {
                let config = channel.into_config();
                if let Ok(data) = Self::as_bytes(&config, value) {
                    channel_processor.process_data(id, tm, config, data, header_changed).await;
                }
                stats.pending.fetch_sub(1, Ordering::Relaxed);
                stats.treated.fetch_add(1, Ordering::Relaxed);
            });
        }
    }

    async fn process_joined(&self, message: Message){
        let (id, tm, header_changed, channels, data) = message.into_parts();
        let stats = Arc::clone(&self.stats);
        let futures = channels.into_iter().zip(data)
            .filter_map(|(channel, (_key, value))| {
                let config = channel.into_config();
                match Self::as_bytes(&config, value) {
                    Ok(data) => {
                        let channel_processor = Arc::clone(&self.channel_processor);
                        let stats = Arc::clone(&stats);
                        Some(async move {
                            channel_processor.process_data(id, tm, config, data, header_changed).await;
                            stats.treated.fetch_add(1, Ordering::Relaxed);
                        })
                    }
                    Err(err) => {
                        log::debug!("Skipping channel on message {}: {}", id, err);
                        None
                    }
                }
            });
        join_all(futures).await;
    }

    fn process_buffered(&self, message: Message, handle: &Handle){
        let (id, tm, header_changed, channels, data) = message.into_parts();
        for (channel, (_key, value)) in channels.into_iter().zip(data) {
            let config = channel.into_config();
            let channel_name = config.name();
            let stats = Arc::clone(&self.stats);
            let channel_processor = Arc::clone(&self.channel_processor);

            if let Ok(data) = Self::as_bytes(&config, value) {
                let sender = {
                    self.channel_buffers.read().unwrap().get(&channel_name).cloned()
                };
                let sender = match sender {
                    Some(sender) => sender,
                    None => {
                        let (tx, mut rx) = tokio::sync::mpsc::channel::<ChannelMessage>(self.arguments.buffer_size);
                        handle.spawn(async move {
                            while let Some(msg) = rx.recv().await {
                                channel_processor.process_data(msg.id, msg.tm, msg.config, msg.data, msg.header_changed).await;
                                stats.pending.fetch_sub(1, Ordering::Relaxed);
                                stats.treated.fetch_add(1, Ordering::Relaxed);
                            }
                        });
                        let mut senders =self.channel_buffers.write().unwrap();
                        senders.entry(channel_name).or_insert_with(|| tx).clone()
                    }
                };
                match sender.try_send(ChannelMessage{id, tm, config, data, header_changed}) {
                    Ok(()) => {
                        self.stats.pending.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Full(msg)) => {
                        self.stats.dropped.fetch_add(1, Ordering::Relaxed);
                        log::debug!("Dropping message {} from {:?}: channel queue is full",id, msg.config.name());
                    }
                    Err(err) => {
                        log::error!("Error trying sending message: {:?}", err);
                    }
                }
            }
        }
    }


    fn process_sharded(&self, message: Message, handle: &Handle){
        let shards = self.channel_shards.get_or_init(|| {
            let mut shards = Vec::with_capacity(self.arguments.channel_shards);
            for shard_index in 0..self.arguments.channel_shards {
                let (tx, mut rx) = tokio::sync::mpsc::channel::<ChannelMessage>(self.arguments.buffer_size);
                let channel_processor = Arc::clone(&self.channel_processor);
                let stats = Arc::clone(&self.stats);
                handle.spawn(async move {
                    while let Some(msg) = rx.recv().await {
                        channel_processor
                            .process_data(msg.id, msg.tm, msg.config, msg.data, msg.header_changed)
                            .await;
                        stats.pending.fetch_sub(1, Ordering::Relaxed);
                        stats.treated.fetch_add(1, Ordering::Relaxed);
                    }

                    log::debug!("Channel shard {} stopped", shard_index);
                });
                shards.push(tx);
            }
            shards
        });

        let (id, tm, header_changed, channels, data) = message.into_parts();
        for (channel, (_key, value)) in channels.into_iter().zip(data) {
            let config = channel.into_config();
            let channel_name = config.name();

            if let Ok(data) = Self::as_bytes(&config, value) {
                let shard_index = Self::channel_shard(&channel_name, shards.len());
                let sender = &shards[shard_index];
                match sender.try_send(ChannelMessage{id, tm, config, data, header_changed}) {
                    Ok(()) => {
                        self.stats.pending.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Full(msg)) => {
                        self.stats.dropped.fetch_add(1, Ordering::Relaxed);
                        log::debug!("Dropping message {} from {:?}: channel shard {} queue is full",id,msg.config.name(),shard_index
                        );
                    }
                    Err(err) => {
                        log::error!("Error trying sending message to channel shard: {:?}", err);
                    }
                }
            }
        }
    }

    fn channel_shard(channel_name: &str, shard_count: usize) -> usize {
        let mut hasher = DefaultHasher::new();
        channel_name.hash(&mut hasher);
        (hasher.finish() as usize) % shard_count
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
                return Err(IOError::other( format!("Received unordered message for {:?} last: {:?}, id: {}", endpoint, source_info.last_id, id)));
            } else if id != source_info.last_id + 1 {
                //For testing if no message is missed
                //log::warn!("Missed ID from  {:?} last: {:?}, id: {}", endpoint, source_info.last_id, id);
            }
        }

        if message.header_changed() || source_info.channels.is_empty() {
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
                        if config.kind() != ScalarType::string { //What to do for variable-lenght strings?
                            return Err(IOError::other( format!("Channel {} data lenght {} is different from configuration size {}", config.name(), arr.len(), config.size())));
                        }
                    }
                    Ok(Some(arr))
                },
                None => {
                    Err(IOError::other( format!("Channel {} data is not u8 array: {} raw={}", config.name(), config.kind(), config.is_raw())))
                }
            },
            None => Ok(None)
        }
    }

    pub async fn on_endpoint_state(&self, endpoint: String, state: EndpointState) {
        log::info!("Endpoint {} state: {:?}", endpoint, state);
    }

    pub async fn on_endpoint_diag(&self, endpoint: String, diag: EndpointDiag, id: Option<u64>) {
        log::debug!("Endpoint {} id {:?} diag: {:?}", endpoint, id, diag);
    }


    pub async fn sources_info(&self) -> IOResult<HashMap<String, SourceInfo>> {
        Ok(self.sources_info.read().unwrap().clone())
    }

    pub async fn source_info(&self, source:&str) -> IOResult<SourceInfo> {
        match self.sources_info.read().unwrap().get(source){
            None => {Err(IOError::new(ErrorKind::InvalidInput, format!("Source not found: {}", source)))},
            Some(info) => {Ok(info.clone())}
        }
    }

    pub fn pending(&self) -> u64 {
        self.stats.pending.load(Ordering::Relaxed)
    }

    pub fn treated(&self) -> u64 {
        self.stats.treated.load(Ordering::Relaxed)
    }

    pub fn dropped(&self) -> u64 {
        self.stats.dropped.load(Ordering::Relaxed)
    }

    pub fn metadata_pending(&self) -> u64 {self.channel_processor.metadata_stats.pending.load(Ordering::Relaxed)}

    pub fn metadata_treated(&self) -> u64 {self.channel_processor.metadata_stats.treated.load(Ordering::Relaxed)}

    pub fn metadata_dropped(&self) -> u64 {self.channel_processor.metadata_stats.dropped.load(Ordering::Relaxed)}

    pub fn channel_buffers(&self) -> u32 {
        self.channel_buffers.read().unwrap().len() as u32
    }

    pub fn reset_stats(&self) {
        self.stats.treated.store(0, Ordering::Relaxed);
        self.stats.pending.store(0, Ordering::Relaxed);
        self.stats.dropped.store(0, Ordering::Relaxed);
        self.channel_processor.metadata_stats.treated.store(0, Ordering::Relaxed);
        self.channel_processor.metadata_stats.pending.store(0, Ordering::Relaxed);
        self.channel_processor.metadata_stats.dropped.store(0, Ordering::Relaxed);
    }

    pub async fn on_timer(&self, minute_changed: bool, hour_changed: bool, day_changed: bool) -> IOResult<()> {
        Ok(())
    }
}