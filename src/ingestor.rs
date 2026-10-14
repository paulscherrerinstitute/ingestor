use std::collections::HashMap;
use std::io::ErrorKind;
use crate::Arguments;
use crate::DB;
use crate::cql;
use crate::codec::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use bsread::{channel, IOError, IOResult, ScalarType, SocketType};
use log::error;
use scylla::statement::prepared::PreparedStatement;
use tokio::sync::RwLock;
use futures::future::join_all;
use crate::arguments::StorageLayout;
use crate::config::SocketKind;
use crate::engine::EngineCommand;

pub const TABLE_SCALARS:&str = "measurements";
pub const TABLE_WAVEFORMS:&str = "waveforms";
pub const TABLE_STRINGS:&str = "strings";

pub const TABLE_CHANNEL_METADATA:&str = "channel_metadata";

pub const BUCKET_DIVISOR_SCALARS:i64 = 1_000_000;
pub const BUCKET_DIVISOR_STRINGS:i64 = 100_000;
pub const BUCKET_DIVISOR_WAVEFORMS:i64 = 10_000;

pub fn get_table_name(layout:&StorageLayout, channel_name:&str, kind:ScalarType, shape:&Option<Vec<u32>>) -> &'static str {
    match layout {
        StorageLayout::Typed => {
            if channel::is_array(shape) {
                TABLE_WAVEFORMS
            }  else {
                kind_to_cql_type(kind)
            }
        }
        StorageLayout::Default => {
            if channel::is_array(shape){
                TABLE_WAVEFORMS
            } else if (kind == ScalarType::string){
                TABLE_STRINGS
            } else {
                TABLE_SCALARS
            }
        }
    }
}

#[derive( Debug, Clone)]
pub struct ChannelMetadata {
    from: u64,
    kind: ScalarType,
    count: usize,
}

impl From<crate::db::ChannelMetadata> for ChannelMetadata {
    fn from(metadata: crate::db::ChannelMetadata) -> Self {
        Self{from: metadata.from_pulse_id as u64,kind: dtype_to_kind(metadata.dtype as u8),count: metadata.element_count as usize,
        }
    }
}

pub struct Ingestor {
    arguments: Arc<Arguments>,
    db: Arc<DB>,
    insert_statements: RwLock<HashMap<String, PreparedStatement>>,
    channel_metadata: RwLock<HashMap<String, ChannelMetadata>>,
    metadata_errors: Arc<AtomicU64>,
}


impl Ingestor {
    pub fn new(arguments:Arc<Arguments>, db:Arc<DB>) -> Self {
        Self { arguments, db, insert_statements: RwLock::new(HashMap::new()), channel_metadata:RwLock::new(HashMap::new()), metadata_errors: Arc::new(AtomicU64::new(0))}
    }


    fn bucket(&self, id:i64, kind:ScalarType, shape:&Option<Vec<u32>>) -> i64 {
        let divider = if channel::is_array(shape){
            BUCKET_DIVISOR_WAVEFORMS
        } else if kind == ScalarType::string {
            BUCKET_DIVISOR_STRINGS
        } else {
            BUCKET_DIVISOR_SCALARS
        };
        id/divider
    }


    pub async fn init(&self) -> IOResult<()> {
        log::info!("Initializing ingestor...");
        self.db.session().expect("Database session not initialized");
        if self.arguments.storage_layout == StorageLayout::Typed {
            self.db.create_data_table(TABLE_WAVEFORMS, "blob", true).await?;
            self.insert_statements.write().await.insert(TABLE_WAVEFORMS.to_string(), self.db.create_data_insert_statement(TABLE_WAVEFORMS, true).await?);
            for cql_type in cql::CQL_TYPES {
                let table_name = cql_type;
                self.db.create_data_table(table_name, cql_type, false).await?;
                self.insert_statements.write().await.insert(table_name.to_string(),self.db.create_data_insert_statement(&table_name, false).await?);
            }
        }  else if self.arguments.storage_layout == StorageLayout::Default {
            self.db.create_data_table(TABLE_WAVEFORMS, "blob", true).await?;
            self.db.create_data_table(TABLE_STRINGS, "text", false).await?;
            self.db.create_data_table(TABLE_SCALARS, "bigint", true).await?;
            self.insert_statements.write().await.insert(TABLE_WAVEFORMS.to_string(), self.db.create_data_insert_statement(TABLE_WAVEFORMS, true).await?);
            self.insert_statements.write().await.insert(TABLE_STRINGS.to_string(), self.db.create_data_insert_statement(TABLE_STRINGS, false).await?);
            self.insert_statements.write().await.insert(TABLE_SCALARS.to_string(), self.db.create_data_insert_statement(TABLE_SCALARS, true).await?);
        }
        self.db.create_channel_metadata_table(TABLE_CHANNEL_METADATA).await?;
        self.insert_statements.write().await.insert(TABLE_CHANNEL_METADATA.to_string(), self.db.create_channel_metadata_insert_statement(TABLE_CHANNEL_METADATA).await?);
        self.channel_metadata.write().await.clear();

        if self.arguments.metadata_on_changed {
            //TODO: can I retrieve the latest metadata for each channel with a single CQL call?
            let channels = self.db.fetch_channels(TABLE_CHANNEL_METADATA).await;
            match channels {
                Ok(channels) => {
                    let db = &self.db;
                    let queries = channels.into_iter().map(|channel| async move {
                        let metadata = db.fetch_channel_metadata(TABLE_CHANNEL_METADATA, &channel).await;
                        (channel, metadata)
                    });
                    let results = join_all(queries).await;

                    {
                        let mut channel_metadata = self.channel_metadata.write().await;
                        for (channel, metadata) in results {
                            match metadata {
                                Ok(metadata) => {
                                    channel_metadata.insert(channel, ChannelMetadata::from(metadata));
                                }
                                Err(e) => {
                                    log::error!("Error fetching channel {} metadata: {}",channel,e);
                                }
                            }
                        }
                    }

                    let channel_metadata = self.channel_metadata.read().await;
                    for (channel, metadata) in channel_metadata.iter() {
                        log::info!("Channel {} metadata initialized: kind={:?} count={}", &channel, metadata.kind,  metadata.count);
                    }
                }
                Err(e) => {
                    log::error!("Error fetching channel names: {}", e);
                }
            }
        }
        Ok(())
    }

    pub async fn on_timer(&self) -> IOResult<()> {
        Ok(())
    }

    pub async fn on_day_change(&self) -> IOResult<()> {
        if !self.arguments.metadata_on_changed {
            log::info!("On-day change event: updating the channel metadata entries");
            //Make sure metadata table is updated every day
            self.channel_metadata.write().await.clear();
        }
        Ok(())
    }


    pub async fn on_header_change(&self, name:&str, kind:ScalarType, shape:&Option<Vec<u32>>, elements:usize, size:usize, id: u64, tm: (u64, u64)) -> IOResult<()> {
        if self.db.enabled_session().is_some() {
            let metadata = self.channel_metadata.read().await.get(name).cloned(); //Lock released
            let insert = match &metadata {
                None => { true }
                Some(metadata) => {
                    metadata.count != elements || metadata.kind != kind
                }
            };
            if insert {
                let reason = if metadata.is_some() {"changed"} else {"added"};
                log::info!("Channel {} metadata {}: kind={:?} count={}", name, reason, kind, elements);
                self.channel_metadata.write().await.insert(name.to_string(), ChannelMetadata { from: id, kind: kind, count: elements });
                let insert_statement = self.insert_statements.read().await.get(TABLE_CHANNEL_METADATA).cloned();
                if let Some(statement) = insert_statement {
                    if self.arguments.metadata_synchronous {
                        Self::append_metadata(&self.db, &self.metadata_errors, &statement, name, kind, elements, id).await;
                    } else {
                        let metadata_errors = self.metadata_errors.clone();
                        let db = self.db.clone();
                        let name = name.to_string();
                        tokio::spawn(async move {
                            Self::append_metadata(&db, &metadata_errors, &statement, &name, kind, elements, id).await;
                        });
                    }
                } else {
                    return Err(IOError::other("Insert statement for channel metadata table not found"));
                }
            }
        }
        Ok(())
    }

    async fn append_metadata(db:&DB, metadata_errors:&Arc<AtomicU64>, statement: &PreparedStatement, name:&str, kind:ScalarType,  elements:usize, id: u64) {
        let result = db.execute_statement_channel_metadata(&statement, name, kind, elements, id as i64).await;
        Self::check_append_metadata_result(id, name, result, metadata_errors);
    }


    fn check_append_metadata_result(id: u64, name: &str, result: IOResult<()>, metadata_errors:&Arc<AtomicU64>) {
        match result {
            Err(e) => {
                let previous = metadata_errors.fetch_add(1, Ordering::Relaxed);
                if previous == 0 {
                    log::error!("Error appending metadata for channel {} on id {}: {}",name, id, e);
                }
            }
            Ok(()) => {
                let failures = metadata_errors.swap(0, Ordering::Relaxed);
                if failures > 0 {
                    log::info!("Metadata recovered on id {}: suppressed {} errors",id,failures - 1);
                }
            }
        }
    }

    pub async fn append_record(&self, name:&str,  kind:ScalarType, shape:&Option<Vec<u32>>, elements:usize, id: u64, tm: (u64, u64), data:Option<Vec<u8>>) -> IOResult<()> {
        if self.db.enabled_session().is_some() {
            let table_name = get_table_name(&self.arguments.storage_layout, name, kind, &shape);
            let id = id as i64;
            if id <= 0 {
                return Err(IOError::other(format!("Invalid id for {}: {}", name, id)));
            }
            
            //let timestamp_sec = tm.0 as i64;
            //let timestamp_nsec = tm.1 as i64;
            let insert_statement = self.insert_statements.read().await.get(table_name).cloned(); //Lock released

            let bucket = self.bucket(id, kind, shape);
            let is_array = channel::is_array(shape);
            let is_scalar_i64 = !is_array && self.arguments.storage_layout == StorageLayout::Default && kind != ScalarType::string;

            match insert_statement{
                None => {
                    log::warn!("Insert statement with name {} not found", name);
                    if is_scalar_i64 {
                        self.db.insert_asi64(&table_name, name, kind, bucket, id, data).await?;
                    } else if is_array {
                        self.db.insert_blob(&table_name, name, kind, bucket, id, data).await?;
                    } else {
                        self.db.insert_typed(&table_name, name, kind, bucket, id, data).await?;
                    }
                }
                Some(statement) => {
                    if is_scalar_i64 {
                        self.db.execute_statement_i64(&statement, name, kind, bucket, id, data).await?;
                    } else if is_array {
                        self.db.execute_statement_blob(&statement, name, kind, bucket, id, data).await?;
                    } else {
                        self.db.execute_statement_typed(&statement, name, kind, bucket, id, data).await?;
                    }
                }
            }
        }
        Ok(())
    }
}
