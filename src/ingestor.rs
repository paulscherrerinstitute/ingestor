use std::collections::HashMap;
use std::io::ErrorKind;
use crate::Arguments;
use crate::DB;
use crate::cql;
use crate::codec::*;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use bsread::{channel, IOError, IOResult, ScalarType, SocketType};
use log::error;
use scylla::statement::prepared::PreparedStatement;
use tokio::sync::RwLock;
use futures::future::join_all;
use crate::arguments::StorageLayout;
use crate::config::SocketKind;

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
}


impl Ingestor {
    pub fn new(arguments:Arc<Arguments>, db:Arc<DB>) -> Self {
        Self { arguments, db, insert_statements: RwLock::new(HashMap::new()), channel_metadata:RwLock::new(HashMap::new())}
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
        let session =  self.db.session().expect("Database session not initialized");
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
                for (channel,metadata) in  channel_metadata.iter() {
                    log::info!("Channel {} metadata initialized: kind={:?} count={}", &channel, metadata.kind,  metadata.count);
                }
            }
            Err(e) => {
                log::error!("Error fetching channel names: {}", e);
            }
        }
        Ok(())
    }

    pub async fn on_header_change(&self, name:String, kind:ScalarType, shape:Option<Vec<u32>>, elements:usize, size:usize, id: u64, tm: (u64, u64)) -> IOResult<()>{
        let metadata = self.channel_metadata.read().await.get(&name).cloned(); //Lock released
        let changed = match metadata {
            None => {true}
            Some(metadata) => {
                metadata.count != elements || metadata.kind != kind
            }
        };
        if changed {
            log::info!("Channel {} metadata changed: kind={:?} count={}", &name, kind,  elements);
            self.channel_metadata.write().await.insert(name.clone(), ChannelMetadata{from: id, kind: kind, count: elements});
            if self.db.enabled_session().is_some() {
                let insert_statement = self.insert_statements.read().await.get(TABLE_CHANNEL_METADATA).cloned();
                if let Some(statement) = insert_statement {
                    if let Err(e) = self.execute_statement_channel_metadata(&statement, &name, kind, elements, id as i64).await {
                        log::error!("Error adding metadata for channel {}: {}", &name, e);
                    }
                } else {
                    log::error!("Insert statement for channel metadata table not found");
                }
            }
        }
        Ok(())
    }

    pub async fn append_record(&self, name:String,  kind:ScalarType, shape:Option<Vec<u32>>, elements:usize, id: u64, tm: (u64, u64), data:Option<Vec<u8>>) -> IOResult<()> {
        if self.db.enabled_session().is_some() {
            let table_name = get_table_name(&self.arguments.storage_layout, &name, kind, &shape);
            let id = id as i64;
            if id <= 0 {
                return Err(IOError::other( format!("Invalid id for {}: {}", name, id)));
            }
            
            //let timestamp_sec = tm.0 as i64;
            //let timestamp_nsec = tm.1 as i64;
            let insert_statement = self.insert_statements.read().await.get(table_name).cloned(); //Lock released

            let bucket = self.bucket(id, kind, &shape);
            let is_array = channel::is_array(&shape);
            let is_scalar_i64 = !is_array && self.arguments.storage_layout == StorageLayout::Default && kind != ScalarType::string;

            match insert_statement{
                None => {
                    log::warn!("Insert statement with name {} not found", &name);
                    if is_scalar_i64 {
                        self.insert_asi64(&table_name, &name, kind, bucket, id, data).await?;
                    } else if is_array {
                        self.insert_blob(&table_name, &name, kind, bucket, id, data).await?;
                    } else {
                        self.insert_typed(&table_name, &name, kind, bucket, id, data).await?;
                    }
                }
                Some(statement) => {
                    if is_scalar_i64 {
                        self.execute_statement_i64(&statement, &name, kind, bucket, id, data).await?;
                    } else if is_array {
                        self.execute_statement_blob(&statement, &name, kind, bucket, id, data).await?;
                    } else {
                        self.execute_statement_typed(&statement, &name, kind, bucket, id, data).await?;
                    }
                }
            }
        }
        Ok(())
    }



    async fn insert_blob(&self, table_name: &str, name: &str, kind: ScalarType, bucket: i64, id: i64, data:Option<Vec<u8>>) -> IOResult<()> {
        let dtype = kind_to_dtype(kind);
        self.db.insert_with_type(table_name, name,(name, bucket, id, data, dtype), ).await
    }

    async fn insert_asi64(&self, table_name: &str, name: &str, kind: ScalarType, bucket: i64, id: i64, data:Option<Vec<u8>>) -> IOResult<()> {
        let data = encode_to_i64(kind, data)?;
        let dtype = kind_to_dtype(kind);
        self.db.insert_with_type(table_name, name,(name, bucket, id, data, dtype), ).await
    }

    async fn insert_typed(&self, table_name: &str, name: &str, kind: ScalarType, bucket: i64, id: i64, data: Option<Vec<u8>>,) -> IOResult<()> {
        match kind {
            ScalarType::string => {
                let value = data
                    .map(String::from_utf8)
                    .transpose()
                    .map_err(|e| IOError::new(ErrorKind::InvalidData, e))?;
                self.db.insert(table_name, name,(name, bucket, id, value)).await
            }
            ScalarType::bool => {
                let value = decode_optional(data,|b: [u8; 1]| b[0] != 0,)?;
                self.db.insert(table_name, name, (name, bucket, id, value)).await
            }
            ScalarType::int8 => {
                let value = decode_optional(data, i8::from_le_bytes)?;
                self.db.insert(table_name, name, (name, bucket, id, value)).await
            }
            ScalarType::uint8 => {
                let value = decode_optional(data, u8::from_le_bytes)?.map(i16::from);
                self.db.insert(table_name, name, (name, bucket, id, value)).await
            }
            ScalarType::int16 => {
                let value = decode_optional(data, i16::from_le_bytes)?;
                self.db.insert(table_name, name, (name, bucket, id, value)).await
            }
            ScalarType::uint16 => {
                let value = decode_optional(data, u16::from_le_bytes)?.map(i32::from);
                self.db.insert(table_name, name, (name, bucket, id, value)).await
            }
            ScalarType::int32 => {
                let value = decode_optional(data, i32::from_le_bytes)?;
                self.db.insert(table_name, name, (name, bucket, id, value)).await
            }
            ScalarType::uint32 => {
                let value = decode_optional(data, u32::from_le_bytes)?.map(i64::from);
                self.db.insert(table_name, name, (name, bucket, id, value)).await
            }
            ScalarType::int64 => {
                let value = decode_optional(data, i64::from_le_bytes)?;
                self.db.insert(table_name, name, (name, bucket, id, value)).await
            }
            ScalarType::uint64 => {
                // CQL has no uint64, so preserve the original bytes as a blob.
                self.db.insert(table_name, name, (name, bucket, id, data)).await
            }
            ScalarType::float32 => {
                let value = decode_optional(data, f32::from_le_bytes)?;
                self.db.insert(table_name, name, (name, bucket, id, value)).await
            }
            ScalarType::float64 => {
                let value = decode_optional(data, f64::from_le_bytes)?;
                self.db.insert(table_name, name, (name, bucket, id, value)).await
            }
        }
    }

    async fn execute_statement_blob(&self, statement: &PreparedStatement, name: &str, kind: ScalarType, bucket: i64, id: i64, data: Option<Vec<u8>>) -> IOResult<()> {
        let dtype = kind_to_dtype(kind);
        self.db.execute_statement(statement, name, (name, bucket, id, data, dtype)).await
    }

    async fn execute_statement_i64(&self, statement: &PreparedStatement, name: &str, kind: ScalarType,bucket: i64,  id: i64, data: Option<Vec<u8>>) -> IOResult<()> {
        let data = encode_to_i64(kind, data)?;
        let dtype = kind_to_dtype(kind);
        self.db.execute_statement(statement, name, (name, bucket, id, data, dtype)).await
    }

    async fn execute_statement_typed(&self,statement: &PreparedStatement,name: &str,kind: ScalarType, bucket: i64, id: i64, data: Option<Vec<u8>>,) -> IOResult<()> {
        match kind {
            ScalarType::string => {
                let value = data
                    .map(String::from_utf8)
                    .transpose()
                    .map_err(|e| IOError::new(ErrorKind::InvalidData, e))?;
                self.db.execute_statement(statement, name, (name, bucket, id, value)).await
            }
            ScalarType::bool => {
                let value = decode_optional(data, |b: [u8; 1]| b[0] != 0)?;
                self.db.execute_statement(statement, name, (name, bucket, id, value)).await
            }
            ScalarType::int8 => {
                let value = decode_optional(data, i8::from_le_bytes)?;
                self.db.execute_statement(statement, name, (name, bucket, id, value)).await
            }
            ScalarType::uint8 => {
                let value = decode_optional(data, u8::from_le_bytes)?.map(i16::from);
                self.db.execute_statement(statement, name, (name, bucket, id, value)).await
            }
            ScalarType::int16 => {
                let value = decode_optional(data, i16::from_le_bytes)?;
                self.db.execute_statement(statement, name, (name, bucket, id, value)).await
            }
            ScalarType::uint16 => {
                let value = decode_optional(data, u16::from_le_bytes)?.map(i32::from);
                self.db.execute_statement(statement, name, (name, bucket, id, value)).await
            }
            ScalarType::int32 => {
                let value = decode_optional(data, i32::from_le_bytes)?;
                self.db.execute_statement(statement, name, (name, bucket, id, value)).await
            }
            ScalarType::uint32 => {
                let value = decode_optional(data, u32::from_le_bytes)?.map(i64::from);
                self.db.execute_statement(statement, name, (name, bucket, id, value)).await
            }
            ScalarType::int64 => {
                let value = decode_optional(data, i64::from_le_bytes)?;
                self.db.execute_statement(statement, name, (name, bucket, id, value)).await
            }
            ScalarType::uint64 => {
                // uint64 is represented as CQL blob.
                self.db.execute_statement(statement, name, (name, bucket, id, data)).await
            }
            ScalarType::float32 => {
                let value = decode_optional(data, f32::from_le_bytes)?;
                self.db.execute_statement(statement, name, (name, bucket, id, value)).await
            }
            ScalarType::float64 => {
                let value = decode_optional(data, f64::from_le_bytes)?;
                self.db.execute_statement(statement, name, (name, bucket, id, value)).await
            }
        }
    }
    async fn execute_statement_channel_metadata(&self, statement: &PreparedStatement, name: &str, kind: ScalarType, count:usize, from: i64) -> IOResult<()> {
        let dtype = kind_to_dtype(kind);
        self.db.execute_statement(statement, name, (name, from, dtype, count as i32)).await
    }
}
