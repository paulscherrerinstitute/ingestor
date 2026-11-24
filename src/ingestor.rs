use std::collections::HashMap;
use crate::Arguments;
use crate::DB;
use crate::cql;
use crate::codec::*;
use std::sync::{Arc, RwLock as StdRwLock, PoisonError};
use std::sync::atomic::Ordering;
use bsread::{channel, IOError, IOResult, ScalarType};
use scylla::statement::prepared::PreparedStatement;
use tokio::sync::{RwLock};
use futures::future::join_all;
use std::io::ErrorKind;
use crate::app::App;
use crate::arguments::StorageLayout;

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
    pub name: Option<String>,
    pub from: u64,
    pub kind: ScalarType,
    pub count: usize,
}

impl From<crate::db::ChannelMetadata> for ChannelMetadata {
    fn from(metadata: crate::db::ChannelMetadata) -> Self {
        Self{name:None, from: metadata.from_pulse_id as u64,kind: dtype_to_kind(metadata.dtype as u8),count: metadata.element_count as usize}
    }
}

/// Prepared insert statements keyed by table name. Immutable once published.
type Statements = HashMap<&'static str, PreparedStatement>;

pub struct Ingestor {
    arguments: Arc<Arguments>,
    db: Arc<DB>,
    /// Swapped atomically by `init()`; readers only clone the `Arc` (no await, no statement clone).
    insert_statements: StdRwLock<Arc<Statements>>,
    channel_metadata: RwLock<HashMap<String, ChannelMetadata>>,
}


impl Ingestor {
    pub fn new(arguments:Arc<Arguments>, db:Arc<DB>) -> Self {
        let insert_statements = StdRwLock::new(Arc::new(HashMap::new()));
        let channel_metadata =RwLock::new(HashMap::new());
        Self { arguments, db, insert_statements, channel_metadata}
    }

    fn bucket(id: i64, kind: ScalarType, is_array: bool) -> i64 {
        let divider = if is_array {
            BUCKET_DIVISOR_WAVEFORMS
        } else if kind == ScalarType::string {
            BUCKET_DIVISOR_STRINGS
        } else {
            BUCKET_DIVISOR_SCALARS
        };
        id / divider
    }

    /// Snapshot of the prepared statements. The lock is held only for the `Arc` clone.
    fn statements(&self) -> Arc<Statements> {
        Arc::clone(&self.insert_statements.read().unwrap_or_else(PoisonError::into_inner))
    }

    pub async fn init(&self) -> IOResult<()> {
        log::info!("Initializing ingestor...");
        self.db.session().expect("Database session not initialized");
        // Build locally and publish at the end, so a failed init never leaves a partial map.
        let mut statements: Statements = HashMap::new();
        if self.arguments.storage_layout == StorageLayout::Typed {
            self.db.create_data_table(TABLE_WAVEFORMS, "blob", true).await?;
            statements.insert(TABLE_WAVEFORMS, self.db.create_data_insert_statement(TABLE_WAVEFORMS, true).await?);
            for cql_type in cql::CQL_TYPES {
                let table_name: &'static str = cql_type;
                self.db.create_data_table(table_name, table_name, false).await?;
                statements.insert(table_name, self.db.create_data_insert_statement(table_name, false).await?);
            }
        }  else if self.arguments.storage_layout == StorageLayout::Default {
            self.db.create_data_table(TABLE_WAVEFORMS, "blob", true).await?;
            self.db.create_data_table(TABLE_STRINGS, "text", false).await?;
            self.db.create_data_table(TABLE_SCALARS, "bigint", true).await?;
            statements.insert(TABLE_WAVEFORMS, self.db.create_data_insert_statement(TABLE_WAVEFORMS, true).await?);
            statements.insert(TABLE_STRINGS, self.db.create_data_insert_statement(TABLE_STRINGS, false).await?);
            statements.insert(TABLE_SCALARS, self.db.create_data_insert_statement(TABLE_SCALARS, true).await?);
        }
        self.db.create_channel_metadata_table(TABLE_CHANNEL_METADATA).await?;
        statements.insert(TABLE_CHANNEL_METADATA, self.db.create_channel_metadata_insert_statement(TABLE_CHANNEL_METADATA).await?);
        *self.insert_statements.write().unwrap_or_else(PoisonError::into_inner) = Arc::new(statements);
        self.channel_metadata.write().await.clear();

        if self.arguments.metadata_on_changed {

            //TODO: this is inneficient, but metadata_on_changed is not currently not planned
            // to be used in production. If it is, we should consider a more efficient approach.
            let channels = self.db.fetch_channels(TABLE_CHANNEL_METADATA).await;
            match channels {
                Ok(channels) => {
                    let db = &self.db;
                    let fetch_metadata_statement = db.create_channel_metadata_fetch_statement(TABLE_CHANNEL_METADATA).await?;
                    let queries = channels.into_iter().map(|channel| {
                        let statement = &fetch_metadata_statement;
                        async move {
                            let metadata = db.execute_statement_fetch_channel_metadata(&statement, &channel).await;
                            (channel, metadata)
                        }
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

    pub async fn clear_metadata(&self){
         self.channel_metadata.write().await.clear();
    }


    pub fn enabled(&self) -> bool {
        self.db.enabled_session().is_some()
    }

    fn to_db_id(name: &str, id: u64) -> IOResult<i64> {
        i64::try_from(id)
            .ok()
            .filter(|id| *id > 0)
            .ok_or_else(|| IOError::new(ErrorKind::InvalidInput, format!("Invalid id for {}: {}", name, id)))
    }

    fn missing_statement(table_name: &str) -> IOError {
        IOError::new(ErrorKind::NotFound,format!("Insert statement for table {} not prepared (ingestor not initialized?)", table_name))
    }




    pub async fn append_metadata(&self, name:&str, kind:ScalarType, elements:usize, id: u64) -> IOResult<()> {
        if self.enabled() {
            let previous = self.channel_metadata.read().await.get(name).cloned(); //Lock released
            if previous.as_ref().is_some_and(|m| m.count == elements && m.kind == kind) {
                return Ok(());
            }
            let from = Self::to_db_id(name, id)?;
            let statements = self.statements();
            let statement = statements.get(TABLE_CHANNEL_METADATA)
                .ok_or_else(|| Self::missing_statement(TABLE_CHANNEL_METADATA))?;

            let reason = if previous.is_some() { "changed" } else { "added" };
            log::info!("Channel {} metadata {}: kind={:?} count={} from={}", name, reason, kind, elements, id);


            self.db.execute_statement_channel_metadata(statement, name, kind, elements, from).await?;
            // Cache only after a successful write, so failed inserts are really retried
            self.channel_metadata.write().await
                .insert(name.to_string(), ChannelMetadata { name: None, from: id, kind, count: elements });
            //This implementation is not ideal for sync metadada_processing or async channel_processing
            //because we may have multiple fb calls before writing the buffer.
            //Async modes are not expected to be used in production.
            // If so the implementation should change buffer first and revert if there is an error in DB.

        }
        Ok(())
    }

    pub async fn append_record(&self, name:&str,  kind:ScalarType, shape:&Option<Vec<u32>>, _elements:usize, id: u64, _tm: (u64, u64), data:Option<Vec<u8>>) -> IOResult<()> {
        if self.enabled() {
            let id = Self::to_db_id(name, id)?;
            let layout = &self.arguments.storage_layout;
            let table_name = get_table_name(layout, name, kind, shape);
            let statements = self.statements();
            let statement = statements.get(table_name).ok_or_else(|| Self::missing_statement(table_name))?;
            let is_array = channel::is_array(shape);
            let bucket = Self::bucket(id, kind, is_array);

            if is_array {
                self.db.execute_statement_blob(statement, name, kind, bucket, id, data).await?;
            } else if *layout == StorageLayout::Default && kind != ScalarType::string {
                self.db.execute_statement_i64(statement, name, kind, bucket, id, data).await?;
            } else {
                self.db.execute_statement_typed(statement, name, kind, bucket, id, data).await?;
            }
        }
        Ok(())
    }
}