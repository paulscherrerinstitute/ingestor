use std::collections::HashMap;
use std::io::ErrorKind;
use crate::Arguments;
use crate::DB;
use crate::cql;
use std::sync::Arc;
use bsread::{channel, IOError, IOResult, ScalarType};
use scylla::_macro_internal::SerializeRow;
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;
use scylla::errors::ExecutionError;
use tokio::sync::RwLock;
use crate::arguments::StorageLayout;

pub struct Ingestor {
    arguments: Arc<Arguments>,
    db: Arc<DB>,
    insert_statements: RwLock<HashMap<String, PreparedStatement>>,
}

macro_rules! insert_record {
    ($self:expr, $table_name:expr, $name:expr, $id:expr, $timestamp_sec:expr, $timestamp_nsec:expr, $value:expr) => {
        if $self.arguments.storage_layout.is_shared() {
            $self.db.insert($table_name,$name, ($name, $id, $timestamp_sec, $timestamp_nsec, $value),).await
        } else {
            $self.db.insert($table_name,$name, ($id, $timestamp_sec, $timestamp_nsec, $value),).await
        }
    };
}

macro_rules! exec_statement {
    ($self:expr,$statement:expr, $name:expr, $id:expr, $timestamp_sec:expr, $timestamp_nsec:expr, $value:expr) => {
        if $self.arguments.storage_layout.is_shared() {
            $self.db.execute_statement($statement, $name,($name, $id, $timestamp_sec, $timestamp_nsec, $value),).await
        } else {
            $self.db.execute_statement($statement, $name,($id, $timestamp_sec, $timestamp_nsec, $value),).await
        }
    };
}



impl Ingestor {
    pub fn new(arguments:Arc<Arguments>, db:Arc<DB>) -> Self {
        Self { arguments, db, insert_statements: RwLock::new(HashMap::new()) }
    }

    fn is_blob_table(&self, kind:ScalarType, shape:&Option<Vec<u32>>) -> bool {
        channel::is_array(shape) || self.arguments.storage_layout.is_blob()
    }


    pub async fn init(&self) -> IOResult<()> {
        let session =  self.db.session().expect("Database session not initialized");
        if self.arguments.storage_layout.is_shared() {
            if self.arguments.storage_layout == StorageLayout::Type {
                for cql_type in cql::CQL_TYPES {
                    let table_name = DB::get_shared_table_name(Some(cql_type));
                    let query = cql::typed_table_creation(&table_name, cql_type);
                    session.query_unpaged(query, &[]).await.
                        map_err(|e| { IOError::other( format!("Error creating table {}: {}", &table_name, e))})?;

                    let prepared_statement = self.db.create_insert_statement(&table_name).await?;
                    self.insert_statements.write().await.insert(table_name, prepared_statement);
                }
            }  else if self.arguments.storage_layout == StorageLayout::Shared {
                let table_name = DB::get_shared_table_name(None);
                let query = cql::blob_table_creation(&table_name);
                session.query_unpaged(query, &[]).await.
                    map_err(|e| { IOError::other( format!("Error creating table {}: {}", &table_name, e))})?;

                let prepared_statement = self.db.create_insert_statement(&table_name).await?;
                self.insert_statements.write().await.insert(table_name, prepared_statement);
            }
        }
        Ok(())
    }

    pub async fn create_table(&self, name:String, kind:ScalarType, shape:Option<Vec<u32>>, size:usize) -> IOResult<()>{
        if !self.arguments.storage_layout.is_shared() {
            let table_name = self.get_table_name(&name, kind, &shape);
            if self.is_blob_table(kind, &shape) {
                self.db.create_table(&table_name, None).await?;
            } else {
                self.db.create_table(&table_name, Some(kind)).await?;
            }
            let prepared_statement = self.db.create_insert_statement(&table_name).await?;
            self.insert_statements.write().await.insert(table_name, prepared_statement);
        }
        Ok(())
    }

    fn get_table_name(&self, channel_name:&str, kind:ScalarType, shape:&Option<Vec<u32>>) -> String {
        match self.arguments.storage_layout {
            StorageLayout::Channel => {DB::get_individual_table_name(channel_name)}
            StorageLayout::Blob => {DB::get_individual_table_name(channel_name)}
            StorageLayout::Type => {DB::get_shared_table_name(Some(if channel::is_array(shape){"blob"} else {cql::kind_to_cql_type(kind)}))}
            StorageLayout::Shared => {DB::get_shared_table_name(None)}
        }
    }

    pub async fn append_record(&self, name:String,  kind:ScalarType, shape:Option<Vec<u32>>, id: u64, tm: (u64, u64), data:Option<Vec<u8>>) -> IOResult<()> {
        if self.db.enabled_session().is_some() {
            let table_name = self.get_table_name(&name, kind, &shape);
            let id = id as i64;
            if id <= 0 {
                return Err(IOError::other( format!("Invalid id for {}: {}", name, id)));
            }
            
            let timestamp_sec = tm.0 as i64;
            let timestamp_nsec = tm.1 as i64;
            let insert_statement = self.insert_statements.read().await.get(&table_name).cloned();
            //Lock released

            match insert_statement{
                None => {
                    log::warn!("Insert statement with name {} not found", &name);
                    if self.is_blob_table(kind, &shape) {
                        self.insert_blob(&table_name, &name, id, timestamp_sec, timestamp_nsec, data).await?;
                    } else {
                        self.insert_typed(&table_name, &name, kind, id, timestamp_sec, timestamp_nsec, data).await?;
                    }
                }
                Some(statement) => {
                    if self.is_blob_table(kind, &shape) {
                        self.execute_statement_blob(&statement, &name, id, timestamp_sec, timestamp_nsec, data).await?;
                    } else {
                        self.execute_statement_typed(&statement, &name, kind, id, timestamp_sec, timestamp_nsec, data).await?;
                    }
                }
            }
        }
        Ok(())
    }



    async fn insert_blob(&self, table_name: &str, name: &str, id: i64,timestamp_sec: i64, timestamp_nsec:i64, data:Option<Vec<u8>>) -> IOResult<()> {
        if self.arguments.storage_layout.is_shared() {
            self.db.insert(table_name, name,(name, id, timestamp_sec, timestamp_nsec, data), ).await
        } else {
            self.db.insert(table_name, name,(id, timestamp_sec, timestamp_nsec, data), ).await
        }
    }

    async fn insert_typed(&self, table_name: &str, name: &str, kind: ScalarType, id: i64,  timestamp_sec: i64, timestamp_nsec: i64,data: Option<Vec<u8>>,) -> IOResult<()> {
        match kind {
            ScalarType::string => {
                let value = data
                    .map(String::from_utf8)
                    .transpose()
                    .map_err(|e| IOError::new(ErrorKind::InvalidData, e))?;
                insert_record!(self, table_name, name, id, timestamp_sec, timestamp_nsec, value)
            }

            ScalarType::bool => {
                let value = data
                    .map(|v| {
                        let bytes: [u8; 1] = v.try_into()
                            .map_err(|_| IOError::new(
                                ErrorKind::InvalidData,
                                "Invalid bool data",
                            ))?;

                        Ok::<bool, IOError>(bytes[0] != 0)
                    })
                    .transpose()?;
                insert_record!(self, table_name, name, id, timestamp_sec, timestamp_nsec, value)
            }

            ScalarType::int8 => {
                let value = data
                    .map(|v| decode(v, i8::from_le_bytes))
                    .transpose()?;
                insert_record!(self, table_name, name, id, timestamp_sec, timestamp_nsec, value)
            }

            ScalarType::uint8 => {
                let value = data
                    .map(|v| decode(v, u8::from_le_bytes))
                    .transpose()?
                    .map(i16::from);
                insert_record!(self, table_name, name, id, timestamp_sec, timestamp_nsec, value)
            }

            ScalarType::int16 => {
                let value = data
                    .map(|v| decode(v, i16::from_le_bytes))
                    .transpose()?;
                insert_record!(self,table_name,  name, id, timestamp_sec, timestamp_nsec, value)
            }

            ScalarType::uint16 => {
                let value = data
                    .map(|v| decode(v, u16::from_le_bytes))
                    .transpose()?
                    .map(i32::from);
                insert_record!(self, table_name, name, id, timestamp_sec, timestamp_nsec, value)
            }

            ScalarType::int32 => {
                let value = data
                    .map(|v| decode(v, i32::from_le_bytes))
                    .transpose()?;
                insert_record!(self, table_name, name, id, timestamp_sec, timestamp_nsec, value)
            }

            ScalarType::uint32 => {
                let value = data
                    .map(|v| decode(v, u32::from_le_bytes))
                    .transpose()?
                    .map(i64::from);
                insert_record!(self, table_name, name, id, timestamp_sec, timestamp_nsec, value)
            }

            ScalarType::int64 => {
                let value = data
                    .map(|v| decode(v, i64::from_le_bytes))
                    .transpose()?;
                insert_record!(self, table_name, name, id, timestamp_sec, timestamp_nsec, value)
            }

            ScalarType::uint64 => {
                // CQL has no uint64, so preserve the original bytes as a blob.
                insert_record!(self, table_name, name, id, timestamp_sec, timestamp_nsec, data)
            }

            ScalarType::float32 => {
                let value = data
                    .map(|v| decode(v, f32::from_le_bytes))
                    .transpose()?;
                insert_record!(self, table_name, name, id, timestamp_sec, timestamp_nsec, value)
            }

            ScalarType::float64 => {
                let value = data
                    .map(|v| decode(v, f64::from_le_bytes))
                    .transpose()?;
                insert_record!(self, table_name, name, id, timestamp_sec, timestamp_nsec, value)
            }
        }
    }

    async fn execute_statement_blob(&self, statement: &PreparedStatement, name: &str, id: i64,timestamp_sec: i64,timestamp_nsec: i64,data: Option<Vec<u8>>) -> IOResult<()> {
        if self.arguments.storage_layout.is_shared() {
            self.db.execute_statement(statement, name, (name, id, timestamp_sec, timestamp_nsec, data)).await
        } else {
            self.db.execute_statement(statement, name, (id, timestamp_sec, timestamp_nsec, data)).await
        }
    }

    async fn execute_statement_typed(&self,statement: &PreparedStatement,name: &str,kind: ScalarType,id: i64,timestamp_sec: i64,timestamp_nsec: i64,data: Option<Vec<u8>>,) -> IOResult<()> {
        match kind {
            ScalarType::string => {
                let value = data
                    .map(String::from_utf8)
                    .transpose()
                    .map_err(|e| IOError::new(ErrorKind::InvalidData, e))?;
                exec_statement!(self, statement, name, id, timestamp_sec, timestamp_nsec, value)
            }

            ScalarType::bool => {
                let value = data
                    .map(|v| decode(v, |b: [u8; 1]| b[0] != 0))
                    .transpose()?;
                exec_statement!(self, statement, name, id, timestamp_sec, timestamp_nsec, value)
            }

            ScalarType::int8 => {
                let value = data
                    .map(|v| decode(v, i8::from_le_bytes))
                    .transpose()?;
                exec_statement!(self, statement, name, id, timestamp_sec, timestamp_nsec, value)
            }

            ScalarType::uint8 => {
                let value = data
                    .map(|v| decode(v, u8::from_le_bytes))
                    .transpose()?
                    .map(i16::from);
                exec_statement!(self, statement, name, id, timestamp_sec, timestamp_nsec, value)
            }

            ScalarType::int16 => {
                let value = data
                    .map(|v| decode(v, i16::from_le_bytes))
                    .transpose()?;
                exec_statement!(self, statement, name, id, timestamp_sec, timestamp_nsec, value)
            }

            ScalarType::uint16 => {
                let value = data
                    .map(|v| decode(v, u16::from_le_bytes))
                    .transpose()?
                    .map(i32::from);
                exec_statement!(self, statement, name, id, timestamp_sec, timestamp_nsec, value)
            }

            ScalarType::int32 => {
                let value = data
                    .map(|v| decode(v, i32::from_le_bytes))
                    .transpose()?;
                exec_statement!(self, statement, name, id, timestamp_sec, timestamp_nsec, value)
            }

            ScalarType::uint32 => {
                let value = data
                    .map(|v| decode(v, u32::from_le_bytes))
                    .transpose()?
                    .map(i64::from);
                exec_statement!(self, statement, name, id, timestamp_sec, timestamp_nsec, value)
            }

            ScalarType::int64 => {
                let value = data
                    .map(|v| decode(v, i64::from_le_bytes))
                    .transpose()?;
                exec_statement!(self, statement, name, id, timestamp_sec, timestamp_nsec, value)
            }

            ScalarType::uint64 => {
                // uint64 is represented as CQL blob.
                exec_statement!(self, statement, name, id, timestamp_sec, timestamp_nsec, data)
            }

            ScalarType::float32 => {
                let value = data
                    .map(|v| decode(v, f32::from_le_bytes))
                    .transpose()?;
                exec_statement!(self, statement, name, id, timestamp_sec, timestamp_nsec, value)
            }

            ScalarType::float64 => {
                let value = data
                    .map(|v| decode(v, f64::from_le_bytes))
                    .transpose()?;
                exec_statement!(self, statement, name, id, timestamp_sec, timestamp_nsec, value)
            }
        }
    }
}

fn decode<T, const N: usize>(data: Vec<u8>,f: impl FnOnce([u8; N]) -> T,) -> IOResult<T> {
    let bytes: [u8; N] = data.try_into()
        .map_err(|_| IOError::new(ErrorKind::InvalidData, "Invalid data size"))?;
    Ok(f(bytes))
}

