use std::collections::HashMap;
use std::io::ErrorKind;
use crate::Arguments;
use crate::DB;
use crate::cql;
use std::sync::Arc;
use bsread::{IOError, IOResult, ScalarType};
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
    ($self:expr, $session:expr, $table_name:expr, $name:expr, $id:expr, $timestamp_sec:expr, $timestamp_nsec:expr, $value:expr) => {
        if $self.arguments.storage_layout.is_shared() {
            $self.insert($session,$table_name,$name, ($name, $id, $timestamp_sec, $timestamp_nsec, $value),).await
        } else {
            $self.insert($session,$table_name,$name, ($id, $timestamp_sec, $timestamp_nsec, $value),).await
        }
    };
}

macro_rules! exec_statement {
    ($self:expr, $session:expr, $statement:expr, $name:expr, $id:expr, $timestamp_sec:expr, $timestamp_nsec:expr, $value:expr) => {
        if $self.arguments.storage_layout.is_shared() {
            $self.execute_statement($session,$statement, $name,($name, $id, $timestamp_sec, $timestamp_nsec, $value),).await
        } else {
            $self.execute_statement($session,$statement, $name,($id, $timestamp_sec, $timestamp_nsec, $value),).await
        }
    };
}



impl Ingestor {
    pub fn new(arguments:Arc<Arguments>, db:Arc<DB>) -> Self {
        Self { arguments, db, insert_statements: RwLock::new(HashMap::new()) }
    }

    fn is_blob_table(&self, kind:ScalarType, shape:&Option<Vec<u32>>) -> bool {
        is_array(shape) || self.arguments.storage_layout.is_blob()
    }


    pub async fn init(&self) -> IOResult<()> {
        let session =  self.db.session().expect("Database session not initialized");
        if self.arguments.storage_layout.is_shared() {
            if self.arguments.storage_layout == StorageLayout::Type {
                for cql_type in cql::CQL_TYPES {
                    let table_name = DB::get_shared_table_name(Some(cql_type));
                    let query = cql::typed_table_creation(&table_name, cql_type);
                    session.query_unpaged(query, &[]).await.
                        map_err(|e| { IOError::new(ErrorKind::Other, format!("Error creating table {}: {}", &table_name, e))})?;

                    let prepared_statement = self.create_insert_statement(session, &table_name).await?;
                    self.insert_statements.write().await.insert(table_name, prepared_statement);
                }
            }  else if self.arguments.storage_layout == StorageLayout::Shared {
                let table_name = DB::get_shared_table_name(None);
                let query = cql::blob_table_creation(&table_name);
                session.query_unpaged(query, &[]).await.
                    map_err(|e| { IOError::new(ErrorKind::Other, format!("Error creating table {}: {}", &table_name, e))})?;

                let prepared_statement = self.create_insert_statement(session, &table_name).await?;
                self.insert_statements.write().await.insert(table_name, prepared_statement);
            }
        }
        Ok(())
    }

    pub async fn create_table(&self, name:String, kind:ScalarType, shape:Option<Vec<u32>>, size:usize) -> IOResult<()>{
        if !self.arguments.storage_layout.is_shared() {
            if let Some(session) = self.db.session() {
                let table_name = self.get_table_name(&name, kind, &shape);
                if self.is_blob_table(kind, &shape) {
                    self.create_blob_table(session, &name).await?;
                } else {
                    self.create_typed_table(session, &name, kind).await?;
                }
                let prepared_statement = self.create_insert_statement(session, &table_name).await?;
                self.insert_statements.write().await.insert(name, prepared_statement);
            }
        }
        Ok(())
    }

    fn get_table_name(&self, channel_name:&str, kind:ScalarType, shape:&Option<Vec<u32>>) -> String {
        match self.arguments.storage_layout {
            StorageLayout::Channel => {DB::get_individual_table_name(channel_name)}
            StorageLayout::Blob => {DB::get_individual_table_name(channel_name)}
            StorageLayout::Type => {DB::get_shared_table_name(Some(if is_array(shape){"blob"} else {cql::kind_to_cql_type(kind)}))}
            StorageLayout::Shared => {DB::get_shared_table_name(None)}
        }
    }

    pub async fn append_record(&self, name:String,  kind:ScalarType, shape:Option<Vec<u32>>, id: u64, tm: (u64, u64), data:Option<Vec<u8>>) -> IOResult<()> {
        if let Some(session) = self.db.enabled_session() {
            let table_name = self.get_table_name(&name, kind, &shape);
            let id = i64::try_from(id).map_err(|e| {IOError::new(ErrorKind::Other,format!("Error converting id {}: {}", id, e))})?;
            let timestamp_sec = i64::try_from(tm.0).map_err(|e| {IOError::new(ErrorKind::Other,format!("Error converting tm {:?}: {}", tm, e))})?;
            let timestamp_nsec = i64::try_from(tm.1).map_err(|e| {IOError::new(ErrorKind::Other,format!("Error converting tm {:?}: {}", tm, e))})?;
            let insert_statement = self.insert_statements.read().await.get(&table_name).cloned();
            //Lock released

            match insert_statement{
                None => {
                    log::warn!("Insert statement with name {} not found", &name);
                    if self.is_blob_table(kind, &shape) {
                        self.insert_blob(session, &table_name, &name, id, timestamp_sec, timestamp_nsec, data).await?;
                    } else {
                        self.insert_typed(session, &table_name, &name, kind, id, timestamp_sec, timestamp_nsec, data).await?;
                    }
                }
                Some(statement) => {
                    if self.is_blob_table(kind, &shape) {
                        self.execute_statement_blob(session, &statement, &name, id, timestamp_sec, timestamp_nsec, data).await?;
                    } else {
                        self.execute_statement_typed(session, &statement, &name, kind, id, timestamp_sec, timestamp_nsec, data).await?;
                    }
                }
            }
        }
        Ok(())
    }


    async fn create_insert_statement(&self, session:&Session, table_name: &str) -> IOResult<PreparedStatement> {
        let query = if self.arguments.storage_layout.is_shared(){
            cql::insert_shared_query(table_name)
        } else {
            cql::insert_query(table_name)
        };
        let statement = session.prepare(query).await
            .map_err(|e| {IOError::new(ErrorKind::Other,format!("Error preparing insert for table {}: {}", table_name, e))})?;
        Ok(statement)
    }

    async fn create_blob_table(&self, session:&Session, table_name: &str) -> IOResult<()> {
        let query = cql::channel_blob_table_creation(&table_name);
        session.query_unpaged(query, &[]).await.
            map_err(|e| { IOError::new(ErrorKind::Other, format!("Error creating table {}: {}", &table_name, e))})?;
        Ok(())
    }

    async fn create_typed_table(&self, session:&Session, table_name: &str, kind: ScalarType) -> IOResult<()> {
        let query = cql::channel_typed_table_creation(&table_name, kind);
        session.query_unpaged(query, &[]).await.
            map_err(|e| { IOError::new(ErrorKind::Other, format!("Error creating table {}: {}", &table_name, e))})?;
        Ok(())
    }

    async fn insert(&self, session:&Session, table_name: &str, name: &str, values: impl SerializeRow,) -> IOResult<()> {
        let query = cql::insert_query(&table_name);
        session.query_unpaged(query, values, ).await.
            map_err(|e| { IOError::new(ErrorKind::Other, format!("Error appending to {}: {}", name, e)) })?;
        Ok(())
    }
    async fn execute_statement(&self, session:&Session, statement: &PreparedStatement, name: &str, values: impl SerializeRow,) -> IOResult<()> {
        session.execute_unpaged( &statement,values,)
            .await
            .map_err(|e| { IOError::new(ErrorKind::Other,format!("Error appending to {}: {}", name, e),)})?;
        Ok(())
    }


    async fn insert_blob(&self, session:&Session, table_name: &str, name: &str, id: i64,timestamp_sec: i64, timestamp_nsec:i64, data:Option<Vec<u8>>) -> IOResult<()> {
        if self.arguments.storage_layout.is_shared() {
            self.insert(session, table_name, name,(name, id, timestamp_sec, timestamp_nsec, data), ).await
        } else {
            self.insert(session, table_name, name,(id, timestamp_sec, timestamp_nsec, data), ).await
        }
    }

    async fn insert_typed(&self, session: &Session, table_name: &str, name: &str, kind: ScalarType, id: i64,  timestamp_sec: i64, timestamp_nsec: i64,data: Option<Vec<u8>>,) -> IOResult<()> {
        match kind {
            ScalarType::string => {
                let value = data
                    .map(String::from_utf8)
                    .transpose()
                    .map_err(|e| IOError::new(ErrorKind::InvalidData, e))?;
                insert_record!(self, session, table_name, name, id, timestamp_sec, timestamp_nsec, value)
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
                insert_record!(self, session, table_name, name, id, timestamp_sec, timestamp_nsec, value)
            }

            ScalarType::int8 => {
                let value = data
                    .map(|v| decode(v, i8::from_le_bytes))
                    .transpose()?;
                insert_record!(self, session, table_name, name, id, timestamp_sec, timestamp_nsec, value)
            }

            ScalarType::uint8 => {
                let value = data
                    .map(|v| decode(v, u8::from_le_bytes))
                    .transpose()?
                    .map(i16::from);
                insert_record!(self, session,table_name, name, id, timestamp_sec, timestamp_nsec, value)
            }

            ScalarType::int16 => {
                let value = data
                    .map(|v| decode(v, i16::from_le_bytes))
                    .transpose()?;
                insert_record!(self, session,table_name,  name, id, timestamp_sec, timestamp_nsec, value)
            }

            ScalarType::uint16 => {
                let value = data
                    .map(|v| decode(v, u16::from_le_bytes))
                    .transpose()?
                    .map(i32::from);
                insert_record!(self, session, table_name, name, id, timestamp_sec, timestamp_nsec, value)
            }

            ScalarType::int32 => {
                let value = data
                    .map(|v| decode(v, i32::from_le_bytes))
                    .transpose()?;
                insert_record!(self, session, table_name, name, id, timestamp_sec, timestamp_nsec, value)
            }

            ScalarType::uint32 => {
                let value = data
                    .map(|v| decode(v, u32::from_le_bytes))
                    .transpose()?
                    .map(i64::from);
                insert_record!(self, session, table_name, name, id, timestamp_sec, timestamp_nsec, value)
            }

            ScalarType::int64 => {
                let value = data
                    .map(|v| decode(v, i64::from_le_bytes))
                    .transpose()?;
                insert_record!(self, session, table_name, name, id, timestamp_sec, timestamp_nsec, value)
            }

            ScalarType::uint64 => {
                // CQL has no uint64, so preserve the original bytes as a blob.
                insert_record!(self, session, table_name, name, id, timestamp_sec, timestamp_nsec, data)
            }

            ScalarType::float32 => {
                let value = data
                    .map(|v| decode(v, f32::from_le_bytes))
                    .transpose()?;
                insert_record!(self, session, table_name, name, id, timestamp_sec, timestamp_nsec, value)
            }

            ScalarType::float64 => {
                let value = data
                    .map(|v| decode(v, f64::from_le_bytes))
                    .transpose()?;
                insert_record!(self, session, table_name, name, id, timestamp_sec, timestamp_nsec, value)
            }
        }
    }

    async fn execute_statement_blob(&self, session:&Session, statement: &PreparedStatement, name: &str, id: i64,timestamp_sec: i64,timestamp_nsec: i64,data: Option<Vec<u8>>) -> IOResult<()> {
        if self.arguments.storage_layout.is_shared() {
            self.execute_statement(session, statement, name, (name, id, timestamp_sec, timestamp_nsec, data)).await
        } else {
            self.execute_statement(session, statement, name, (id, timestamp_sec, timestamp_nsec, data)).await
        }
    }

    async fn execute_statement_typed(&self,session: &Session,statement: &PreparedStatement,name: &str,kind: ScalarType,id: i64,timestamp_sec: i64,timestamp_nsec: i64,data: Option<Vec<u8>>,) -> IOResult<()> {
        match kind {
            ScalarType::string => {
                let value = data
                    .map(String::from_utf8)
                    .transpose()
                    .map_err(|e| IOError::new(ErrorKind::InvalidData, e))?;
                exec_statement!(self, session, statement, name, id, timestamp_sec, timestamp_nsec, value)
            }

            ScalarType::bool => {
                let value = data
                    .map(|v| decode(v, |b: [u8; 1]| b[0] != 0))
                    .transpose()?;
                exec_statement!(self, session, statement, name, id, timestamp_sec, timestamp_nsec, value)
            }

            ScalarType::int8 => {
                let value = data
                    .map(|v| decode(v, i8::from_le_bytes))
                    .transpose()?;
                exec_statement!(self, session, statement, name, id, timestamp_sec, timestamp_nsec, value)
            }

            ScalarType::uint8 => {
                let value = data
                    .map(|v| decode(v, u8::from_le_bytes))
                    .transpose()?
                    .map(i16::from);
                exec_statement!(self, session, statement, name, id, timestamp_sec, timestamp_nsec, value)
            }

            ScalarType::int16 => {
                let value = data
                    .map(|v| decode(v, i16::from_le_bytes))
                    .transpose()?;
                exec_statement!(self, session, statement, name, id, timestamp_sec, timestamp_nsec, value)
            }

            ScalarType::uint16 => {
                let value = data
                    .map(|v| decode(v, u16::from_le_bytes))
                    .transpose()?
                    .map(i32::from);
                exec_statement!(self, session, statement, name, id, timestamp_sec, timestamp_nsec, value)
            }

            ScalarType::int32 => {
                let value = data
                    .map(|v| decode(v, i32::from_le_bytes))
                    .transpose()?;
                exec_statement!(self, session, statement, name, id, timestamp_sec, timestamp_nsec, value)
            }

            ScalarType::uint32 => {
                let value = data
                    .map(|v| decode(v, u32::from_le_bytes))
                    .transpose()?
                    .map(i64::from);
                exec_statement!(self, session, statement, name, id, timestamp_sec, timestamp_nsec, value)
            }

            ScalarType::int64 => {
                let value = data
                    .map(|v| decode(v, i64::from_le_bytes))
                    .transpose()?;
                exec_statement!(self, session, statement, name, id, timestamp_sec, timestamp_nsec, value)
            }

            ScalarType::uint64 => {
                // uint64 is represented as CQL blob.
                exec_statement!(self, session, statement, name, id, timestamp_sec, timestamp_nsec, data)
            }

            ScalarType::float32 => {
                let value = data
                    .map(|v| decode(v, f32::from_le_bytes))
                    .transpose()?;
                exec_statement!(self, session, statement, name, id, timestamp_sec, timestamp_nsec, value)
            }

            ScalarType::float64 => {
                let value = data
                    .map(|v| decode(v, f64::from_le_bytes))
                    .transpose()?;
                exec_statement!(self, session, statement, name, id, timestamp_sec, timestamp_nsec, value)
            }
        }
    }
}

fn decode<T, const N: usize>(data: Vec<u8>,f: impl FnOnce([u8; N]) -> T,) -> IOResult<T> {
    let bytes: [u8; N] = data.try_into()
        .map_err(|_| IOError::new(ErrorKind::InvalidData, "Invalid data size"))?;
    Ok(f(bytes))
}

fn is_array(shape:&Option<Vec<u32>>) -> bool {
    match shape{
        None => {false}
        Some(shape) => {
            !shape.is_empty() && (shape[0] > 0)
        }
    }
}
