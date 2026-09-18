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

pub struct Ingestor {
    arguments: Arc<Arguments>,
    db: Arc<DB>,
    insert_statements: RwLock<HashMap<String, PreparedStatement>>,
}


impl Ingestor {
    pub fn new(arguments:Arc<Arguments>, db:Arc<DB>) -> Self {
        Self { arguments, db, insert_statements: RwLock::new(HashMap::new()) }
    }

    fn is_blob_table(&self, kind:ScalarType, shape:&Option<Vec<u32>>) -> bool {
        match shape{
            None => {self.arguments.storage_layout.is_blob()}
            Some(shape) => {
                if shape.is_empty() || shape[0] == 0 {
                    self.arguments.storage_layout.is_blob()
                } else {
                    true
                }
            }
        }
    }

    pub async fn create_table(&self, name:String, kind:ScalarType, shape:Option<Vec<u32>>, size:usize) -> IOResult<()>{
        if let Some(session) = self.db.session() {
            if self.is_blob_table(kind, &shape) {
                self.create_blob_table(session, &name).await?;
            } else {
                self.create_typed_table(session, &name, kind).await?;
            }
            let prepared_statement = self.create_insert_statement(session, &name).await?;
            self.insert_statements.write().await.insert(name, prepared_statement);
        }
        Ok(())
    }

    pub async fn append_record(&self, name:String,  kind:ScalarType, shape:Option<Vec<u32>>, id: u64, tm: (u64, u64), data:Option<Vec<u8>>) -> IOResult<()> {
        if let Some(session) = self.db.enabled_session() {
            let id = i64::try_from(id).map_err(|e| {IOError::new(ErrorKind::Other,format!("Error converting id {}: {}", id, e))})?;
            let timestamp_sec = i64::try_from(tm.0).map_err(|e| {IOError::new(ErrorKind::Other,format!("Error converting tm {:?}: {}", tm, e))})?;
            let timestamp_nsec = i64::try_from(tm.1).map_err(|e| {IOError::new(ErrorKind::Other,format!("Error converting tm {:?}: {}", tm, e))})?;
            let insert_statement = self.insert_statements.read().await.get(&name).cloned();
            //Lock released
            match insert_statement{
                None => {
                    log::warn!("Insert statement with name {} not found", &name);
                    if self.is_blob_table(kind, &shape) {
                        self.insert_blob(session, &name, id, timestamp_sec, timestamp_nsec, data).await?;
                    } else {
                        self.insert_typed(session, &name, kind, id, timestamp_sec, timestamp_nsec, data).await?;
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


    async fn create_insert_statement(&self, session:&Session, name: &str) -> IOResult<PreparedStatement> {
        let query = cql::insert_query(name);
        let statement = session.prepare(query).await
            .map_err(|e| {IOError::new(ErrorKind::Other,format!("Error preparing insert for {}: {}", name, e))})?;
        Ok(statement)
    }

    async fn create_blob_table(&self, session:&Session, name: &str) -> IOResult<()> {
        let query = cql::blob_table_creation(&name);
        session.query_unpaged(query, &[]).await.
            map_err(|e| { IOError::new(ErrorKind::Other, format!("Error creating table {}: {}", &name, e))})?;
        Ok(())
    }

    async fn create_typed_table(&self, session:&Session, name: &str, kind: ScalarType) -> IOResult<()> {
        let query = cql::typed_table_creation(&name, kind);
        session.query_unpaged(query, &[]).await.
            map_err(|e| { IOError::new(ErrorKind::Other, format!("Error creating table {}: {}", &name, e))})?;
        Ok(())
    }

    async fn insert(&self, session:&Session, name: &str, values: impl SerializeRow,) -> IOResult<()> {
        let query = cql::insert_query(&name);
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


    async fn insert_blob(&self, session:&Session, name: &str, id: i64,timestamp_sec: i64, timestamp_nsec:i64, data:Option<Vec<u8>>) -> IOResult<()> {
        self.insert(session, name,(id, timestamp_sec, timestamp_nsec, data), ).await
    }

    async fn insert_typed(&self, session: &Session, name: &str, kind: ScalarType, id: i64,  timestamp_sec: i64, timestamp_nsec: i64,data: Option<Vec<u8>>,) -> IOResult<()> {
        match kind {
            ScalarType::string => {
                let value = data
                    .map(String::from_utf8)
                    .transpose()
                    .map_err(|e| IOError::new(ErrorKind::InvalidData, e))?;
                self.insert(session, name, (id, timestamp_sec, timestamp_nsec, value),).await
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
                self.insert(session, name, (id, timestamp_sec, timestamp_nsec, value),).await
            }

            ScalarType::int8 => {
                let value = data
                    .map(|v| decode(v, i8::from_le_bytes))
                    .transpose()?;
                self.insert(session, name, (id, timestamp_sec, timestamp_nsec, value),).await
            }

            ScalarType::uint8 => {
                let value = data
                    .map(|v| decode(v, u8::from_le_bytes))
                    .transpose()?
                    .map(i16::from);
                self.insert(session, name, (id, timestamp_sec, timestamp_nsec, value),).await
            }

            ScalarType::int16 => {
                let value = data
                    .map(|v| decode(v, i16::from_le_bytes))
                    .transpose()?;
                self.insert(session, name, (id, timestamp_sec, timestamp_nsec, value),).await
            }

            ScalarType::uint16 => {
                let value = data
                    .map(|v| decode(v, u16::from_le_bytes))
                    .transpose()?
                    .map(i32::from);
                self.insert(session, name, (id, timestamp_sec, timestamp_nsec, value),).await
            }

            ScalarType::int32 => {
                let value = data
                    .map(|v| decode(v, i32::from_le_bytes))
                    .transpose()?;
                self.insert(session, name, (id, timestamp_sec, timestamp_nsec, value),).await
            }

            ScalarType::uint32 => {
                let value = data
                    .map(|v| decode(v, u32::from_le_bytes))
                    .transpose()?
                    .map(i64::from);
                self.insert(session, name, (id, timestamp_sec, timestamp_nsec, value),).await
            }

            ScalarType::int64 => {
                let value = data
                    .map(|v| decode(v, i64::from_le_bytes))
                    .transpose()?;
                self.insert(session, name, (id, timestamp_sec, timestamp_nsec, value),).await
            }

            ScalarType::uint64 => {
                // CQL has no uint64, so preserve the original bytes as a blob.
                self.insert(session, name, (id, timestamp_sec, timestamp_nsec, data),).await
            }

            ScalarType::float32 => {
                let value = data
                    .map(|v| decode(v, f32::from_le_bytes))
                    .transpose()?;
                self.insert(session, name, (id, timestamp_sec, timestamp_nsec, value),).await
            }

            ScalarType::float64 => {
                let value = data
                    .map(|v| decode(v, f64::from_le_bytes))
                    .transpose()?;
                self.insert(session, name, (id, timestamp_sec, timestamp_nsec, value),).await
            }
        }
    }

    async fn execute_statement_blob(&self, session:&Session, statement: &PreparedStatement, name: &str, id: i64,timestamp_sec: i64,timestamp_nsec: i64,data: Option<Vec<u8>>) -> IOResult<()> {
        self.execute_statement(session, statement, name, (id, timestamp_sec, timestamp_nsec, data)).await
    }
    async fn execute_statement_typed(&self,session: &Session,statement: &PreparedStatement,name: &str,kind: ScalarType,id: i64,timestamp_sec: i64,timestamp_nsec: i64,data: Option<Vec<u8>>,) -> IOResult<()> {
        match kind {
            ScalarType::string => {
                let value = data
                    .map(String::from_utf8)
                    .transpose()
                    .map_err(|e| IOError::new(ErrorKind::InvalidData, e))?;
                self.execute_statement(session,statement,name,(id, timestamp_sec, timestamp_nsec, value),).await
            }

            ScalarType::bool => {
                let value = data
                    .map(|v| decode(v, |b: [u8; 1]| b[0] != 0))
                    .transpose()?;
                self.execute_statement(session,statement,name,(id, timestamp_sec, timestamp_nsec, value),).await
            }

            ScalarType::int8 => {
                let value = data
                    .map(|v| decode(v, i8::from_le_bytes))
                    .transpose()?;
                self.execute_statement(session,statement,name,(id, timestamp_sec, timestamp_nsec, value),).await
            }

            ScalarType::uint8 => {
                let value = data
                    .map(|v| decode(v, u8::from_le_bytes))
                    .transpose()?
                    .map(i16::from);
                self.execute_statement(session,statement,name,(id, timestamp_sec, timestamp_nsec, value),).await
            }

            ScalarType::int16 => {
                let value = data
                    .map(|v| decode(v, i16::from_le_bytes))
                    .transpose()?;
                self.execute_statement(session,statement,name,(id, timestamp_sec, timestamp_nsec, value),).await
            }

            ScalarType::uint16 => {
                let value = data
                    .map(|v| decode(v, u16::from_le_bytes))
                    .transpose()?
                    .map(i32::from);
                self.execute_statement(session,statement,name,(id, timestamp_sec, timestamp_nsec, value),).await
            }

            ScalarType::int32 => {
                let value = data
                    .map(|v| decode(v, i32::from_le_bytes))
                    .transpose()?;
                self.execute_statement(session,statement,name,(id, timestamp_sec, timestamp_nsec, value),).await
            }

            ScalarType::uint32 => {
                let value = data
                    .map(|v| decode(v, u32::from_le_bytes))
                    .transpose()?
                    .map(i64::from);
                self.execute_statement(session,statement,name,(id, timestamp_sec, timestamp_nsec, value),).await
            }

            ScalarType::int64 => {
                let value = data
                    .map(|v| decode(v, i64::from_le_bytes))
                    .transpose()?;
                self.execute_statement(session,statement,name,(id, timestamp_sec, timestamp_nsec, value),).await
            }

            ScalarType::uint64 => {
                // uint64 is represented as CQL blob.
                self.execute_statement(session,statement,name,(id, timestamp_sec, timestamp_nsec, data),).await
            }

            ScalarType::float32 => {
                let value = data
                    .map(|v| decode(v, f32::from_le_bytes))
                    .transpose()?;
                self.execute_statement(session,statement,name,(id, timestamp_sec, timestamp_nsec, value),).await
            }

            ScalarType::float64 => {
                let value = data
                    .map(|v| decode(v, f64::from_le_bytes))
                    .transpose()?;
                self.execute_statement(session,statement,name,(id, timestamp_sec, timestamp_nsec, value),).await
            }
        }
    }
}

fn decode<T, const N: usize>(data: Vec<u8>,f: impl FnOnce([u8; N]) -> T,) -> IOResult<T> {
    let bytes: [u8; N] = data.try_into()
        .map_err(|_| IOError::new(ErrorKind::InvalidData, "Invalid data size"))?;
    Ok(f(bytes))
}