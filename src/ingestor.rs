use std::io::ErrorKind;
use crate::Arguments;
use crate::DB;
use std::sync::Arc;
use bsread::{IOError, IOResult};
use scylla::errors::ExecutionError;


pub struct Ingestor {
    arguments: Arc<Arguments>,
    db: Arc<DB>,
}


impl Ingestor {
    pub fn new(arguments:Arc<Arguments>, db:Arc<DB>) -> Self {
        Self { arguments, db }
    }


    pub async fn create_table(&self, name:String, kind:String, shape:Option<Vec<u32>>, size:usize) -> IOResult<()>{
        if let Some(session) = self.db.session() {
            let cql = format!(
                    r#"
            CREATE TABLE IF NOT EXISTS "{}" (
                id bigint PRIMARY KEY,
                timestamp_sec bigint,
                timestamp_nsec bigint,
                data blob
            )
            "#,
                name.replace('"', "\"\"")
            );
            session.query_unpaged(cql, &[]).await.
                map_err(|e| {IOError::new(ErrorKind::Other,format!("Error creating table {}: {}", name,  e))})?;
        }
        Ok(())
    }

    pub async fn append_record(&self, name:String,  id: u64, tm: (u64, u64), data:Option<Vec<u8>>) -> IOResult<()> {
        if let Some(session) = self.db.session() {
            /*
            let id = i64::try_from(id).map_err(|e| {IOError::new(ErrorKind::Other,format!("Error converting id {}: {}", id, e))})?;
            let timestamp_sec = i64::try_from(tm.0).map_err(|e| {IOError::new(ErrorKind::Other,format!("Error converting tm {:?}: {}", tm, e))})?;
            let timestamp_nsec = i64::try_from(tm.1).map_err(|e| {IOError::new(ErrorKind::Other,format!("Error converting tm {:?}: {}", tm, e))})?;

            let cql = format!(
                r#"
            INSERT INTO "{}"
                (id, timestamp_sec, timestamp_nsec, data)
            VALUES (?, ?, ?, ?)
            "#,
                name.replace('"', "\"\"")
            );

            session.query_unpaged(cql, (id, timestamp_sec, timestamp_nsec, data), ).await.
                map_err(|e| {IOError::new(ErrorKind::Other,format!("Error appending to {}: {}", name, e))})?;
             */
        }
        Ok(())
    }
}