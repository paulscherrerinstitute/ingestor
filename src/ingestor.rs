use std::collections::HashMap;
use std::io::ErrorKind;
use crate::Arguments;
use crate::DB;
use std::sync::Arc;
use bsread::{IOError, IOResult};
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

    pub async fn create_table(&self, name:String, kind:String, shape:Option<Vec<u32>>, size:usize) -> IOResult<()>{
        if let Some(session) = self.db.session() {
            let query = self.db.creation_query(&name);
            session.query_unpaged(query, &[]).await.
                map_err(|e| {IOError::new(ErrorKind::Other,format!("Error creating table {}: {}", &name,  e))})?;
        }
        let prepared_statement = self.db.create_insert_statement(&name).await?;
        self.insert_statements.write().await.insert(name, prepared_statement);
        Ok(())
    }

    pub async fn append_record(&self, name:String,  id: u64, tm: (u64, u64), data:Option<Vec<u8>>) -> IOResult<()> {
        if let Some(session) = self.db.enabled_session() {
            let id = i64::try_from(id).map_err(|e| {IOError::new(ErrorKind::Other,format!("Error converting id {}: {}", id, e))})?;
            let timestamp_sec = i64::try_from(tm.0).map_err(|e| {IOError::new(ErrorKind::Other,format!("Error converting tm {:?}: {}", tm, e))})?;
            let timestamp_nsec = i64::try_from(tm.1).map_err(|e| {IOError::new(ErrorKind::Other,format!("Error converting tm {:?}: {}", tm, e))})?;

            match self.insert_statements.write().await.get(&name){
                None => {
                    log::warn!("Insert statement with name {} not found", &name);
                    let query = self.db.insert_query(&name);
                    session.query_unpaged(query, (id, timestamp_sec, timestamp_nsec, data), ).await.
                        map_err(|e| {IOError::new(ErrorKind::Other,format!("Error appending to {}: {}", name, e))})?;
                }
                Some(statement) => {
                    session.execute_unpaged( &statement,(id, timestamp_sec, timestamp_nsec, data),)
                        .await
                        .map_err(|e| { IOError::new(ErrorKind::Other,format!("Error appending to {}: {}", name, e),)})?;
                    
                }
            }
/*

*/
        }
        Ok(())
    }
}