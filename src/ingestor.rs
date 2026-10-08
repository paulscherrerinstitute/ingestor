use std::collections::HashMap;
use std::io::ErrorKind;
use crate::Arguments;
use crate::DB;
use crate::cql;
use crate::codec::*;
use std::sync::Arc;
use bsread::{channel, IOError, IOResult, ScalarType};
use scylla::statement::prepared::PreparedStatement;
use tokio::sync::RwLock;
use crate::arguments::StorageLayout;

pub const TABLE_SCALARS:&str = "scalars";
pub const TABLE_WAVEFORMS:&str = "waveforms";
pub const TABLE_STRINGS:&str = "strings";

pub fn get_table_name(layout:&StorageLayout, channel_name:&str, kind:ScalarType, shape:&Option<Vec<u32>>) -> &'static str {
    match layout {
        StorageLayout::Typed => {
            if channel::is_array(shape) {
                "blob"
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
        if self.arguments.storage_layout.is_typed() {
           if  kind == ScalarType::uint64 {
               return true;
           }
        }
        channel::is_array(shape)
    }

    fn is_scalar_i64_table(&self, kind:ScalarType, shape:&Option<Vec<u32>>) -> bool {
        if self.arguments.storage_layout == StorageLayout::Default {
            return !channel::is_array(shape) && kind != ScalarType::string;
        }
        false
    }


    pub async fn init(&self) -> IOResult<()> {
        let session =  self.db.session().expect("Database session not initialized");
        if self.arguments.storage_layout == StorageLayout::Typed {
            for cql_type in cql::CQL_TYPES {
                let table_name = cql_type;
                let with_type_col = *cql_type=="blob";
                self.db.create_table(table_name, cql_type, with_type_col).await?;
                self.insert_statements.write().await.insert(table_name.to_string(),self.db.create_insert_statement(&table_name, with_type_col).await?);
            }
        }  else if self.arguments.storage_layout == StorageLayout::Default {
            self.db.create_table(TABLE_WAVEFORMS, "blob", true).await?;
            self.db.create_table(TABLE_STRINGS, "text", false).await?;
            self.db.create_table(TABLE_SCALARS, "bigint", true).await?;
            self.insert_statements.write().await.insert(TABLE_WAVEFORMS.to_string(), self.db.create_insert_statement(TABLE_WAVEFORMS, true).await?);
            self.insert_statements.write().await.insert(TABLE_STRINGS.to_string(), self.db.create_insert_statement(TABLE_STRINGS, false).await?);
            self.insert_statements.write().await.insert(TABLE_SCALARS.to_string(), self.db.create_insert_statement(TABLE_SCALARS, true).await?);
        }
        Ok(())
    }

    pub async fn on_header_change(&self, name:String, kind:ScalarType, shape:Option<Vec<u32>>, size:usize) -> IOResult<()>{
        Ok(())
    }

    pub async fn append_record(&self, name:String,  kind:ScalarType, shape:Option<Vec<u32>>, id: u64, tm: (u64, u64), data:Option<Vec<u8>>) -> IOResult<()> {
        if self.db.enabled_session().is_some() {
            let table_name = get_table_name(&self.arguments.storage_layout, &name, kind, &shape);
            let id = id as i64;
            if id <= 0 {
                return Err(IOError::other( format!("Invalid id for {}: {}", name, id)));
            }
            
            //let timestamp_sec = tm.0 as i64;
            //let timestamp_nsec = tm.1 as i64;
            let insert_statement = self.insert_statements.read().await.get(&table_name.to_string()).cloned();
            //Lock released
            let bucket = 0;
            match insert_statement{
                None => {
                    log::warn!("Insert statement with name {} not found", &name);
                    if self.is_scalar_i64_table(kind, &shape) {
                        self.insert_asi64(&table_name, &name, kind, bucket, id, data).await?;
                    } else if self.is_blob_table(kind, &shape) {
                        self.insert_blob(&table_name, &name, kind, bucket, id, data).await?;
                    } else {
                        self.insert_typed(&table_name, &name, kind, bucket, id, data).await?;
                    }
                }
                Some(statement) => {
                    if self.is_scalar_i64_table(kind, &shape) {
                        self.execute_statement_i64(&statement, &name, kind, bucket, id, data).await?;
                    } else if self.is_blob_table(kind, &shape) {
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
}
