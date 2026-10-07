use bsread::{IOError, IOResult, ErrorKind, ScalarType};
use scylla::client::session::Session;
use tokio::io::SimplexStream;
use crate::db::DB;


pub const COLUMN_CHANNEL:&str = "channel";
pub const COLUMN_BUCKET:&str = "bucket";
pub const COLUMN_ID:&str = "id";
//pub const COLUMN_SECS:&str = "timestamp_sec";
//pub const COLUMN_NANOS:&str = "timestamp_nsec";

pub const COLUMN_TYPE:&str = "dtype";
pub const COLUMN_DATA:&str = "data";
pub const CQL_TYPES: &[&str] = &["text", "boolean", "tinyint", "smallint", "int", "bigint", "float", "double", "blob"];


pub fn now() -> String {
    "SELECT now() FROM system.local".to_string()
}

pub fn keyspace_creation() -> String {
    format!(
        "CREATE KEYSPACE IF NOT EXISTS {} \
            WITH REPLICATION = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}",
        DB::keyspace()
    )
}


//WITH replication = {'class': 'NetworkTopologyStrategy', '<datacenter_name>': 3}
//AND durable_writes = true;

pub fn table_names() -> String {
    format!("SELECT table_name \
                 FROM system_schema.tables \
                 WHERE keyspace_name = '{}'",
            DB::keyspace()
    )
}

pub fn columns(table_name:&str) -> String {
    format!("SELECT column_name \
                 FROM system_schema.columns \
                 WHERE keyspace_name = '{}' \
                 AND table_name = '{}'",
            DB::keyspace(),
            table_name,
    )
}

pub fn column_type(table_name:&str, column_name:&str) -> String {
    format!("SELECT type \
                 FROM system_schema.columns \
                 WHERE keyspace_name = '{}' \
                 AND table_name = '{}' \
                 AND column_name = '{}'",
            DB::keyspace(),
            table_name,
            column_name
    )
}


pub fn keyspace_names() -> String {
    "SELECT keyspace_name FROM system_schema.keyspaces".to_string()
}

pub fn create_data_table(name: &str, cql_type: &str, with_type_col: bool) -> String {
    let type_col = if with_type_col {format!("\n                {} tinyint,", COLUMN_TYPE)} else {String::new()};
    format!(
        r#"
            CREATE TABLE IF NOT EXISTS "{}"."{}" (
                {} text,
                {} bigint,
                {} bigint,
                {} {},{}
                PRIMARY KEY (({}, {}), {})
            ) WITH CLUSTERING ORDER BY ({} ASC)
              AND default_time_to_live = 259200
              AND gc_grace_seconds = 0
              AND compaction = {{'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': 8, 'compaction_window_unit': 'HOURS'}};
         "#,
        DB::keyspace(), name.replace('"', "\"\""),
        COLUMN_CHANNEL,
        COLUMN_BUCKET,
        COLUMN_ID,
        COLUMN_DATA, cql_type, type_col,
        COLUMN_CHANNEL,COLUMN_BUCKET, COLUMN_ID,
        COLUMN_ID,
    )
}


pub fn insert_data_query(name: &str, with_type_col: bool) -> String {
    let type_col = if with_type_col { format!(", {}", COLUMN_TYPE) } else {String::new()};
    let type_val = if with_type_col { ", ?".to_string() } else {String::new()};
    format!(
        r#"
            INSERT INTO "{}"."{}"
                 ({}, {}, {}, {}{})
            VALUES (?, ?, ?, ?{})
        "#,
        DB::keyspace(), name.replace('"', "\"\""),
        COLUMN_CHANNEL, COLUMN_BUCKET, COLUMN_ID, COLUMN_DATA, type_col, type_val
    )
}


pub fn kind_to_cql_type(kind:ScalarType) -> &'static str {
    match kind {
        ScalarType::string  => "text",
        ScalarType::bool    => "boolean",
        ScalarType::int8    => "tinyint",
        ScalarType::uint8   => "smallint",
        ScalarType::int16   => "smallint",
        ScalarType::uint16  => "int",
        ScalarType::int32   => "int",
        ScalarType::uint32  => "bigint",
        ScalarType::int64   => "bigint",
        ScalarType::uint64  => "blob",
        ScalarType::float32 => "float",
        ScalarType::float64 => "double"
    }
}

