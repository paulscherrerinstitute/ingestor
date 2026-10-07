use bsread::{IOError, IOResult, ErrorKind, ScalarType};
use scylla::client::session::Session;
use tokio::io::SimplexStream;
use crate::db::DB;


pub const COLUMN_CHANNEL:&str = "channel";
pub const COLUMN_ID:&str = "id";
pub const COLUMN_SECS:&str = "timestamp_sec";
pub const COLUMN_NANOS:&str = "timestamp_nsec";
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

pub fn channel_blob_table_creation(name: &str) -> String {
    format!(
        r#"
        CREATE TABLE IF NOT EXISTS "{}"."{}" (
            {} bigint PRIMARY KEY,
            {} bigint,
            {} bigint,
            {} blob
        )
        "#,
        DB::keyspace(), name.replace('"', "\"\""),
        COLUMN_ID,
        COLUMN_SECS,
        COLUMN_NANOS,
        COLUMN_DATA
    )
}

pub fn channel_typed_table_creation(name: &str, kind:ScalarType) -> String {
    let cql_type = kind_to_cql_type(kind);
    format!(
        r#"
        CREATE TABLE IF NOT EXISTS "{}"."{}" (
            {} bigint PRIMARY KEY,
            {} bigint,
            {} bigint,
            {} {})
        "#,
        DB::keyspace(), name.replace('"', "\"\""),
        COLUMN_ID,
        COLUMN_SECS,
        COLUMN_NANOS,
        COLUMN_DATA, cql_type
    )
}

pub fn blob_table_creation(name: &str) -> String {
    format!(
        r#"
        CREATE TABLE IF NOT EXISTS "{}"."{}" (
            {} text,
            {} bigint,
            {} bigint,
            {} bigint,
            {} blob,
            PRIMARY KEY ({}, {})
        ) WITH CLUSTERING ORDER BY (id ASC);
        "#,
        DB::keyspace(), name.replace('"', "\"\""),
        COLUMN_CHANNEL,
        COLUMN_ID,
        COLUMN_SECS,
        COLUMN_NANOS,
        COLUMN_DATA,
        COLUMN_CHANNEL, COLUMN_ID
    )
}

pub fn typed_table_creation(name: &str, cql_type:&str) -> String {
    format!(
        r#"
        CREATE TABLE IF NOT EXISTS "{}"."{}" (
            {} text,
            {} bigint,
            {} bigint,
            {} bigint,
            {} {},
            PRIMARY KEY ({}, {})
        ) WITH CLUSTERING ORDER BY (id ASC);
        "#,
        DB::keyspace(), name.replace('"', "\"\""),
        COLUMN_CHANNEL,
        COLUMN_ID,
        COLUMN_SECS,
        COLUMN_NANOS,
        COLUMN_DATA, cql_type,
        COLUMN_CHANNEL, COLUMN_ID
    )
}



pub fn insert_query(name: &str) -> String {
    format!(
        r#"
            INSERT INTO "{}"."{}"
                ({}, {}, {}, {})
            VALUES (?, ?, ?, ?)
        "#,
        DB::keyspace(),name.replace('"', "\"\""),
        COLUMN_ID, COLUMN_SECS, COLUMN_NANOS, COLUMN_DATA,
    )
}

pub fn insert_shared_query(name: &str) -> String {
    format!(
        r#"
            INSERT INTO "{}"."{}"
                 ({}, {}, {}, {}, {})
            VALUES (?, ?, ?, ?, ?)
        "#,
        DB::keyspace(), name.replace('"', "\"\""),
        COLUMN_CHANNEL, COLUMN_ID, COLUMN_SECS, COLUMN_NANOS, COLUMN_DATA,
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

