use bsread::{IOError, IOResult, ErrorKind, ScalarType};
use tokio::io::SimplexStream;
use crate::db::DB;

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

pub fn channel_blob_table_creation(name: &str) -> String {
    format!(
        r#"
        CREATE TABLE IF NOT EXISTS "{}"."{}" (
            id bigint PRIMARY KEY,
            timestamp_sec bigint,
            timestamp_nsec bigint,
            data blob
        )
        "#,
        DB::keyspace(),
        name.replace('"', "\"\"")
    )
}

pub fn channel_typed_table_creation(name: &str, kind:ScalarType) -> String {
    let cql_type = kind_to_cql_type(kind);
    format!(
        r#"
        CREATE TABLE IF NOT EXISTS "{}"."{}" (
            id bigint PRIMARY KEY,
            timestamp_sec bigint,
            timestamp_nsec bigint,
            data {})
        "#,
        DB::keyspace(),
        name.replace('"', "\"\""),
        cql_type
    )
}

pub fn blob_table_creation(name: &str) -> String {
    format!(
        r#"
        CREATE TABLE IF NOT EXISTS "{}"."{}" (
            channel text,
            id bigint,
            timestamp_sec bigint,
            timestamp_nsec bigint,
            data blob,
            PRIMARY KEY (channel, id)
        ) WITH CLUSTERING ORDER BY (id ASC);
        "#,
        DB::keyspace(),
        name.replace('"', "\"\""),
    )
}

pub fn typed_table_creation(name: &str, cql_type:&str) -> String {
    format!(
        r#"
        CREATE TABLE IF NOT EXISTS "{}"."{}" (
            channel text,
            id bigint,
            timestamp_sec bigint,
            timestamp_nsec bigint,
            data {},
            PRIMARY KEY (channel, id)
        ) WITH CLUSTERING ORDER BY (id ASC);
        "#,
        DB::keyspace(),
        name.replace('"', "\"\""),
        cql_type
    )
}



pub fn insert_query(name: &str) -> String {
    format!(
        r#"
            INSERT INTO "{}"."{}"
                (id, timestamp_sec, timestamp_nsec, data)
            VALUES (?, ?, ?, ?)
        "#,
        DB::keyspace(),
        name.replace('"', "\"\"")
    )
}

pub fn insert_shared_query(name: &str) -> String {
    format!(
        r#"
            INSERT INTO "{}"."{}"
                (channel, id, timestamp_sec, timestamp_nsec, data)
            VALUES (?, ?, ?, ?, ?)
        "#,
        DB::keyspace(),
        name.replace('"', "\"\"")
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

