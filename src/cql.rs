use bsread::{IOError, IOResult, ErrorKind, ScalarType};
use scylla::client::session::Session;
use tokio::io::SimplexStream;
use crate::db::DB;

pub const CQL_TYPES: &[&str] = &["text", "boolean", "tinyint", "smallint", "int", "bigint", "float", "double", "blob"];

pub const COLUMN_CHANNEL:&str = "channel_name";
pub const COLUMN_BUCKET:&str = "pulse_bucket";
pub const COLUMN_ID:&str = "pulse_id";
pub const COLUMN_TYPE:&str = "dtype";
pub const COLUMN_DATA:&str = "val";
pub const COLUMN_FROM:&str = "from_pulse_id";
pub const COLUMN_COUNT:&str = "element_count";


//pub const COLUMN_SECS:&str = "timestamp_sec";
//pub const COLUMN_NANOS:&str = "timestamp_nsec";



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

pub fn create_channel_metadata_table(name: &str) -> String {
    format!(
        r#"
            CREATE TABLE IF NOT EXISTS "{}"."{}" (
                {} text,
                {} bigint,
                {} tinyint,
                {} int,
                PRIMARY KEY (({}), {})
            ) WITH CLUSTERING ORDER BY ({} DESC)
              AND default_time_to_live = 2678400
              AND gc_grace_seconds = 0
              AND compaction = {{'class': 'SizeTieredCompactionStrategy'}};
         "#,
        DB::keyspace(), name.replace('"', "\"\""),
        COLUMN_CHANNEL,
        COLUMN_FROM,
        COLUMN_TYPE,
        COLUMN_COUNT,
        COLUMN_CHANNEL,COLUMN_FROM,
        COLUMN_FROM,
    )
}

pub fn insert_channel_metadata_query(name: &str) -> String {
    format!(
        r#"
            INSERT INTO "{}"."{}"
                 ({}, {}, {}, {})
            VALUES (?, ?, ?, ?)
        "#,
        DB::keyspace(), name.replace('"', "\"\""),
        COLUMN_CHANNEL, COLUMN_FROM, COLUMN_TYPE, COLUMN_COUNT
    )
}

pub fn fetch_channels_query(name: &str) -> String {
    format!(
        r#"
            SELECT DISTINCT {}
            FROM "{}"."{}";
        "#,
        COLUMN_CHANNEL,
        DB::keyspace(), name.replace('"', "\"\""),
    )
}

pub fn fetch_channel_metadata_query(name: &str, channnel: &str) -> String {
    format!(
        r#"
            SELECT {}, {}, {}, {}
            FROM "{}"."{}"
            WHERE {} = '{}'
            LIMIT 1;
        "#,
        COLUMN_CHANNEL, COLUMN_FROM, COLUMN_TYPE, COLUMN_COUNT,
        DB::keyspace(), name.replace('"', "\"\""),
        COLUMN_CHANNEL, channnel
    )
}






