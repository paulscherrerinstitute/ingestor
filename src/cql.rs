use tokio::io::SimplexStream;
use crate::db::DB;

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

pub fn blob_table_creation(name: &str) -> String {
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
