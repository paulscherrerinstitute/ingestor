use crate::db::DB;


pub fn now() -> String {
    "SELECT now() FROM system.local".to_string()
}

pub fn keyspace_creation() -> String {
    format!(
        "CREATE KEYSPACE IF NOT EXISTS {} \
                        WITH REPLICATION = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}",
        DB::KEYSPACE
    )
}

pub fn table_names() -> String {
    format!("SELECT table_name \
             FROM system_schema.tables \
             WHERE keyspace_name = '{}'",
            DB::KEYSPACE)
}

pub fn creation_query(name: &str) -> String {
    format!(
        r#"
        CREATE TABLE IF NOT EXISTS "{}"."{}" (
            id bigint PRIMARY KEY,
            timestamp_sec bigint,
            timestamp_nsec bigint,
            data blob
        )
        "#,
        DB::KEYSPACE,
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
        DB::KEYSPACE,
        name.replace('"', "\"\"")
    )
}
