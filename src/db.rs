use std::collections::HashMap;
use crate::{Arguments, app, cql};
use bsread::{IOError, IOResult};
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::observability::metrics::Metrics;
use serde::Serialize;
use std::io::ErrorKind;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use scylla::errors::MetadataError::Keyspaces;
use scylla::response::query_result::QueryResult;
use scylla::statement::prepared::PreparedStatement;
use scylla::deserialize::row::DeserializeRow;
use tokio::sync::{OnceCell, RwLock};
use std::sync::OnceLock;
use scylla::_macro_internal::SerializeRow;
use crate::arguments::StorageLayout;
use crate::cql::COLUMN_CHANNEL;

#[derive(scylla::DeserializeRow)]
struct TableName {
    table_name: String,
}

#[derive(scylla::DeserializeRow)]
struct KeyspaceName {
    keyspace_name: String,
}

#[derive(scylla::DeserializeRow)]
struct Column {
    column_name: String
}

#[derive(scylla::DeserializeRow)]
struct ColumnType {
    cql_type: String,
}

#[derive(scylla::DeserializeRow)]
struct ChannelName {
    channel_name: String,
}
#[derive(scylla::DeserializeRow, Debug, Clone)]
pub struct ChannelMetadata {
    pub channel_name: String,
    pub from_pulse_id: i64,
    pub dtype: i8,
    pub element_count: i32,
}

pub struct DB {
    arguments: Arc<Arguments>,
    session: OnceCell<Session>,
    enabled: AtomicBool,
}

const SHARED_TABLE_NAME:&str = "data";

const KEYSPACE_DEFAULT:&str = "beam_data";


static KEYSPACE: OnceLock<String> = OnceLock::new();

impl DB {
    pub fn new(arguments: Arc<Arguments>) -> Self {
        let keyspace = match arguments.storage_layout{
            StorageLayout::Default => {KEYSPACE_DEFAULT.to_string()},
            _ => {format! ("db_{:?}", arguments.storage_layout).to_lowercase()}
        };
        KEYSPACE.set(keyspace.to_string()).expect("KEYSPACE has already been initialized");
        Self { arguments, session: OnceCell::new(), enabled: AtomicBool::new(false) }
    }

    pub fn keyspace() -> &'static String {
        KEYSPACE.get().expect("KEYSPACE has not been initialized")
    }


    async fn create_session(arguments: &Arguments) -> IOResult<Session> {
        match SessionBuilder::new().known_node(&arguments.database).build().await {
            Ok(session) => {
                log::info!("Database session created for {}", &arguments.database);

                if let Err(e) = session.query_unpaged(cql::now(), &[]).await{
                    log::error!("Error connecting to database {}: {}", &arguments.database, e);
                    return Err(IOError::new(ErrorKind::ConnectionRefused, format!("Error connecting to database: {}", e)));
                } else {
                    log::info!("Connected to database {}", &arguments.database);
                }

                if arguments.create {
                    let query = cql::keyspace_creation();
                    if let Err(e) = session.query_unpaged(query, &[]).await {
                        log::error!("Error creating keyspace {}: {}", Self::keyspace(), e);
                    }
                } else {
                    if !Self::keyspace_exists(&session).await?{
                        log::error!("Keyspace does not exist: {}", Self::keyspace());
                        return Err(IOError::new(ErrorKind::NotFound, format!("Keyspace does not exist: {}", Self::keyspace())));
                    }
                }
                Ok(session)
            }
            Err(e) => {
                log::error!("Failed creating session on database {}: {}",  &arguments.database, e);
                Err(IOError::new(ErrorKind::ConnectionRefused, format!("Failed to build  session: {}", e)))
            }
        }
    }

    fn get_session(&self) -> IOResult<&Session> {
        if let Some(session) = self.session() {
            Ok(session)
        } else {
            Err(IOError::new(ErrorKind::NotFound, "Session not initialized"))
        }
    }

    pub async fn query(&self, query:&str) -> IOResult<QueryResult>{
        let ret = self.get_session()?.query_unpaged(query, &[]).await.
            map_err(|e| {IOError::new(ErrorKind::Other,format!("Error performing query {}: {}", query,  e))})?;
        Ok(ret)
    }

    pub async fn create_data_table(&self, table_name: &str, cql_type:&str, with_type_col: bool) -> IOResult<()> {
        if self.arguments.create {
            let query = cql::create_data_table(&table_name, cql_type, with_type_col);
            if let Err(e) = self.query(&query).await {
                log::error!("Error creating data table: {}", table_name);
                return Err(e);
            }
        } else {
            if !self.table_exists(table_name).await?{
                log::error!("Table does not exist: {}", table_name);
                return Err(IOError::new(ErrorKind::NotFound, format!("Table does not exist: {}", table_name)));
            }
            let table_type = self.column_type(table_name, cql::COLUMN_DATA).await?;
            if  table_type != cql_type{
                return Err(IOError::new(ErrorKind::NotFound, format!("Invalid table data type: {} ({})", table_name, cql_type )));
            }
        }
        Ok(())
    }

    pub async fn create_channel_metadata_table(&self, table_name: &str) -> IOResult<()> {
        if self.arguments.create {
            let query = cql::create_channel_metadata_table(&table_name);
            if let Err(e) = self.query(&query).await {
                log::error!("Error creating channel metadata table: {}", table_name);
                return Err(e);
            }
        } else {
            if !self.table_exists(table_name).await?{
                log::error!("Channel metadata table does not exist: {}", table_name);
                return Err(IOError::new(ErrorKind::NotFound, format!("Table does not exist: {}", table_name)));
            }
        }
        Ok(())
    }


    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    pub async fn connect(&self) -> IOResult<()> {
        //loop {
        if self.is_connected() {
            return Ok(());
        }
        match Self::create_session(&self.arguments).await {
            Ok(session) => {
                self.session.set(session)
                    .map_err(|e| { IOError::new(ErrorKind::ConnectionRefused, format!("Error creating session: {}", e)) })?;
                Ok(())
            }
            Err(e) => {
                Err(e)
            }
        }
        //tokio::time::sleep(Duration::from_millis(5000)).await;
        //}
    }

    pub fn is_connected(&self) -> bool {
        self.session.get().is_some()
    }

    pub fn session(&self) -> Option<&Session> {
        self.session.get()
    }

    pub fn enabled_session(&self) -> Option<&Session> {
        if self.is_enabled() {
            self.session.get()
        } else {
            None
        }
    }

    async fn keyspace_exists(session: &Session) -> IOResult<bool> {
        Ok(Self::keyspaces(session).await.unwrap_or_default().contains(Self::keyspace()))
    }

    pub async fn keyspaces(session: &Session) ->  IOResult<Vec<String>> {
        let query = cql::keyspace_names();
        let result = session.query_unpaged(query.as_str(), &[]).await.
            map_err(|e| {IOError::new(ErrorKind::Other,format!("Error performing query {}: {}", &query,  e))})?;
        let rows = Self::get_rows(result)?;
        Ok(rows.into_iter().map(|r:KeyspaceName| r.keyspace_name).collect())
    }

    pub async fn table_exists(&self, name:&str) -> IOResult<bool> {
        Ok(self.tables().await.unwrap_or_default().contains(&name.to_string()))
    }

    pub async fn tables(&self) -> IOResult<Vec<String>> {
        let rows = self.query_rows::<TableName>(&cql::table_names()).await?;
        Ok(rows.into_iter().map(|r| r.table_name).collect())
    }

    pub async fn columns(&self,table: &str,) -> IOResult<Vec<String>> {
        let rows = self.query_rows::<Column>(&cql::columns(table),).await?;
        Ok(rows.into_iter().map(|r| r.column_name).collect())
    }

    pub async fn column_type(&self, table: &str,column: &str,) -> IOResult<String> {
        let rows = self.query_rows::<ColumnType>(&cql::column_type(table, column),).await?;
        if rows.is_empty() {
            Err(IOError::other(format!("Column not found: {}-{}", table, column)))
        } else {
            Ok(rows[0].cql_type.clone())
        }
    }

    pub async fn create_data_insert_statement(&self, table_name: &str, with_type_col: bool) -> IOResult<PreparedStatement> {
        let query =  cql::insert_data_query(table_name,with_type_col);
        let statement = self.get_session()?.prepare(query).await
            .map_err(|e| {IOError::other(format!("Error preparing insert for table {}: {}", table_name, e))})?;
        Ok(statement)
    }

    pub async fn create_channel_metadata_insert_statement(&self, table_name: &str) -> IOResult<PreparedStatement> {
        let query =  cql::insert_channel_metadata_query(table_name);
        let statement = self.get_session()?.prepare(query).await
            .map_err(|e| {IOError::other(format!("Error preparing insert for table {}: {}", table_name, e))})?;
        Ok(statement)
    }


    pub async fn insert_with_type(&self, table_name: &str, name: &str, values: impl SerializeRow,) -> IOResult<()> { ;
        let query = cql::insert_data_query(&table_name, true);
        self.get_session()?.query_unpaged(query, values, ).await.
            map_err(|e| { IOError::other( format!("Error appending {}: {}", name, e)) })?;
        Ok(())
    }

    pub async fn insert(&self, table_name: &str, name: &str, values: impl SerializeRow,) -> IOResult<()> { ;
        let query = cql::insert_data_query(&table_name, false);
        self.get_session()?.query_unpaged(query, values, ).await.
            map_err(|e| { IOError::other( format!("Error appending {}: {}", name, e)) })?;
        Ok(())
    }
    pub async fn execute_statement(&self, statement: &PreparedStatement, name: &str, values: impl SerializeRow,) -> IOResult<()> {
        self.get_session()?.execute_unpaged( &statement,values,)
            .await
            .map_err(|e| { IOError::other(format!("Error appending {}: {}", name, e),)})?;
        Ok(())
    }

    pub async fn fetch_channels(&self,table: &str,) -> IOResult<Vec<String>> {
        let rows = self.query_rows::<ChannelName>(&cql::fetch_channels_query(table),).await?;
        Ok(rows.into_iter().map(|r| r.channel_name).collect())
    }

    pub async fn fetch_channel_metadata(&self,table: &str, channel: &str) -> IOResult<ChannelMetadata> {
        let mut rows = self.query_rows::<ChannelMetadata>(&cql::fetch_channel_metadata_query(table, channel),).await?;
        if rows.is_empty() {
            Err(IOError::other(format!("Channel not found: {}-{}", table, channel)))
        } else {
            Ok(rows.remove(0))
        }
    }

    pub fn metrics(&self) -> Option<ScyllaMetrics>{
        Some(ScyllaMetrics::from_metrics(self.session()?.get_metrics()))
    }

    async fn query_rows<T>(&self, query: &str) -> IOResult<Vec<T>>
    where
        T: for<'a> DeserializeRow<'a, 'a>,
    {
        let result = self.query(query).await?;
        Self::get_rows(result)
    }

    fn get_rows<T>(result:QueryResult) -> IOResult<Vec<T>>
    where
        T: for<'a> DeserializeRow<'a, 'a>,
    {
        let rows_result = result
            .into_rows_result()
            .map_err(|e| {
                IOError::other(format!("Error decoding result: {}", e),)
            })?;

        rows_result
            .rows::<T>()
            .map_err(|e| {
                IOError::other(format!("Error decoding rows: {}", e),)
            })?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                IOError::other(format!("Error decoding row: {}", e),)
            })
    }
}


#[derive(Serialize)]
pub struct ScyllaMetrics {
    pub requests: u64,
    pub errors: u64,
    pub retries: u64,
    pub mean_rate: f64,
    pub one_minute_rate: f64,
    pub five_minute_rate: f64,
    pub fifteen_minute_rate: f64,
    pub latency: Option<ScyllaLatency>,
    pub total_connections: u64,
    pub connection_timeouts: u64,
    pub request_timeouts: u64,
}

#[derive(Serialize)]
pub struct ScyllaLatency {
    pub min: u64,
    pub mean: u64,
    pub median: u64,
    pub stddev: u64,
    pub p95: u64,
    pub p99: u64,
    pub max: u64,
}

impl ScyllaMetrics {
    fn from_metrics(metrics: Arc<Metrics>) -> ScyllaMetrics {
        let latency = match  &metrics.get_snapshot(){
            Ok(snapshot) => {
                Some(ScyllaLatency {
                    min: snapshot.min,
                    mean: snapshot.mean,
                    median: snapshot.median,
                    stddev: snapshot.stddev,
                    p95: snapshot.percentile_95,
                    p99: snapshot.percentile_99,
                    max: snapshot.max,
                })
            }
            Err(e) => {
                log::debug!("Scylla snapshot error: {}", e);
                None
            }
        };

        Self {
            requests: metrics.get_requests_unpaged_num(),
            errors: metrics.get_errors_unpaged_num(),
            retries: metrics.get_retries_num(),

            mean_rate: metrics.get_mean_rate(),
            one_minute_rate: metrics.get_one_minute_rate(),
            five_minute_rate: metrics.get_five_minute_rate(),
            fifteen_minute_rate: metrics.get_fifteen_minute_rate(),

            latency,
            total_connections: metrics.get_total_connections(),
            connection_timeouts: metrics.get_connection_timeouts(),
            request_timeouts: metrics.get_request_timeouts(),
        }
    }
}
