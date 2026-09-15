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
use tokio::sync::OnceCell;


#[derive(scylla::DeserializeRow)]
struct TableName {
    table_name: String,
}

pub struct DB {
    arguments: Arc<Arguments>,
    session: OnceCell<Session>,
    enabled: AtomicBool,
}

impl DB {
    pub const KEYSPACE: &str = "databuffer";

    pub fn new(arguments: Arc<Arguments>) -> Self {
        Self { arguments, session: OnceCell::new(), enabled: AtomicBool::new(true) }
    }

    async fn create_session(arguments: &Arguments) -> IOResult<Session> {
        match SessionBuilder::new().known_node(&arguments.database).build().await {
            Ok(session) => {
                log::info!("Database session created for {}", &arguments.database);

                if let Err(e) = session.query_unpaged(cql::now(), &[]).await{
                    log::error!("Error connecting to database {}: {}", &arguments.database, e);
                    //return Err(IOError::new(ErrorKind::ConnectionRefused, format!("Error connecting to database: {}", e)));
                } else {
                    log::info!("Connected to database {}", &arguments.database);
                }

                let query = cql::keyspace_creation();
                if let Err(e) =  session.query_unpaged(query, &[]).await{
                    log::error!("Error creating keyspace {}: {}", Self::KEYSPACE, e);
                }

                Ok(session)
            }
            Err(e) => {
                log::error!("Failed creating session on database {}: {}",  &arguments.database, e);
                Err(IOError::new(ErrorKind::ConnectionRefused, format!("Failed to build  session: {}", e)))
            }
        }
    }

    pub async fn query(&self, query:&str) -> IOResult<QueryResult>{
        if let Some(session) = self.session() {
            let ret = session.query_unpaged(query, &[]).await.
                map_err(|e| {IOError::new(ErrorKind::Other,format!("Error performing query {}: {}", query,  e))})?;
            Ok(ret)
        } else {
            Err(IOError::new(ErrorKind::NotFound, "No session found"))
        }

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

    pub async fn tables(&self) ->  IOResult<Vec<String>> {
        let query = cql::table_names();
        let result = self.query(&query).await?;

        let rows_result = result.into_rows_result()
            .map_err(|e| {
                IOError::new(ErrorKind::Other, format!("Error decoding result: {}", e))
            })?;

        let rows = rows_result
            .rows::<TableName>()
            .map_err(|e| {
                IOError::new(ErrorKind::Other, format!("Error decoding rows: {}", e))
            })?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                IOError::new(ErrorKind::Other, format!("Error decoding row: {}", e))
            })?;
        Ok(rows.into_iter().map(|r| r.table_name).collect())
    }


    pub async fn create_insert_statement(&self, name: &str) -> IOResult<PreparedStatement> {
        let session = self.session()
            .ok_or_else(|| IOError::new(ErrorKind::NotFound, "No session found"))?;
        let query = cql::insert_query(name);
        let statement = session.prepare(query).await
            .map_err(|e| {IOError::new(ErrorKind::Other,format!("Error preparing insert for {}: {}", name, e))})?;
        Ok(statement)
    }


    pub fn metrics(&self) -> Option<ScyllaMetrics>{
        Some(ScyllaMetrics::from_metrics(self.session()?.get_metrics()))
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
