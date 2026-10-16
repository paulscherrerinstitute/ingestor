use crate::channel_processor::ChannelProcessor;
use crate::config::Config;
use crate::db::{ScyllaMetrics, DB, DatabaseStatus};
use crate::engine::{Engine, Stats};
use crate::engine_client::EngineClient;
use crate::ingestor::Ingestor;
use crate::processor::{Processor, SourceInfo};
use crate::{exit, Arguments};
use bsread::{EndpointDiag, EndpointState, IOError, IOResult, SocketType};
use log::LevelFilter;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use chrono::{DateTime, Local, Timelike};
use scylla::response::query_result::QueryResult;
use sysinfo::{Pid, ProcessesToUpdate, System};
use tokio::task::JoinHandle;



#[derive(Serialize, Debug, PartialEq, Clone)]
pub enum State {
    Starting,
    Started,
    Paused,
    Stopping,
    Stopped,
    Error,
    Closed,
}

#[derive(Serialize)]
pub struct Status {
    state: State,
    endpoints: HashMap<String, EndpointState>,
    database: DatabaseStatus,
    uptime_seconds: u32,
}

pub struct App {
    arguments:Arc<Arguments>,
    config:Config,
    state:State,
    timer_handle: Option<JoinHandle<()>>,
    engine_client: EngineClient,
    processor: Arc<Processor>,
    channel_processor: Arc<ChannelProcessor>,
    ingestor: Arc<Ingestor>,
    db: Arc<DB>,
    startup: DateTime<Local>,
}


impl App {
    //pub async fn new(arguments:Arguments) -> IOResult<Self> {
    pub fn new(arguments:Arc<Arguments>) -> Self {
        let mut config = Config{sources:Vec::new()};
        match Config::load(&arguments.config_path){
            Ok(c) => {
                config = c
            },
            Err(e) => {
                log::error!("Error loading config from {:?}: {}", &arguments.config_path, e);
            }
        }
        let db =Arc::new(DB::new(arguments.clone()));
        let ingestor=Arc::new(Ingestor::new(arguments.clone(), db.clone()));
        let handle = tokio::runtime::Handle::current();
        let (engine_client, engine_rx) = EngineClient::new();
        let channel_processor = Arc::new(ChannelProcessor::new(arguments.clone(), ingestor.clone()));
        let processor = Arc::new(Processor::new(arguments.clone(), channel_processor.clone()));
        let startup =  chrono::Local::now();
        Engine::launch(arguments.clone(), engine_rx, handle.clone(), processor.clone());
        App {arguments, config, engine_client, processor, channel_processor, db, ingestor, state:State::Stopped, timer_handle: None, startup}
    }

    fn assertState(&self, state:State) -> IOResult<()> {
        if self.state != state {
            Err(IOError::new(ErrorKind::NotFound, format!("Invalid state: {:?}", &self.state)))
        } else {
            Ok(())
        }
    }

    fn assertStateNot(&self, state:State) -> IOResult<()> {
        if self.state == state {
            Err(IOError::new(ErrorKind::NotFound, format!("Invalid state: {:?}", &self.state)))
        } else {
            Ok(())
        }
    }

    pub async fn set_config(&mut self, config: Config) -> IOResult<()> {
        self.config = config;
        if let Err(e) = self.config.save(&self.arguments.config_path) {
                log::error!("Error saving config to {:?}: {}", &self.arguments.config_path, e);
        }
        self.engine_client.send_config(self.config.clone()).await
    }

    pub async fn stop(& mut self) -> IOResult<()> {
        log::info!("Stopping service");
        self.set_state(State::Stopping);
        self.set_db_enabled(false).await;
        if let Some(handle) = self.timer_handle.take() {
            handle.abort();
            self.timer_handle = None;
        }
        self.engine_client.disconnect().await?;
        self.set_state(State::Stopped);
        Ok(())
    }
    fn set_state(&mut self, state: State) {
        log::info!("Setting state to {:?}", state);
        self.state = state;
    }

    async fn set_db_enabled(&mut self, enabled:bool){
        if enabled != self.db.is_enabled() {
            self.db.set_enabled(enabled);
            if (enabled){
                //Initialize BSREAD header info, so will receive header changed events on first message
                log::info!("Enabled database access and resetting BSREAD headers");
                if let Err(e) = self.reset_headers().await{
                    log::error!("Error resetting headers: {}", e);
                }
            } else {
                log::info!("Disabled database access");
            }
        }
    }
    
    pub async fn pause(&mut self) -> IOResult<()> {
        if self.state == State::Stopped{
            self.init(false).await?;
        } else {
            self.assertState(State::Started)?;
        }
        self.set_db_enabled(false).await;
        self.set_state(State::Paused);
        Ok(())
    }

    pub async fn start(&mut self) -> IOResult<()> {
        if self.state != State::Started {
            if !self.is_started() {
                self.init(true).await?;
            }
        }
        self.set_db_enabled(true).await;
        self.set_state(State::Started);
        Ok(())
    }

    async fn init(&mut self, database_enabled:bool) -> IOResult<()> {
        log::info!("Starting service");
        self.set_state(State::Starting);

        self.db.connect().await.inspect_err(|e| {
            log::error!("Error connecting to database: {:?}", e);
            self.set_state(State::Error);
        })?;
        //let session = self.db.read().await.session();
        //self.ingestor.write().await.set_session(session);
        self.ingestor.init().await.inspect_err(|e| {
            log::error!("Error initializing ingestor: {:?}", e);
            self.set_state(State::Error);
        })?;

        if database_enabled {
            self.set_db_enabled(true);
        }

        self.engine_client.send_config(self.config.clone()).await.inspect_err(|e| {
            log::error!("Error sending config in application startup: {:?}", e);
            self.set_state(State::Error);
        })?;
        self.engine_client.connect().await.inspect_err(|e| {
            log::error!("Error connecting in application startup: {:?}", e);
            self.set_state(State::Error);
        })?;
        let engine_client = self.engine_client.clone();
        let ingestor = self.ingestor.clone();
        let processor = self.processor.clone();
        let channel_processor = self.channel_processor.clone();
        let db = self.db.clone();
        let timer_handle  = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            let now = chrono::Local::now();
            let (mut current_day, mut current_hour,mut  current_minute) =
                (now.date_naive(), now.hour(), now.minute());
            loop {
                interval.tick().await;
                let now = chrono::Local::now();
                let (today, hour, minute) = (now.date_naive(), now.hour(), now.minute());
                let (day_changed, hour_changed, minute_changed) =
                    (today != current_day,hour != current_hour,minute != current_minute);

                if let Err(e) = engine_client.on_timer(minute_changed, hour_changed, day_changed).await{
                    log::error!("Error on engine timer: {:?}", e);
                }
                if let Err(e) = processor.on_timer(minute_changed, hour_changed, day_changed).await{
                    log::error!("Error on processor timer: {:?}", e);
                }
                if let Err(e) = channel_processor.on_timer(minute_changed, hour_changed, day_changed).await{
                    log::error!("Error on channel processor timer: {:?}", e);
                }
                if let Err(e) = ingestor.on_timer(minute_changed, hour_changed, day_changed).await{
                    log::error!("Error on ingestor timer: {:?}", e);
                }
                if let Err(e) = db.on_timer(minute_changed, hour_changed, day_changed).await{
                    log::error!("Error on database timer: {:?}", e);
                }
                (current_day, current_hour, current_minute)  = (today, hour, minute);
            }
        });
        self.timer_handle = Some(timer_handle);
        Ok(())
    }


    pub fn is_started(& self) -> bool {
        self.state == State::Started || self.state == State::Paused
    }

    pub async fn wait(&self){
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            if self.state == State::Closed {
                break;
            }
        }
    }

    pub fn state(&self) -> State {
        self.state.clone()
    }
    pub fn arguments(&self) -> Arguments {
        (*self.arguments).clone()
    }

    pub fn config(&self) -> Config {
        self.config.clone()
    }


    pub async fn status(&self) ->  IOResult<Status>  {
        let endpoints = self.engine_client.endpoints().await?;
        let database = self.db.status();
        let uptime_seconds =  (Local::now() - self.startup).as_seconds_f32() as u32;;
        let state = self.state();
        Ok(Status{state, endpoints, database, uptime_seconds})
    }

    pub async fn stats(&self) -> IOResult<Stats> {
        self.engine_client.stats().await
    }

    pub async fn diags(&self,) -> IOResult<HashMap<String, HashMap<EndpointDiag, u32>>> {
        self.engine_client.diags().await
    }

    pub async fn reset_stats(&self,) -> IOResult<()> {
        self.engine_client.reset_stats().await
    }

    pub async fn reset_headers(&self,) -> IOResult<()> {
        self.engine_client.reset_headers().await
    }

    pub async fn log_level(&self) ->  IOResult<String>  { Ok(log::max_level().to_string()) }

    pub async fn set_log_level(&self, level:String) ->  IOResult<()>  {
        let filter = LevelFilter::from_str(&level)
            .map_err(|e| {IOError::new(ErrorKind::InvalidInput,format!("Invalid level: {}", level))})?;
        Ok(log::set_max_level(filter))
    }

    pub async fn sources(&self) -> IOResult<HashMap<String, SourceInfo>> {
        self.processor.sources_info().await
    }
    pub async fn source(&self, source:&str) -> IOResult<SourceInfo> {
        self.processor.source_info(source).await
    }

    pub async fn metrics(&self) -> IOResult<ScyllaMetrics> {
        match self.db.metrics(){
            None => {
                Err(IOError::new(ErrorKind::NotFound, "No connection to database"))
            }
            Some(metrics) => {
                Ok(metrics)
            }
        }
    }

    pub fn db(&self) -> Arc<DB> {
        self.db.clone()
    }


    pub async fn close(&mut self) {
        if let  Err(e) = self.stop().await{
            log::error!("Error stopping application{}", e);
        }
        self.set_state(State::Closed);
        exit(None)
    }
}
impl Drop for App {
    fn drop(&mut self) {

    }
}