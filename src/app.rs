use crate::channel_processor::ChannelProcessor;
use crate::config::Config;
use crate::db::{ScyllaMetrics, DB};
use crate::engine::Engine;
use crate::engine_client::EngineClient;
use crate::ingestor::Ingestor;
use crate::processor::{Processor, SourceInfo};
use crate::Arguments;
use bsread::{EndpointDiag, EndpointState, IOError, IOResult, SocketType};
use log::LevelFilter;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
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
    endpoints: HashMap<String, EndpointState>
}

impl Status {
    pub fn new(endpoints: HashMap<String, EndpointState>) -> Self {
        Self{endpoints}
    }
}

#[derive(Serialize)]
pub struct Stats {
    pub received: u32,
    pub errors: u32,
    pub dropped: u32,
    pub processing: u32,
    pub processed: u32,
    pub received_rate: f32,
    pub errors_rate: f32,
    pub dropped_rate: f32,
    pub processed_rate: f32,
    pub duplicated_sources: u32,
    pub disabled_sources: u32,
    pub connected_sources: u32,
    pub connecting_sources: u32,
    pub disconnected_sources: u32,
    pub cpu:f32,
    pub memory:u64,
    pub files:usize,
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
        Engine::launch(arguments.clone(), engine_rx, handle.clone(), processor.clone());
        App {arguments, config, engine_client, processor, channel_processor, db, ingestor, state:State::Starting, timer_handle: None}
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

    pub fn process_resources() -> (f32, u64, usize) {
        let mut system = System::new();
        let pid = Pid::from_u32(std::process::id());
        system.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
        if let Some(process) = system.process(pid) {
            let open_files = system
                .process(pid)
                .and_then(|process| process.open_files())
                .unwrap_or(0);
            (process.cpu_usage(), process.memory(), open_files)
        } else {
            (0.0, 0, 0)
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
        log::info!("Stpping service");
        self.set_state(State::Stopping);
        if let Some(handle) = self.timer_handle.take() {
            handle.abort();
            self.timer_handle = None;
        }
        self.engine_client.disconnect().await?;
        self.set_state(State::Stopped);
        Ok(())
    }
    fn set_state(&mut self, state: State) {
        log::info!("Setting state to {:?}", &self.state);
        self.state = state;
    }

    pub async fn pause(&mut self) -> IOResult<()> {
        self.assertState(State::Started)?;
        self.db.set_enabled(false);
        self.set_state(State::Paused);
        Ok(())
    }
    pub fn set_db_enabled(&mut self, enabled:bool) -> IOResult<()> {
        self.db.set_enabled(enabled);
        Ok(())
    }



    pub async fn start(&mut self) -> IOResult<()> {
        if !self.is_started(){
            log::info!("Starting service");
            self.set_state(State::Starting);

            self.db.connect().await.inspect_err(|e| {
                log::error!("Error connecting to database: {:?}", e);
                self.set_state(State::Error);
            })?;
            //let session = self.db.read().await.session();
            //self.ingestor.write().await.set_session(session);

            self.engine_client.send_config(self.config.clone()).await.inspect_err(|e| {
                log::error!("Error sending config in application startup: {:?}", e);
                self.set_state(State::Error);
            })?;
            self.engine_client.connect().await.inspect_err(|e| {
                log::error!("Error connecting in application startup: {:?}", e);
                self.set_state(State::Error);
            })?;
            let engine_client = self.engine_client.clone();
            let timer_handle  = tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(10));
                loop {
                    interval.tick().await;
                    engine_client.on_timer().await;
                }
            });
            self.timer_handle = Some(timer_handle);
            self.set_state(State::Started);
        } else if self.state == State::Paused {
            self.db.set_enabled(true);
            self.set_state(State::Started);
        }
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


    pub async fn status(&self) ->  IOResult<Status>  { self.engine_client.status().await }

    pub async fn stats(&self) -> IOResult<Stats> {
        self.engine_client.stats().await
    }

    pub async fn diags(&self,) -> IOResult<HashMap<String, HashMap<EndpointDiag, u32>>> {
        self.engine_client.diags().await
    }

    pub async fn reset_stats(&self,) -> IOResult<()> {
        self.engine_client.reset_stats().await
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


    pub fn close(&mut self) -> IOResult<()> {
        self.set_state(State::Closed);
        Ok(())
    }
}
impl Drop for App {
    fn drop(&mut self) {

    }
}