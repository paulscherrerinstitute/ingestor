use std::thread;
use std::time::Duration;
use serde::{Serialize, Deserialize};
use std::collections::{HashMap, HashSet};
use std::io::ErrorKind;
use std::sync::Arc;
use bsread::{Bsread, EndpointDiag, EndpointState, IOError, IOResult};
use bsread::message::DECOMPRESSION_ERROR;
use sysinfo::{Pid, ProcessesToUpdate, System};
use tokio::runtime::Handle;
use crate::{engine, Arguments};
use crate::Config;
use tokio::sync::mpsc::{channel, Sender};
use crate::engine::{Engine, EngineCommand};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use crate::engine_client::EngineClient;
use crate::processor::Processor;

#[derive(Serialize, PartialEq, Clone)]
pub enum State {
    Starting,
    Started,
    Stopping,
    Stopped,
    Error,
    Closed,
}

#[derive(Serialize)]
pub struct Status {
    state: State,
    endpoints: HashMap<String, EndpointState>
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
    pub cpu:f32,
    pub memory:u64,
    pub files:usize,
}

pub struct App {
    arguments:Arguments,
    config:Config,
    state:State,
    timer_handle: Option<JoinHandle<()>>,
    engine_client: EngineClient,
}


impl App {
    pub fn new(arguments:Arguments) -> Self {
        let mut config = Config{endpoints:Vec::new()};
        if let Some(config_path) = arguments.config_path.clone(){
            match Config::load(&config_path){
                Ok(c) => {
                    config = c
                },
                Err(e) => {
                    log::error!("Error loading config from {}: {}", &config_path, e);
                }
            }
        }
        let handle = tokio::runtime::Handle::current();
        let (engine_client, engine_rx) = EngineClient::new();
        let processor = Arc::new(Processor::new(arguments.clone()));
        Engine::launch(arguments.clone(), engine_rx, handle.clone(), processor.clone());
        App {arguments, config, engine_client, state:State::Starting, timer_handle: None}
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
        if let Some(config_path) = self.arguments.config_path.clone(){
            if let Err(e) = self.config.save(config_path.as_str()) {
                    log::error!("Error saving config to {}: {}", &config_path, e);
            }
        }
        self.engine_client.send_config(self.config.clone()).await
    }

    pub async fn stop(& mut self) -> IOResult<()> {
        log::info!("Stpping service");
        self.state = State::Stopping;
        if let Some(handle) = self.timer_handle.take() {
            handle.abort();
            self.timer_handle = None;
        }
        self.engine_client.disconnect().await?;
        self.state = State::Stopped;
        Ok(())
    }


    pub async fn start(&mut self) -> IOResult<()> {
        if (!self.is_started()){
            log::info!("Starting service");
            self.state = State::Starting;
            self.engine_client.send_config(self.config.clone()).await.inspect_err(|e| {
                log::error!("Error sending config:g in application startup {:?}", e);
                self.state = State::Error;
            })?;
            self.engine_client.connect().await.inspect_err(|e| {
                log::error!("Error connecting in application startup: {:?}", e);
                self.state = State::Error;
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
            self.state = State::Started;
        }
        Ok(())
    }


    pub fn is_started(& self) -> bool {
        self.state == State::Started
    }

    pub fn wait(&self){
        loop {
            thread::sleep(Duration::from_millis(100));
        }
    }

    pub fn state(&self) -> State {
        self.state.clone()
    }
    pub fn arguments(&self) -> Arguments {
        self.arguments.clone()
    }

    pub fn config(&self) -> Config {
        self.config.clone()
    }


    pub async fn status(&self) ->  IOResult<Status> {
        let endpoints = self.engine_client.status().await?;
        Ok(Status {state: self.state(),endpoints,})
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

    pub fn close(&mut self) -> IOResult<()> {
        self.state = State::Closed;
        Ok(())
    }
}
impl Drop for App {
    fn drop(&mut self) {

    }
}