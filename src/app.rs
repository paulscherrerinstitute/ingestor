use std::thread;
use std::time::Duration;
use serde::{Serialize, Deserialize};
use std::collections::{HashMap, HashSet};
use std::io::ErrorKind;
use std::sync::Arc;
use bsread::{Bsread, EndpointDiag, EndpointState, IOError, IOResult};
use bsread::message::DECOMPRESSION_ERROR;
use sysinfo::{Pid, ProcessesToUpdate, System};
use crate::{engine, Arguments};
use crate::Config;
use tokio::sync::mpsc::{channel, Sender};
use crate::engine::{Engine, EngineCommand};
use tokio::sync::oneshot;
use crate::processor::Processor;

#[derive(Serialize, PartialEq, Clone)]
pub enum State {
    Initializing,
    Stopped,
    Started,
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
    pub cpu:f32,
    pub memory:u64,
    pub files:usize,
}

pub struct App {
    arguments:Arguments,
    config:Config,
    engine_tx: Sender<EngineCommand>,
    state:State,
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
        let (engine_tx, mut engine_rx) = channel::<engine::EngineCommand>(32);
        let processor = Arc::new(Processor::new());
        Engine::launch(arguments.clone(), engine_rx, handle.clone(), processor);
        App {arguments, config, engine_tx, state:State::Initializing}
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
        match self.send_config().await{
            Ok(_) => {
                Ok(())
            }
            Err(e) => {
                Err(IOError::new(ErrorKind::Other, e))
            }
        }
    }

    pub async fn stop(& mut self) -> IOResult<()> {
        log::info!("Stpping service");
        match self.disconnect().await{
            Ok(_) => {
                self.state = State::Stopped;
                Ok(())
            }
            Err(e) => {
                Err(IOError::new(ErrorKind::ConnectionAborted, e))
            }
        }
    }


    pub async fn start(&mut self) -> IOResult<()> {
        if (!self.is_started()){
            log::info!("Starting service");
            if let Err(e) =  self.send_config().await {
                return Err(IOError::new(ErrorKind::Other, e));
            }

            match self.connect().await{
                Ok(_) => {
                    self.state = State::Started;
                }
                Err(e) => {
                    self.state = State::Error;
                    return Err(IOError::new(ErrorKind::ConnectionAborted, e));
                }
            }
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

    async fn send_command<T>(&self,command: impl FnOnce(oneshot::Sender<IOResult<T>>) -> EngineCommand,) -> IOResult<T> {
        let (tx, rx) = oneshot::channel();
        self.engine_tx.send(command(tx)).await
            .map_err(|_| {IOError::new(ErrorKind::BrokenPipe,"Engine is not running",)})?;
        rx.await.
            map_err(|_| {IOError::new(ErrorKind::BrokenPipe,"Engine terminated",)})?
    }

    async fn connect(&mut self) -> IOResult<()> {
        self.send_command(|response| EngineCommand::Start { response }).await
    }

    async fn disconnect(&self) -> IOResult<()> {
        self.send_command(|response| EngineCommand::Stop { response }).await
    }

    async fn send_config(&self) -> IOResult<()> {
        let config = self.config.clone();
        self.send_command(|response| {EngineCommand::Config { config, response }}).await
    }

    pub async fn status(&self) ->  IOResult<Status> {
        let endpoints = self.send_command(|response| EngineCommand::Status { response })
            .await?;
        Ok(Status {state: self.state(),endpoints,})
    }

    pub async fn stats(&self) -> IOResult<Stats> {
        self.send_command(|response| EngineCommand::Stats { response }).await
    }

    pub async fn diags(&self,) -> IOResult<HashMap<String, HashMap<EndpointDiag, u32>>> {
        self.send_command(|response| EngineCommand::Diags { response }).await
    }

    pub async fn reset_stats(&self) -> IOResult<()> {
        self.send_command(|response| EngineCommand::ResetStats { response }).await
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