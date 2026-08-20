use std::thread;
use std::time::Duration;
use serde::{Serialize, Deserialize};
use std::collections::{HashMap, HashSet};
use std::io::ErrorKind;
use std::sync::Arc;
use bsread::{Bsread, EndpointDiag, EndpointState, IOError, IOResult};
use bsread::message::DECOMPRESSION_ERROR;
use crate::{engine, Arguments};
use crate::Config;
use tokio::sync::mpsc::{channel, Sender};
use crate::engine::{Engine, EngineCommand};
use tokio::sync::oneshot;

#[derive(Serialize, PartialEq, Clone)]
pub enum State {
    Initializing,
    Stopped,
    Started,
    Error,
    Closed
}

#[derive(Serialize)]
pub struct Status {
    state: State,
    endpoints: HashMap<String, EndpointState>
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
        let (engine_tx, mut engine_rx) = channel::<engine::EngineCommand>(32);
        Engine::launch(arguments.clone(), engine_rx);
        App {arguments, config, engine_tx, state:State::Initializing}
    }

    async fn connect(&mut self) -> IOResult<()> {
        let (tx, rx) = oneshot::channel();
        self.engine_tx
            .send(EngineCommand::Connect { response: tx })
            .await
            .map_err(|_| {IOError::new(std::io::ErrorKind::BrokenPipe,"Engine is not running",)})?;
        rx.await
            .map_err(|_| {IOError::new(std::io::ErrorKind::BrokenPipe,"Engine terminated",)})?;
        Ok(())
    }

    async fn disconnect(&self) -> IOResult<()> {
        let (tx, rx) = oneshot::channel();
        self.engine_tx
            .send(EngineCommand::Disconnect { response: tx })
            .await
            .map_err(|_| {IOError::new(std::io::ErrorKind::BrokenPipe,"Engine is not running",)})?;
        rx.await
            .map_err(|_| {IOError::new(std::io::ErrorKind::BrokenPipe,"Engine terminated",)})?;
        Ok(())
    }

    async fn send_config(&self) -> IOResult<()> {
        let (tx, rx) = oneshot::channel();
        self.engine_tx
            .send(EngineCommand::Config {config: self.config.clone(),response: tx,})
            .await
            .map_err(|_| {IOError::new(std::io::ErrorKind::BrokenPipe,"Engine is not running",)})?;
        rx.await
            .map_err(|_| {IOError::new(std::io::ErrorKind::BrokenPipe,"Engine terminated",)})?;
        Ok(())
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

    pub async fn status(&self) ->  IOResult<Status> {
        let (tx, rx) = oneshot::channel();
        self.engine_tx
            .send(EngineCommand::Status { response: tx })
            .await
            .map_err(|_| {IOError::new(std::io::ErrorKind::BrokenPipe,"Engine is not running",)})?;
        let endpoints =  match rx.await
            .map_err(|_| {IOError::new(std::io::ErrorKind::BrokenPipe,"Engine terminated",)}) {
            Ok(endpoints) => endpoints?,
            Err(_) => {HashMap::new()}
        };
        Ok(Status {state: self.state(),endpoints})
    }

    pub async fn  diags(& self) -> IOResult<HashMap<String, HashMap<EndpointDiag, u32>>>{
        let (tx, rx) = oneshot::channel();
        self.engine_tx
            .send(EngineCommand::Diags { response: tx })
            .await
            .map_err(|_| {IOError::new(std::io::ErrorKind::BrokenPipe,"Engine is not running",)})?;
        rx.await
            .map_err(|_| {IOError::new(std::io::ErrorKind::BrokenPipe,"Engine terminated",)})?
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