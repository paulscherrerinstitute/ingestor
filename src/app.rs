use std::thread;
use std::time::Duration;
use serde::{Serialize, Deserialize};
use std::collections::HashSet;
use std::io::ErrorKind;
use std::sync::Arc;
use bsread::{Bsread, IOError, IOResult};
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
}


pub struct App {
    arguments:Arguments,
    config:Config,
    engine_tx: Sender<EngineCommand>,
    state:State,
}

impl App {
    pub fn new(arguments:Arguments) -> Self{
        let config = Config{endpoints:Vec::new()};
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
        match self.connect().await{
            Ok(_) => {
                self.state = State::Started;
                Ok(())
            }
            Err(e) => {
                self.state = State::Error;
                Err(IOError::new(ErrorKind::ConnectionAborted, e))
            }
        }

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

    pub fn status(&self) -> Status {
        Status {
            state: self.state()
        }
    }

    pub fn config(&self) -> Config {
        self.config.clone()
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