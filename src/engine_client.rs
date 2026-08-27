use std::collections::HashMap;
use std::io::ErrorKind;
use bsread::{EndpointDiag, EndpointState, IOError, IOResult};
use serde::Serialize;
use tokio::sync::mpsc::{channel, Sender, Receiver};
use tokio::sync::oneshot;
use crate::app::{Stats, Status};
use crate::{engine, Config};
use crate::engine::EngineCommand;

#[derive(Clone)]
pub struct EngineClient {
    tx: Sender<EngineCommand>
}

impl EngineClient {
    pub fn new() -> (Self, Receiver<EngineCommand>) {
        let (tx, mut rx) = channel::<engine::EngineCommand>(32);
        (Self{tx}, rx)
    }

    async fn send_command<T>(&self,command: impl FnOnce(oneshot::Sender<IOResult<T>>) -> EngineCommand,) -> IOResult<T> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(command(tx)).await
            .map_err(|_| {IOError::new(ErrorKind::BrokenPipe,"Engine is not running",)})?;
        rx.await.
            map_err(|_| {IOError::new(ErrorKind::BrokenPipe,"Engine terminated",)})?
    }

    pub async fn connect(&mut self) -> IOResult<()> {
        self.send_command(|response| EngineCommand::Start { response }).await
    }

    pub async fn disconnect(&self) -> IOResult<()> {
        self.send_command(|response| EngineCommand::Stop { response }).await
    }

    pub async fn send_config(&self, config:Config) -> IOResult<()> {
        self.send_command(|response| {EngineCommand::Config { config, response }}).await
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

    pub async fn status(&self) ->  IOResult<HashMap<String, EndpointState>> {
        self.send_command(|response| EngineCommand::Status { response }).await
    }

    pub async fn on_timer(&self) -> IOResult<()> {
        self.send_command(|response| EngineCommand::Timer { response }).await
    }

}

