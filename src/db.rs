use crate::channel_processor::ChannelProcessor;
use crate::ingestor::Ingestor;
use crate::{Arguments, app};
use app::State;
use bsread::{IOError, IOResult};
use clap::builder::Str;
use log::Record;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::errors::NewSessionError;
use serde::Serialize;
use std::io::ErrorKind;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::OnceCell;

pub struct DB {
    arguments:Arc<Arguments>,
    session: OnceCell<Session>,
}

impl DB {

    pub fn new(arguments:Arc<Arguments>) -> Self {
        Self { arguments, session:OnceCell::new() }
    }

    async fn create_session(arguments:&Arguments) -> IOResult<Session> {
        match SessionBuilder::new().known_node(&arguments.database).build().await {
            Ok(session) => {
                log::info!("Connected to database {}", &arguments.database);
                Ok(session)
            }
            Err(e) => {
                log::error!("Failed to connect to database {}: {}",  &arguments.database, e);
                Err(IOError::new(ErrorKind::ConnectionRefused, format!("Failed to build  session: {}", e)))
            }
        }
    }

    pub async fn connect(&self) -> IOResult<()> {
        //loop {
            if self.is_connected() {
                return Ok(());
            }
            match Self::create_session(&self.arguments).await {
                Ok(session) => {
                    self.session.set(session)
                        .map_err(|e| {IOError::new(ErrorKind::ConnectionRefused,format!("Error creating session: {}", e))})?;
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
}