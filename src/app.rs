use std::io::ErrorKind;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use bsread::{Bsread, IOError, IOResult};
use serde::Serialize;
use crate::api::AppError;

#[derive(Clone)]
#[derive(Serialize)]
pub enum State {
    Initializing,
    Running,
    Error,
    Closing,
    Closed
}

#[derive(Serialize)]
pub struct Status {
    state: State,
    contexts: usize,
    pools: usize,
    receivers: usize,
    connections: usize,
}

#[derive(Clone)]
pub struct App {
    state:State,
    contexts: Vec<Arc<Bsread>>,
    debug:bool
}

impl App {
    pub fn new(debug:bool) -> Self{
        let contexts = Vec::new();
        App {state:State::Initializing, contexts, debug}
    }

    pub fn start(& mut self){
        self.state = State::Running;
    }

    pub fn wait(&self){
        loop {
            thread::sleep(Duration::from_millis(100));
        }
    }

    pub fn state(&self) -> State {
        self.state.clone()
    }

    pub fn status(&self) -> Status {
        Status {
            state: self.state(),
            contexts: self.contexts.iter().len(),
            pools:0,
            receivers: 0,
            connections: 0,
        }
    }

        pub fn close(&mut self) -> IOResult<()> {
        self.state = State::Closing;
        self.state = State::Closed;
        Ok(())
    }

}
impl Drop for App {
    fn drop(&mut self) {

    }
}