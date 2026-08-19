use std::io::ErrorKind;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use bsread::{Bsread, ConnectionMode, EndpointEvent, EndpointState, IOError, IOResult, Pool, SocketType};
use serde::Serialize;
use std::collections::HashSet;
use crate::api::AppError;
use crate::Arguments;
use crate::Config;
use tokio::sync::mpsc::Receiver;

pub enum EngineCommand {
    Connect {
        response: tokio::sync::oneshot::Sender<IOResult<()>>,
    },
    Disconnect {
        response: tokio::sync::oneshot::Sender<IOResult<()>>,
    },
    Config {
        config: Config,
        response: tokio::sync::oneshot::Sender<IOResult<()>>,
    },
}

pub struct Engine {
    arguments:Arguments,
    contexts: Vec<Arc<Bsread>>,
    endpoints:Vec<String>,
    pools: Vec<Pool>,
    current: usize,
}

impl Engine {
    pub fn launch(arguments:Arguments, engine_rx:Receiver<EngineCommand>) {
        let contexts = Vec::new();
        let pools = Vec::new();
        let endpoints = Vec::new();

        let engine = Engine {arguments, contexts, endpoints, pools, current:0,};

        tokio::spawn(async move {
            let mut engine = engine;
            let mut engine_rx = engine_rx;
            while let Some(command) = engine_rx.recv().await {
                match command {
                    EngineCommand::Connect { response } => {
                        let result = engine.connect();
                        let _ = response.send(result);
                    }

                    EngineCommand::Disconnect { response } => {
                        let result = engine.disconnect();
                        let _ = response.send(result);
                    }

                    EngineCommand::Config { config, response } => {
                        engine.endpoints = config.endpoints;
                        let _ = response.send(Ok(()));
                    }
                }
            }
        });
    }


    fn disconnect(& mut self) -> IOResult<()> {
        //for (context, pool) in self.contexts.iter().zip(self.pools.iter()) {
        for mut pool in self.pools.iter_mut() {
            pool.disconnect();
        }
        for mut context in self.contexts.iter_mut() {
        }
        self.pools = Vec::new();
        self.contexts = Vec::new();
        self.current = 0;
        Ok(())
    }

    fn connect(& mut self) -> IOResult<()> {
        self.disconnect();
        self.add_pool();
        self.current = 0;
        for endpoint in self.endpoints.clone() {
            self.try_connect_endpoint(&endpoint);
        }
        Ok(())
    }

    pub fn apply_config(&mut self, config: Config) -> IOResult<()> {
        if self.endpoints.len()==0{
            //Initial config
            self.endpoints = config.endpoints;
            self.connect()
        } else {
            //Incremental config
            //remove duplicates
            let mut endpoints = config.endpoints;
            let mut seen = HashSet::new();
            endpoints.retain(|x| seen.insert(x.to_string()));

            let added: Vec<String> = endpoints
                .iter()
                .filter(|e| !self.endpoints.contains(e))
                .cloned()
                .collect();

            let removed: Vec<String> = self.endpoints
                .iter()
                .filter(|e| !endpoints.contains(e))
                .cloned()
                .collect();

            for endpoint in removed {
                self.disconnect_endpoint(&endpoint);
            }

            for endpoint in added {
                self.try_connect_endpoint(&endpoint);
            }
            self.endpoints = endpoints;
            Ok(())
        }
    }


    fn endpoint_pool(&mut self, endpoint: &String) -> Option<&mut Pool>{
        for mut pool in self.pools.iter_mut() {
            if pool.has_endpoint(endpoint) {
                return Some(pool);
            }
        }
        None
    }

    fn disconnect_endpoint(&mut self, endpoint: &String) {
        match self.endpoint_pool(endpoint) {
            Some(pool) => {
                pool.remove_endpoint(endpoint);
            }
            None => {}
        }
    }

    fn connect_endpoint(&mut self, endpoint: &String) -> IOResult<()> {
        let capacity = self.capacity();
        if self.endpoint_pool(endpoint).is_none(){
            let mut pool = self.current();
            if pool.connections() > capacity {
                self.add_pool()?;
                pool = self.current();
            }
            pool.add_endpoint(endpoint, None)?
        }
        Ok(())
    }

    fn try_connect_endpoint(&mut self, endpoint: &String) {
        if let Err(e) = self.connect_endpoint(&endpoint) {
            log::error!("Error connecting to endpoint {}: {:?}", endpoint, e);
        }
    }

    fn current(& mut self) -> &mut Pool {
        &mut self.pools[self.current]
    }

    fn add_pool(&mut self) -> IOResult<()>  {
        let context =  Bsread::new()?;
        let mut pool = context.pool(vec![], SocketType::PULL, ConnectionMode::Individual, self.receivers() )?;
        self.contexts.push(context);
        self.pools.push(pool);
        self.current = self.current + 1;
        Ok(())
    }

    pub fn capacity(& self) -> usize {
        self.arguments.capacity
    }

    pub fn receivers(& self) -> usize {
        self.arguments.receivers
    }

    pub fn pools(& self) -> &Vec<Pool> {
        &self.pools
    }


}
impl Drop for Engine {
    fn drop(&mut self) {
        self.disconnect();
    }
}