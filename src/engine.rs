use std::io::ErrorKind;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use bsread::{Bsread, ConnectionMode, EndpointDiag, EndpointEvent, EndpointState, IOError, IOResult, Message, Pool, ReceivedMessage, SocketType};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::Thread;
use crate::api::AppError;
use crate::processor::Processor;
use crate::Arguments;
use crate::Config;
use tokio::sync::mpsc::Receiver;
use crossbeam_channel;
use tokio::runtime::{Runtime, Handle};

pub enum EngineCommand {
    Start {
        response: tokio::sync::oneshot::Sender<IOResult<()>>,
    },
    Stop {
        response: tokio::sync::oneshot::Sender<IOResult<()>>,
    },
    Config {
        config: Config,
        response: tokio::sync::oneshot::Sender<IOResult<()>>,
    },
    Diags {
        response: tokio::sync::oneshot::Sender<IOResult<(HashMap<String, HashMap<EndpointDiag, u32>>)>>,
    },
    Status {
        response: tokio::sync::oneshot::Sender<IOResult<(HashMap<String, EndpointState>)>>,
    },
}

pub struct Engine {
    arguments:Arguments,
    contexts: Vec<Arc<Bsread>>,
    pools: Vec<Pool>,
    event_receivers: Vec<crossbeam_channel::Receiver<EndpointEvent>>,
    endpoints:Vec<String>,
    handle: Handle,
    current: usize,
    connected:bool,
    processor: Arc<Processor>,
}

impl Engine {
    pub fn launch(arguments:Arguments, engine_rx:Receiver<EngineCommand>, handle:Handle, processor:Arc<Processor>) {
        let contexts = Vec::new();
        let pools = Vec::new();
        let endpoints = Vec::new();
        let event_receivers = Vec::new();

        //let handle =  Handle::current().clone();
        //Creating a dedicated runtime instead of ?
        let processor = processor.clone();
        let engine = Engine {arguments, contexts, event_receivers, endpoints, pools, handle, processor, current:0,connected: false};

        //Cannot be async, ZMQ calls must be called from the same thread.
        //tokio::spawn(async move {
        std::thread::spawn(move || {
            let mut engine = engine;
            let mut engine_rx = engine_rx;
            /*
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                //.thread_name("async_carrier")
                .thread_name_fn(|| {
                    static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
                    let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
                    format!("async_carrier-{}", id)
                })
                .enable_all()
                .build()
                .unwrap();
            engine.handle = runtime.handle().clone();
            */

            //while let Some(command) = engine_rx.recv().await {
            while let Some(command) = engine_rx.blocking_recv() {
                match command {
                    EngineCommand::Start { response } => {
                        let result = engine.start();
                        let _ = response.send(result);
                    }

                    EngineCommand::Stop { response } => {
                        let result = engine.stop();
                        let _ = response.send(result);
                    }

                    EngineCommand::Config { config, response } => {
                        engine.config(config);
                        let _ = response.send(Ok(()));
                    }

                    EngineCommand::Diags { response } => {
                        let _ = response.send(Ok(engine.diags()));
                    }


                    EngineCommand::Status { response } => {
                        let _ = response.send(Ok(engine.status()));
                    }
                }
            }
        });
    }


    fn stop(& mut self) -> IOResult<()> {
        //for (context, pool) in self.contexts.iter().zip(self.pools.iter()) {
        for mut pool in self.pools.iter_mut() {
            pool.disconnect();
            pool.stop_async();
        }
        for mut context in self.contexts.iter_mut() {
        }
        self.pools = Vec::new();
        self.contexts = Vec::new();
        self.event_receivers = Vec::new();
        self.current = 0;
        self.connected = false;
        Ok(())
    }

    fn start(& mut self) -> IOResult<()> {
        self.stop();
        self.add_pool();
        self.current = 0;
        self.connected = true;
        for endpoint in self.endpoints.clone() {
            self.try_connect_endpoint(&endpoint);
        }
        Ok(())
    }

    fn config(&mut self, config: Config) -> IOResult<()> {
        if self.endpoints.len()==0{
            //Initial config
            self.endpoints = config.endpoints;
            if self.connected {
                self.start()?;
            }
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
        }
        Ok(())
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
        let pool_size = self.pool_size();
        if self.endpoint_pool(endpoint).is_none(){
            let mut pool = self.current();
            if pool.connections() > pool_size {
                self.add_pool()?;
                pool = self.current();
            }
            pool.add_endpoint(endpoint, None)?;
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
        //let callback = |msg| async move {
        //}
        let processor = self.processor.clone();
        let callback = {
            let processor = processor.clone();
            move |msg: ReceivedMessage| {
                let processor = processor.clone();
                async move {
                    processor.process(msg.endpoint, msg.message).await;
                }
            }
        };

        let context =  Bsread::new()?;
        let mut pool = context.pool(vec![], SocketType::PULL, ConnectionMode::Individual, self.receivers() )?;
        let event_receiver = pool.enable_monitoring()?;
        //pool.connect()?;

        let handle = self.handle.clone();
        pool.start_async(callback, self.arguments.concurrent, Some(handle),)?;

        self.contexts.push(context);
        self.pools.push(pool);
        self.event_receivers.push(event_receiver);
        self.current = self.current + 1;
        Ok(())
    }

    fn pool_size(& self) -> usize {
        self.arguments.pool_size
    }

    fn receivers(& self) -> usize {
        self.arguments.receivers
    }

    fn pools(& self) -> &Vec<Pool> {
        &self.pools
    }

    fn diags(& self) -> HashMap<String, HashMap<EndpointDiag, u32>>{
        let mut diagnostics = HashMap::new();
        for pool in self.pools.iter() {
            diagnostics.extend(pool.diagnostics());
        }
        diagnostics
    }

    fn status(& self) -> HashMap<String, EndpointState> {
        let mut endpoint_states = HashMap::new();
        for pool in self.pools.iter() {
            endpoint_states.extend(pool.endpoint_states());
        }
        endpoint_states
    }

}
impl Drop for Engine {
    fn drop(&mut self) {
        self.stop();
    }
}