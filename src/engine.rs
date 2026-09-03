use std::io::ErrorKind;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use bsread::{Bsread, ConnectionMode, EndpointDiag, EndpointEvent, EndpointState, IOError, IOResult, Message, Pool, ReceivedMessage, SocketConfig, SocketType};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::thread::Thread;
use bsread::receiver::AsyncExecution;
use crate::app::{Stats, App, Status, Config, Source};
use crate::api::AppError;
use crate::processor::Processor;
use crate::Arguments;
use tokio::sync::mpsc::Receiver;
use crossbeam_channel;
use crossbeam_channel::RecvError;
use tokio::runtime::{Runtime, Handle};
use sysinfo::{Pid, ProcessesToUpdate, System};
use futures::future::join_all;
use std::time::Instant;


pub enum EngineCommand {
    Start {
        response: tokio::sync::oneshot::Sender<IOResult<()>>,
    },
    Stop {
        response: tokio::sync::oneshot::Sender<IOResult<()>>,
    },
    Timer {
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
        response: tokio::sync::oneshot::Sender<IOResult<(Status)>>,
    },
    Stats {
        response: tokio::sync::oneshot::Sender<IOResult<(Stats)>>,
    },
    ResetStats {
        response: tokio::sync::oneshot::Sender<IOResult<()>>,
    },
}

struct ProcessingStats {
    processing: AtomicU32,
    processed: AtomicU32,
    duplicated_sources: AtomicU32,
    disabled_sources: AtomicU32,
}

pub struct Engine {
    arguments:Arguments,
    contexts: Vec<Arc<Bsread>>,
    pools: Vec<Pool>,
    sources:Vec<Source>,
    handle: Handle,
    current: usize,
    connected:bool,
    processor: Arc<Processor>,
    processing_stats: Arc<ProcessingStats>,
    last_stats: Option<Stats>,
}

impl Engine {

    pub fn new(arguments:Arguments,  handle:Handle, processor:Arc<Processor>) -> Self{
        let contexts = Vec::new();
        let pools = Vec::new();
        let sources = Vec::new();
        Engine {arguments, contexts, sources, pools, handle, processor,
            current:0,
            connected: false,
            processing_stats: Arc::new(ProcessingStats{ processing: AtomicU32::new(0), processed: AtomicU32::new(0),
                duplicated_sources: AtomicU32::new(0), disabled_sources: AtomicU32::new(0)}),
            last_stats: None
        }
    }

    pub fn launch(arguments:Arguments, engine_rx:Receiver<EngineCommand>, handle:Handle, processor:Arc<Processor>) {
        let engine = Engine::new(arguments, handle, processor);
        //std::thread::spawn(move || {
        tokio::spawn(async move {
            let mut engine = engine;
            let mut engine_rx = engine_rx;
            while let Some(command) = engine_rx.recv().await {
            //while let Some(command) = engine_rx.blocking_recv() {
                match command {
                    EngineCommand::Start { response } => {
                        let result = engine.start();
                        let _ = response.send(result);
                    }

                    EngineCommand::Stop { response } => {
                        let result = engine.stop();
                        let _ = response.send(result);
                    }

                    EngineCommand::Timer { response } => {
                        engine.on_timer();
                        let _ = response.send(Ok(()));
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

                    EngineCommand::Stats { response } => {
                        let _ = response.send(Ok(engine.stats()));
                    }

                    EngineCommand::ResetStats { response } => {
                        engine.reset_stats();
                        let _ = response.send(Ok(()));
                    }

                }
            }
        });
    }


    pub fn stop(& mut self) -> IOResult<()> {
        let start = Instant::now();
        //for (context, pool) in self.contexts.iter().zip(self.pools.iter()) {
        for mut pool in self.pools.iter_mut() {
            pool.disconnect();
            pool.stop_async();
        }
        for mut context in self.contexts.iter_mut() {
        }
        self.pools = Vec::new();
        self.contexts = Vec::new();
        self.current = 0;
        self.connected = false;
        log::info!("Stopped in {:?}", start.elapsed());
        Ok(())
    }

    pub fn start(& mut self) -> IOResult<()> {
        let start = Instant::now();

        self.stop();
        self.add_pool();
        self.current = 0;
        self.connected = true;
        for source in self.sources.clone() {
            self.try_connect_endpoint(&source.address, &source.socket_type.map(Into::into));
        }

        //for i in 1000..1200{
        //    self.try_connect_endpoint(&format!("tcp://129.129.66.28:{}", i), None);
        //}
        //for i in 10..210{
        //    self.try_connect_endpoint(&format!("tcp://129.129.66.{}:1000", i), None);
        //}

        //let futures = self.endpoints.clone().into_iter()
        //    .map(|endpoint| self.try_connect_endpoint(&endpoint));
        //join_all(futures).await;
        log::info!("Started in {:?}", start.elapsed());
        Ok(())
    }

    pub fn config(&mut self, config: Config) -> IOResult<()> {
        self.processing_stats.duplicated_sources.store(0, Ordering::Relaxed);
        self.processing_stats.disabled_sources.store(0, Ordering::Relaxed);

        let mut sources = config.sources;

        sources.retain(|source| {
            if source.enabled == Some(false) {
                self.processing_stats.disabled_sources.fetch_add(1, Ordering::Relaxed);
                log::info!("Ignoring disabled source: {}", source.address);
                false
            } else {
                true
            }
        });

        let mut addresses = HashSet::new();
        sources.retain(|source| {
            if addresses.insert(source.address.clone()) {
                true
            } else {
                self.processing_stats.duplicated_sources.fetch_add(1, Ordering::Relaxed);
                log::warn!("Removing duplicate source with address: {}",source.address);
                false
            }
        });

        if self.sources.len()==0{
            //Initial config
            self.sources = sources;
            if self.connected {
                self.start()?;
            }
        } else {
            let start = Instant::now();
            //Incremental config
            let added: Vec<Source> = sources
                .iter()
                .filter(|new| {
                    !self.sources.iter().any(|old| {
                        old.address == new.address && old.socket_type == new.socket_type
                    })
                })
                .cloned()
                .collect();

            let removed: Vec<Source> = self.sources
                .iter()
                .filter(|old| {
                    !sources.iter().any(|new| {
                        new.address == old.address && new.socket_type == old.socket_type
                    })
                })
                .cloned()
                .collect();

            for source in removed {
                self.disconnect_endpoint(&source.address);
            }

            for source in added {
                self.try_connect_endpoint(&source.address, &source.socket_type.map(Into::into));
            }
            self.sources = sources;
            log::info!("Reconfigured in {:?}", start.elapsed());
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

    fn connect_endpoint(&mut self, endpoint: &String, socket_type:&Option<SocketType>) -> IOResult<()> {
        let pool_size = self.pool_size();
        if self.endpoint_pool(endpoint).is_none(){
            let mut pool = self.current();
            if pool.connections() > pool_size {
                self.add_pool()?;
                pool = self.current();
            }
            pool.add_endpoint(endpoint, socket_type.clone(), None)?;
        }
        Ok(())
    }

    fn try_connect_endpoint(&mut self, endpoint: &String, socket_type:&Option<SocketType>) {
        let start = Instant::now();

        if let Err(e) = self.connect_endpoint(&endpoint, &socket_type) {
            log::error!("Error connecting to endpoint {}: {:?}", endpoint, e);
        }

        log::debug!("Connecting to {} took {:?}", endpoint, start.elapsed());
    }

    fn current(& mut self) -> &mut Pool {
        &mut self.pools[self.current]
    }

    fn add_pool(&mut self) -> IOResult<()>  {
        let processor = Arc::clone(&self.processor);
        let processing_stats = Arc::clone(&self.processing_stats);

        let callback = move |msg: ReceivedMessage| {
            let processor = Arc::clone(&processor);
            let processing_stats = Arc::clone(&processing_stats);

            async move {
                processing_stats.processing.fetch_add(1, Ordering::Relaxed);
                processor.process(msg.endpoint, msg.message).await;
                processing_stats.processing.fetch_sub(1, Ordering::Relaxed);
                processing_stats.processed.fetch_add(1, Ordering::Relaxed);
            }
        };

        let context =  Bsread::new()?;
        let mut pool = context.pool(vec![], SocketType::PULL, ConnectionMode::Individual, self.receivers() )?;
        pool.set_raw(true);
        pool.set_blocking_config(self.arguments.blocking_config);
        if let Err(err) = self.set_zmq_options(&mut pool){
            log::error!("Error setting zmq options {}", err);
        }

        let event_receiver = pool.enable_monitoring()?;
        //pool.connect()?;
        let handle = self.handle.clone();
        let processor = Arc::clone(&self.processor);

        thread::spawn(move || {
            loop {
                match event_receiver.recv() {
                    Ok(event) => {
                        let processor = processor.clone();
                        handle.spawn(async move {
                            match event {
                                EndpointEvent::State(endpoint, state) => {
                                    processor.on_endpoint_state(endpoint, state).await;
                                }
                                EndpointEvent::Diagnostic(endpoint, diag, id) => {
                                    processor.on_endpoint_diag(endpoint, diag, id).await;
                                }
                            }
                        });
                    }
                    Err(err) => {
                        log::error!("Error receiving monitor event, quitting receive thread: {:?}",err);
                        break;
                    }
                }
            }
        });


        let handle = self.handle.clone();
        let execution = if self.arguments.concurrent {
            AsyncExecution::Concurrent
        } else {
            AsyncExecution::Ordered { capacity: self.arguments.buffer_size, blocking: false }
        };
        pool.start_async(callback, execution, Some(handle),)?;

        self.contexts.push(context);
        self.pools.push(pool);
        self.current = self.current + 1;
        Ok(())
    }

    fn set_zmq_options(&self, pool: &mut Pool) -> IOResult<()> {
        pool.set_rcvhwm(self.arguments.receive_hwm)?;
        pool.set_linger(0)?;
        pool.set_keepalive(30, 10, 3)?;
        pool.set_heartbeat(10_000, 30_000, 30_000)?;
        if self.arguments.disable_handshake {
            pool.set_handshake_ivl(0);
        }
        Ok(())
    }

    pub fn pool_size(& self) -> usize {
        self.arguments.pool_size
    }

    pub fn receivers(& self) -> usize {
        self.arguments.receivers
    }

    pub fn pools(& self) -> &Vec<Pool> {
        &self.pools
    }

    pub fn diags(& self) -> HashMap<String, HashMap<EndpointDiag, u32>>{
        let mut diagnostics = HashMap::new();
        for pool in self.pools.iter() {
            diagnostics.extend(pool.diagnostics());
        }
        diagnostics
    }

    pub fn status(& self) -> Status {
        let mut endpoint_states = HashMap::new();
        for pool in self.pools.iter() {
            endpoint_states.extend(pool.endpoint_states());
        }
        Status::new(endpoint_states)
    }

    pub fn stats(& self) -> Stats {
        let (cpu, memory, files) = App::process_resources();

        Stats {
            received: self. messages(),
            errors:  self.errors(),
            dropped:  self.dropped(),
            processing:self.processing(),
            processed: self.processed(),
            duplicated_sources: self.processing_stats.duplicated_sources.load(Ordering::Relaxed),
            disabled_sources: self.processing_stats.disabled_sources.load(Ordering::Relaxed),
            received_rate: self.last_stats.as_ref().map_or(0.0, |stats| stats.received_rate),
            errors_rate: self.last_stats.as_ref().map_or(0.0, |stats| stats.errors_rate),
            dropped_rate: self.last_stats.as_ref().map_or(0.0, |stats| stats.dropped_rate),
            processed_rate: self.last_stats.as_ref().map_or(0.0, |stats| stats.processed_rate),
            cpu, memory, files,
        }
    }

    pub fn reset_stats(&mut self) {
        for mut pool in self.pools.iter_mut() {
            pool.reset_counters();
        }
        self.processing_stats.processed.store(0, Ordering::Relaxed);
        self.processing_stats.processing.store(0, Ordering::Relaxed);
    }

    pub fn messages(&self) -> u32 {
        self.pools
            .iter()
            .map(|r| r.messages())
            .sum()
    }

    pub fn errors(&self) -> u32 {
        self.pools
            .iter()
            .map(|r| r.errors())
            .sum()
    }

    pub fn dropped(&self) -> u32 {
        self.pools
            .iter()
            .map(|r| r.dropped())
            .sum()
    }

    pub fn processing(&self) -> u32 {
        self.processing_stats.processing.load(Ordering::Relaxed)
    }

    pub fn processed(&self) -> u32 {
        self.processing_stats.processed.load(Ordering::Relaxed)
    }

    //Every 10s
    pub fn on_timer(&mut self)  {
        let received = self. messages();
        let errors =  self.errors();
        let dropped =  self.dropped();
        let processing =self.processing();
        let processed = self.processed();

        let (received_rate, errors_rate, dropped_rate, processed_rate) = if let Some(last_stats) = self.last_stats.as_ref() {
            let new_received = if received < last_stats.received{received} else {received - last_stats.received};
            let new_errors = if errors < last_stats.errors{errors} else {errors - last_stats.errors};
            let new_dropped = if dropped < last_stats.dropped{dropped} else {dropped - last_stats.dropped};
            let new_processed = if processed < last_stats.processed{processed} else {processed - last_stats.processed};
            ((new_received as f32) / 10.0, (new_errors as f32) / 10.0, (new_dropped as f32) / 10.0, (new_processed as f32) / 10.0)
        } else {
            (0.0, 0.0, 0.0, 0.0)
        };
        self.last_stats = Some(Stats{
            received,errors, dropped, processing, processed,
            received_rate, errors_rate, dropped_rate, processed_rate,
            duplicated_sources:0, disabled_sources:0,
            cpu:0.0, memory:0, files:0
        });
    }
}
impl Drop for Engine {
    fn drop(&mut self) {
        self.stop();
    }
}