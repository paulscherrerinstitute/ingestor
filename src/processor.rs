use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use bsread::{Bsread, EndpointEvent, EndpointState, EndpointDiag, Message, Pool};
use tokio::runtime::Handle;
use crate::Arguments;
use std::sync::{Mutex, RwLock};
use std::sync::atomic::{AtomicU32, Ordering};

pub struct Processor {
    last_ids:Arc<Mutex<HashMap<String, i32>>>,
}

static RECEIVER_INDEX: AtomicU32 = AtomicU32::new(0);
fn index() -> u32{
    RECEIVER_INDEX.fetch_add(1, Ordering::Relaxed) + 1
}

impl Processor {
    pub fn new() -> Self {
        Self {
            last_ids: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn process(&self, endpoint: Option<String>, message: Message) {
        //Limit scope of mutex, so tokio::time::sleep, othewise future needs to be Future<Output = ()> + Send + 'static
        {
            let id = message.id() as i32;
            //if id % 2 ==0{
            //    thread::sleep(Duration::from_millis(50));
            // }
            let endpoint = endpoint.unwrap_or(String::from(""));
            let mut last_ids = self.last_ids.lock().unwrap();

            //println!("{:?} {:?}, {}", thread::current().id(), endpoint, id);
            let last_id = last_ids.get(endpoint.as_str()).unwrap_or(&(-1)).clone();
            if last_id > 0 && last_id >= id {
                println!("Received unordered message for {:?} last: {:?}, id: {}", endpoint, last_id, id);
            }
            last_ids.insert(endpoint.to_string(), id);
        }
        //tokio::time::sleep(Duration::from_millis(500)).await;
    }

    pub async fn on_endpoint_state(&self, endpoint: String, state: EndpointState) {
        println!("Endpoint {} state: {:?}", endpoint, state);
    }

    pub async fn on_endpoint_diag(&self, endpoint: String, diag: EndpointDiag, id: Option<u64>) {
        println!("Endpoint {} id {:?} diag: {:?}", endpoint, id, diag);
    }
}