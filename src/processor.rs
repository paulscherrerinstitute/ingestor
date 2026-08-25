use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use bsread::{Bsread, EndpointEvent, Message, Pool};
use tokio::runtime::Handle;
use crate::Arguments;
use std::sync::{Mutex, RwLock};

pub struct Processor {
    last_ids:Arc<Mutex<HashMap<String, i32>>>
}

impl Processor {

    pub fn new() -> Self {
        Self {
            last_ids:Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn process(&self, endpoint:Option<String>, message:Message){
        let id = message.id() as i32;
        //if id % 2 ==0{
        //    thread::sleep(Duration::from_millis(50));
       // }
        let endpoint = endpoint.unwrap_or(String::from(""));
        let mut last_ids = self.last_ids.lock().unwrap();

        //println!("{:?} {:?}, {}", thread::current().id(), endpoint, id);
        let last_id = last_ids.get(endpoint.as_str()).unwrap_or(&(-1)).clone();
        if last_id > 0  && last_id >= id {
            println!("Received unordered message for {:?} last: {:?}, id: {}", endpoint, last_id, id);
        }
        last_ids.insert(endpoint.to_string(), id);

        //tokio::time::sleep(Duration::from_millis(50)).await;
        //thread::sleep(Duration::from_millis(50));
    }


}