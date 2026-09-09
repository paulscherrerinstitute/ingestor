use std::sync::{Arc};
use scylla::client::session::Session;
use crate::{app, Arguments};
use crate::{db  , DB};


pub struct Ingestor {
    arguments:Arguments,
    db: Arc<DB>,
}


impl Ingestor {
    pub fn new(arguments:Arguments, db:Arc<DB>) -> Self {
        Self { arguments, db }
    }


    pub async fn create_table(&self, name:String, kind:String, shape:Option<Vec<u32>>, size:usize) {
        if let Some(session) = self.db.session() {
            println!("Create {}", name);
        }
    }

    pub async fn append_record(&self, name:String, data:Option<Vec<u8>>) {
        if let Some(session) = self.db.session() {

        }
    }
}