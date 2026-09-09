use crate::Arguments;
use crate::DB;
use std::sync::Arc;


pub struct Ingestor {
    arguments: Arc<Arguments>,
    db: Arc<DB>,
}


impl Ingestor {
    pub fn new(arguments:Arc<Arguments>, db:Arc<DB>) -> Self {
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