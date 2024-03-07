use core::time;
use std::{any::Any, collections::HashMap, fmt::format, hash::Hash, marker::PhantomData, sync::{mpsc::{channel, Receiver, Sender}, Arc, Mutex}, thread::{self, JoinHandle}};

use orm_lib::serializer::util::{key_types, TableDeserialize, TableSerialize} ;
use orm_lib::db_client::client::DbConn;
use orm_lib::orm::orm::ORM;
use tokio_postgres::{Client, NoTls, Row, connect};
use std::thread::spawn;


#[derive(Clone, Debug)]
pub struct Person { 
    id: usize, 
    name : String
}

impl TableSerialize for Person  {

    fn prim_key_name() -> String {
        "id".to_string()
    }

    fn table_name() -> String { 
        "mouser".to_string()
    }
    fn primary_key(&self) -> key_types {
        key_types::U32(self.id as u32)
    }

    fn fields_names() -> Vec<String> {
        let mut names = Vec::new();
        names.push("id".to_string());
        names.push("name".to_string());
        names
    }

    fn field_types() -> Vec<String> {
        let mut types = Vec::new();
        types.push("INT PRIMARY KEY".to_string());
        types.push("TEXT".to_string());
        types
    }

    fn into_map(&self) -> std::collections::HashMap<String, key_types> {
        let mut map = HashMap::new();
        map.insert("id".to_string(), key_types::U32(self.id as u32));
        map.insert("name".to_string(), key_types::String(self.name.clone()));
        map
    }

}

impl TableDeserialize for Person {
    fn from_map(map : HashMap<String,key_types>) -> Option<Self> {
        let id = map.get("id").unwrap().clone();
        let name = map.get("name").unwrap().clone();
        if let (key_types::U32(actual_id), key_types::String(actual_name)) = (id, name) {
            let person = Person {
                id: actual_id as usize,
                name: actual_name.clone(),
            };
            Some(person)
        } else {
            None
        }
    }
}




#[tokio::main]
async fn main() {
    let person = Person { id : 2, name: "neha".to_string()};
    //println!("Hello, world!");
    let conn_str = "postgresql://raja:hello@localhost:5432/testdb";
    //println!("{}", db_conn.connection);
    let stmt = "CREATE TABLE Person(id INT PRIMARY KEY, name VARCHAR(10));";
    
    // let result = db_conn.insert(person);
    // match result {
    //     Ok(val) => println!("{}", val),
    //     Err(err) => println!("{}", err)
    // }
    // let val = db_conn.find_by_prim_key(key_types::U32(1)).unwrap();
    // println!("value : {:#?}", val);
    // let all = db_conn.find_all();
    // println!("{:#?}", all);
    
    let mut orm = ORM::<Person>::new(conn_str.to_string()).await;
    let all = orm.find_all().await;
    let orm_arc = Arc::new(Mutex::new(orm));

    let orm_arc_clone = Arc::clone(&orm_arc);
    println!("{:#?}", all);
    findAll_blocking(orm_arc).await;
    findAll_non_blocking(orm_arc_clone).await;
}

async fn findAll_blocking(mut orm : Arc<Mutex<ORM<Person>>>) { 
    
    thread::sleep(time::Duration::from_secs(3));
    let all = orm.lock().unwrap().find_all().await;
    println!("first : {:#?}", all);
}




async fn findAll_non_blocking(mut orm : Arc<Mutex<ORM<Person>>>) { 
    let all = orm.lock().unwrap().find_all().await;
    //thread::sleep(time::Duration::from_secs(3));
    println!("second : {:#?}", all);
}
