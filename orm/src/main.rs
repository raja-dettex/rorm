use std::{collections::HashMap, fmt::format, hash::Hash, marker::PhantomData};

use orm_lib::{key_types, TableDeserialize, TableSerialize} ;
use postgres::{Client, NoTls, Row};


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
    fn primary_key(&self) -> orm_lib::key_types {
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



pub struct DbConn<T> { 
    connection : Client,
    marker: PhantomData<T>
}

impl<T : TableSerialize + TableDeserialize>  DbConn<T> { 
    pub fn new(conn_str : &str) -> Result<DbConn<T>, String> 
    where T: TableSerialize + TableDeserialize
    {
        let connection = Client::connect(conn_str, NoTls);
        
        match connection {
            Ok(conn) => Ok(DbConn{connection : conn, marker: PhantomData}),
            Err(err) => Err(err.to_string())
        }
    } 


    pub fn migrate(&mut self) -> Result<u64, String> 
    {
        let fields = T::fields_names();
        let types = T::field_types();
        let mut stmt = String::new();
        stmt.push_str(format!("CREATE TABLE {}(", T::table_name()).as_str());
        for (i, value) in fields.iter().enumerate() { 
            if i <  fields.len() - 1 { 
                let str = format!("{} {}," , value, types.get(i).unwrap());    
                stmt.push_str(str.as_str());
            }
            else { let str = format!("{} {}" , value, types.get(i).unwrap());
                stmt.push_str(str.as_str());
            }
        }
        stmt.push_str(");");
        println!("stmt : {}" , stmt);
        // let serialized = 
        // let a = self.connection.execute(statement, &[])
        //         .map_err(|err| err.to_string());
        let res = self.connection.execute(&stmt, &[]).map_err(|err| err.to_string());
        res
    }

    pub fn insert(&mut self, t : T) -> Result<u64, String>
    {
        let t_map = t.into_map();
        let mut stmt = format!("INSERT INTO {} VALUES (", T::table_name());

    let mut i = 0;
    for (key, val) in t_map.clone() {
        let formatted_value = match val {
            key_types::U32(value) => value.to_string(),
            key_types::String(value) => format!("'{}'", value),
        };

        stmt.push_str(&formatted_value);

        if i < t_map.len() - 1 {
            stmt.push_str(", ");
        }

        i += 1;
    }

    stmt.push_str(");");
    println!("{}", stmt);

        let res = self.connection.execute(&stmt, &[]).map_err(|err| err.to_string());
        res
    }

    pub fn find_by_prim_key(&mut self, key_val: key_types)  -> Option<T>
    { 
        
        let key = match key_val { 
            key_types::U32(k) => k.to_string(),
            key_types::String(str) => format!("'{}'", str)
        };
        let stmt = format!("SELECT * FROM {} WHERE {}={};", T::table_name(), T::prim_key_name(), key);
        println!("{}", stmt);
        let result = self.connection.query_one(&stmt, &[]).map_err(|err| err.to_string())
            .unwrap();
        let map = row_to_map(result);
        let person = T::from_map(map);
        person
    }

    pub fn find_all(&mut self) -> Vec<T>{
        let query = format!("SELECT * FROM {};", T::table_name());
        let result = self.connection.query(&query, &[])
            .map_err(|err|err.to_string()).unwrap();
        let mut items = Vec::new();
        for row in result { 
            let mut map = row_to_map(row);
            let item = T::from_map(map).unwrap();
            items.push(item);
        }  
        items
    }

    

}

fn row_to_map(row: Row) -> HashMap<String, key_types> {
    let mut map = HashMap::new();

    for column in row.columns() {
        let column_name = column.name();
        let column_value: key_types = match column.type_().name() {
            
            "int4" => key_types::U32(row.get::<_,i32>(column_name) as u32),
            "text" => key_types::String(row.get::<_, String>(column_name)),
            // Add more type mappings as needed based on your database schema
            _ => unimplemented!("Unsupported column type"),
        };

        map.insert(column_name.to_string(), column_value);
    }

    map
}


pub struct ORM<T> { 
    pub conn_pool : Vec<DbConn<T>>,
    marker : PhantomData<T>
}

impl<T: TableSerialize + TableDeserialize>  ORM<T> {
    pub fn new(conn_str : String) -> Self { 
        let mut pool = Vec::new();
        for i in 0..10 { 
            let db_conn = DbConn::<T>::new(conn_str.as_str())
            .map_err(|err| err).unwrap();
            pool.push(db_conn);
        }
        ORM { conn_pool: pool, marker: PhantomData }
        
    }
}


fn main() {
    let person = Person { id : 2, name: "neha".to_string()};
    //println!("Hello, world!");
    let fields = Person::fields_names();
    let types = Person::field_types();
    println!("{:#?} \n {:#?}", fields, types);
    let person_map = person.into_map();
    println!("{:#?}",person_map );
    let retrieved = Person::from_map(person_map);
    println!("{:#?}", retrieved.unwrap());
    let conn_str = "postgresql://raja:hello@localhost:5432/testdb";
    let mut db_conn = DbConn::<Person>::new(conn_str).map_err(|err| err.to_string()).unwrap();
    //println!("{}", db_conn.connection);
    let stmt = "CREATE TABLE Person(id INT PRIMARY KEY, name VARCHAR(10));";
    let res = db_conn.migrate().map_err(|err| err);
    
    // let result = db_conn.insert(person);
    // match result {
    //     Ok(val) => println!("{}", val),
    //     Err(err) => println!("{}", err)
    // }
    let val = db_conn.find_by_prim_key(key_types::U32(1)).unwrap();
    println!("value : {:#?}", val);
    let all = db_conn.find_all();
    println!("{:#?}", all);
    let orm = ORM::<Person>::new(conn_str.to_string());
}
