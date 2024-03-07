use std::{collections::HashMap, marker::PhantomData, sync::{Arc, Mutex}};
use crate::serializer::util::{key_types, TableDeserialize, TableSerialize};
use tokio_postgres::{ Client, NoTls, Row};


pub struct DbConn<T> 
{ 
    pub client : Arc<Mutex<Client>>,
    marker: PhantomData<T>
}



impl<T> Clone for DbConn<T>
where
    T: TableSerialize + TableDeserialize,
{
    fn clone(&self) -> Self {
        // Implement the cloning logic here
        // You may need to clone fields of DbConn<T> as well
        DbConn {
            // Clone fields here
            client: self.client.clone(),
            marker: PhantomData,
        }
    }
}

impl<T : TableSerialize + TableDeserialize>  DbConn<T> { 


    pub async fn new(conn_str : &str) -> Result<DbConn<T>, String> 
    {
        let (client , connection) = tokio_postgres::connect(conn_str, NoTls)
        .await.expect("failed to connec");
        
        tokio::spawn(async move  { 
            if let Err(e) = connection.await {
                println!("error {}", e.to_string());
            }
         });
        let dbConn = DbConn { client : Arc::new(Mutex::new(client)), marker: PhantomData};
        Ok(dbConn)
    } 


    pub async fn migrate(&mut self) -> Result<u64, String> 
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
        let res = self.client.lock().unwrap().execute(&stmt, &[]).await.map_err(|err| err.to_string());
        res
    }

    pub async fn insert(&mut self, t : T) -> Result<u64, String>
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

        let res = self.client.lock().unwrap().execute(&stmt, &[]).await.map_err(|err| err.to_string());
        res
    }

    pub async fn find_by_prim_key(&mut self, key_val: key_types)  -> Option<T>
    { 
        
        let key = match key_val { 
            key_types::U32(k) => k.to_string(),
            key_types::String(str) => format!("'{}'", str)
        };
        let stmt = format!("SELECT * FROM {} WHERE {}={};", T::table_name(), T::prim_key_name(), key);
        println!("{}", stmt);
        let result = self.client.lock().unwrap().query_one(&stmt, &[]).await.map_err(|err| err.to_string())
            .unwrap();
        let map = row_to_map(result);
        let person = T::from_map(map);
        person
    }

    pub async fn find_all(&mut self) -> Vec<T>{
        let query = format!("SELECT * FROM {};", T::table_name());
        let result = self.client.lock().unwrap().query(&query, &[])
            .await
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

