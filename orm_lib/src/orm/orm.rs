use std::marker::PhantomData;

use crate::db_client::client::DbConn;

use crate::serializer::util::*;



pub struct ORM<T> 
{ 
    pub index : usize,
    pub conn_pool : Vec<DbConn<T>>,
    marker : PhantomData<T>,
}


// pub fn next_conn<T>(list : Vec<DbConn<T>>, mut index : usize) -> (DbConn<T>, usize)  
// where T : TableSerialize + TableDeserialize
// { 
    
//     if index == list.len() - 1 { 
//         index == 0;
//     } else { 
//         index += 1;
//     }
  
//     let conn = requie
//     (conn, index)
// }


impl<T: TableSerialize + TableDeserialize>  ORM<T> {
    pub async fn new(conn_str : String) -> Self { 
        let mut pool = Vec::new();
        for i in 0..10 { 
            let db_conn = DbConn::<T>::new(conn_str.as_str())
            .await
            .map_err(|err| err).unwrap();
            pool.push(db_conn);
        }
        
        let orm = ORM { index : 0, conn_pool: pool, marker: PhantomData };
        orm
    }

    pub fn next_conn(&mut self) -> DbConn<T>{ 
        if self.index == self.conn_pool.len() - 1 { 
            self.index == 0;
        } else { 
            self.index += 1;
        }
        let mut conn = self.conn_pool.get(self.index).unwrap().clone();
        conn
    }

    pub async fn find_all(&mut self) -> Vec<T> { 
        return self.next_conn().find_all().await;
    }

    pub async fn find_by_primary_key(&mut self, key_val : key_types) -> Option<T>{ 
        return self.next_conn().find_by_prim_key(key_val).await;
    }
    pub async fn insert(&mut self, t:T) -> Result<u64, String> { 
        self.next_conn().insert(t).await
    }

    pub async fn migrate_and_start(&mut self) -> Result<u64, String> { 
        self.next_conn().migrate().await
    }
}
