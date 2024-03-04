use std::{collections::HashMap, hash::Hash};



#[derive(Debug, Clone)]
pub enum key_types { 
    String(String),
    U32(u32),
}

pub trait TableSerialize { 
    fn prim_key_name() -> String;
    fn table_name() -> String;
    fn primary_key(&self) -> key_types;
    fn fields_names() -> Vec<String>;
    fn field_types() -> Vec<String>;
    fn into_map(&self) -> HashMap<String, key_types>;
}

pub trait TableDeserialize : Sized{ 
    fn from_map(map : HashMap<String,key_types>) -> Option<Self>;
}