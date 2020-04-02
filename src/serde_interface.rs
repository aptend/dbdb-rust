use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::de;
use std::io::{Read, Write};

use anyhow::Result;

pub trait SerdeInterface {
    fn from_reader<T, R>(reader: R) -> Result<T>
    where
        T: DeserializeOwned,
        R: Read;
    fn to_writer<T, W>(writer: W, value: &T) -> Result<()>
    where
        T: Serialize,
        W: Write;
}

pub struct SerdeJson;

impl SerdeInterface for SerdeJson {
    fn from_reader<T, R>(reader: R) -> Result<T>
    where
        T: DeserializeOwned,
        R: Read,
    {
        // get a deserializer from serde_json
        let mut der = de::Deserializer::new(de::IoRead::new(reader));
        let t: T = Deserialize::deserialize(&mut der)?;
        // we don't check if we have consumed the whole
        Ok(t)
    }
    fn to_writer<T, W>(writer: W, value: &T) -> Result<()>
    where
        T: Serialize,
        W: Write,
    {
        Ok(serde_json::to_writer(writer, value)?)
    }
}

pub struct SerdeBincode;

impl SerdeInterface for SerdeBincode {
    fn from_reader<T, R>(reader: R) -> Result<T>
    where
        T: DeserializeOwned,
        R: Read,
    {
        let t: T = bincode::deserialize_from(reader)?;
        Ok(t)
    }
    fn to_writer<T, W>(writer: W, value: &T) -> Result<()>
    where
        T: Serialize,
        W: Write,
    {
        Ok(bincode::serialize_into(writer, value)?)
    }
}
