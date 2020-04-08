//! Choose one way to serialize and deserialize your data.
//!
//! Now there are two ways available:
//! - json
//! - bincode
//!
//! # Examples
//!
//! Add `SerdeInterface` to your stuct as a `PhantomData`.
//! ```no_run
//! struct Foo<S> {
//!     foo: String,
//!     serde: PhantomData<S>
//! }
//!
//! impl<S: SerdeInterface> Foo<S> {
//!     fn store<W: Write>(writer: &mut W) {
//!         S::to_writer(writer, &self.foo).unwrap()
//!     }
//! }
//! ```

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::de;
use std::io::{Read, Write};

use anyhow::Result;

/// Uniform interface for serde::Serializer and serde::Deserializer implementations
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

/// Json interface. It doesn't check if there are trailing characters when deserializing from stream.
///
/// This Sturct has no fields, you can use it as PhantomData
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
        // we don't check if we have consumed the whole stream
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

/// Bincode interface.
///
/// This Sturct has no fields, you can use it as PhantomData
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
