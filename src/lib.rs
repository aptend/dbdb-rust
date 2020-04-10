#![allow(dead_code)]
//! This project is inspired by [500lines/dbdb](https://www.aosabook.org/en/500L/dbdb-dog-bed-database.html)
//!
//! DBDB (Dog Bed Database) is a library that implements a simple key/value database. It lets you associate a key with a value, and store that association on disk for later retrieval.
//!
//! DBDB aims to preserve data in the face of computer crashes and error conditions. It also avoids holding all data in RAM at once so you can store more data than you have RAM.
//!

pub mod logical_tree;
pub mod serde_interface;
pub mod storage;
