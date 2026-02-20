use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Record {
    pub timestamp: i64,
    pub payload: Vec<u8>,
}

impl Record {
    pub fn new(timestamp: i64, payload: Vec<u8>) -> Self {
        Self { timestamp, payload }
    }
}
