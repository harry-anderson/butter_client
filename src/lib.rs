use std::collections::BTreeMap;

use serde::{de, Deserialize, Deserializer, Serialize};
use serde_json::{from_value, Value};
use simd_json;
use tracing::trace;

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct BinanceSnapshot {
    #[serde(rename = "lastUpdateId")]
    pub last_update_id: i64,
    #[serde(rename = "bids")]
    pub bids: Vec<(String, String)>,
    #[serde(rename = "asks")]
    pub asks: Vec<(String, String)>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Order {
    pub price: f64,
    pub quant: f64,
}

pub fn parse_json(mut b: Vec<u8>) -> anyhow::Result<Value> {
    let v: Value = simd_json::serde::from_slice(&mut b)?;
    Ok(v)
}
