use std::{collections::BTreeMap, time::Duration};
use tokio_tungstenite::tungstenite::protocol::Message;

use futures_util::{pin_mut, SinkExt, StreamExt};
use serde::{de, Deserialize, Deserializer, Serialize};
use serde_json::{from_value, Value};
use simd_json;
use std::sync::Arc;
use tokio::{
    select,
    sync::{broadcast, RwLock},
    time::{sleep_until, Instant},
};
use tokio_tungstenite::{connect_async, tungstenite::Error};
use tracing::{debug, error};
use url::Url;

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

pub struct ConnectionManager {
    url: Url,
    retries: u64,
    backoff: u64,
    proxy_tx: broadcast::Sender<Message>,
}

impl ConnectionManager {
    pub fn new(remote_url: String, proxy_tx: broadcast::Sender<Message>) -> Self {
        let url = url::Url::parse(&remote_url).unwrap();
        Self {
            url,
            proxy_tx,
            retries: 30,
            backoff: 1,
        }
    }

    pub async fn connect(&mut self) {
        loop {
            let url = &self.url;
            if self.retries > 0 {
                debug!(
                    "connecting to {} in {}s retries left {}",
                    url, self.backoff, self.retries,
                );
                self.retries -= 1;
                self.backoff *= 2;

                sleep_until(Instant::now() + Duration::from_secs(self.backoff)).await;
                debug!("connecting...");
                match self.inner_connect().await {
                    Ok(_) => continue,
                    Err(err) => match err {
                        _ => {
                            error!("disconnection error {:?}", err);
                            continue;
                        }
                    },
                }
            }
        }
    }

    async fn inner_connect(&mut self) -> Result<(), Error> {
        let (ws_stream, _) = connect_async(self.url.clone()).await?;
        debug!("connect success {}", self.url);

        self.retries = 30;
        self.backoff = 1;

        let (ws_sender, ws_receiver) = ws_stream.split();
        let (_to_remote_tx, to_remote_rx) = tokio::sync::mpsc::unbounded_channel();
        pin_mut!(ws_sender, ws_receiver, to_remote_rx);

        loop {
            select! {
                msg = to_remote_rx.recv() => {
                    if let Some(msg) = msg {
                        ws_sender.send(msg).await.unwrap();
                    }
                }
                msg = ws_receiver.next() => {
                    match msg {
                        Some(msg) => {
                            match msg {
                                Err(err) => {
                                    break Err(err)
                                }
                                Ok(msg) => {
                                    if let Err(err) = self.proxy_tx.send(msg) {
                                        error!("{}", err)
                                    }
                                }
                            }
                        }
                        None => {
                           break Err(Error::ConnectionClosed)
                        }
                    }
                }
            }
        }
    }
}
