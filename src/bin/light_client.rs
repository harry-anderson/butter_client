use butter_client::{parse_json, BinanceMessage, CoinbaseMsg, ConnectionManager};
use chrono::Utc;
use futures_util::{future::join_all, pin_mut, SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{from_str, from_value, Value};
use structopt::StructOpt;
use tokio::{select, sync::broadcast};
use tokio_tungstenite::{
    tungstenite::protocol::Message,
    tungstenite::{Error, Result},
};
use tracing::{debug, error, field::debug, trace};

#[derive(Debug, Serialize, Deserialize)]
struct Remote {
    url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    sub_msg: Option<String>,
    market: Market,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum Market {
    Binance,
    Coinbase,
}

#[derive(Debug, Serialize, Deserialize)]
struct Config {
    remotes: Vec<Remote>,
}

#[derive(Debug, StructOpt)]
struct Opt {
    #[structopt(short)]
    config_file: String,
}

#[tokio::main]
async fn main() {
    let opt = Opt::from_args();
    let cfg = tokio::fs::read_to_string(opt.config_file)
        .await
        .expect("failed to read config");
    let cfg: Config = serde_yaml::from_str(&cfg).unwrap();
    tracing_subscriber::fmt::init();

    let mut tasks = vec![];
    for remote in cfg.remotes {
        let task = async move {
            let (proxy_tx, mut proxy_rx) = broadcast::channel::<Message>(256);
            let (peer_tx, peer_rx) = broadcast::channel::<Message>(256);
            let proxy_tx_c = proxy_tx.clone();
            let mut mgr = ConnectionManager::new(remote.url, proxy_tx_c, peer_rx);
            let remote_client = async move {
                mgr.connect().await;
            };

            let msg_reader = async move {
                //TODO match msg
                while let Ok(msg) = proxy_rx.recv().await {
                    if let Err(err) = handle_msg(&remote.market, msg).await {
                        error!("handle_msg error: {}", err)
                    }
                }
                debug("msg_reader exited")
            };

            if let Some(sub_msg) = remote.sub_msg {
                if let Err(err) = peer_tx.send(Message::Text(sub_msg)) {
                    error!("peer_tx send error {}", err)
                }
            }

            pin_mut!(remote_client, msg_reader);

            select! {
                _ = remote_client => {}
                _ = msg_reader => {}
            }
        };
        tasks.push(task)
    }
    let out = join_all(tasks).await;

    debug!("out={:?}", out)
}

async fn handle_msg(market: &Market, msg: Message) -> Result<(), Error> {
    match msg {
        Message::Close(c) => {
            if let Some(c) = c {
                trace!("close_frame={:?}", c);
            }
            Err(Error::ConnectionClosed)
        }
        Message::Text(text) => {
            let text = text.as_bytes().to_vec();
            match parse_json(text) {
                Ok(v) => {
                    consume_msg(market, v)?;
                }
                Err(err) => error!("parse_err={}", err),
            }
            Ok(())
        }
        Message::Binary(bytes) => {
            match parse_json(bytes) {
                Ok(v) => {
                    consume_msg(market, v)?;
                }
                Err(err) => error!("parse_err={}", err),
            }
            Ok(())
        }
        _ => {
            trace!("msg={:?}", msg);
            Ok(())
        }
    }
}

fn consume_msg(market: &Market, v: Value) -> Result<(), Error> {
    match market {
        Market::Binance => {
            match from_value::<BinanceMessage>(v) {
                Ok(msg) => {
                    debug!("time={}", msg.event_time);
                }
                Err(err) => {
                    debug!("binance parse error {}", err);
                }
            }
            Ok(())
        }
        Market::Coinbase => {
            match from_value::<CoinbaseMsg>(v) {
                Ok(msg) => {
                    debug!("time={}", msg.time);
                }
                Err(err) => {
                    debug!("coinbase parse error {}", err);
                }
            }

            Ok(())
        }
    }
}
