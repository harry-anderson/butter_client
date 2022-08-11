use butter_client::{parse_json, BinanceSnapshot, Order};
use futures_util::{future::join_all, pin_mut, SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use structopt::StructOpt;
use tokio::select;

use serde_json::Value;

use tokio_tungstenite::{connect_async, tungstenite::protocol::Message, tungstenite::Error};
use tracing::{error, trace};

#[derive(Debug, Serialize, Deserialize)]
struct RemoteConnection {
    market: Market,
    url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    sub_msg: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum Market {
    Binance,
}

#[derive(Debug, Serialize, Deserialize)]
struct Config {
    remotes: Vec<RemoteConnection>,
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
        .expect("failed to read c.yaml");
    let cfg: Config = serde_yaml::from_str(&cfg).unwrap();
    tracing_subscriber::fmt::init();

    let mut tasks = vec![];
    // rotes
    for remote in cfg.remotes {
        let task = tokio::spawn(client(remote));
        tasks.push(task)
    }

    let out = join_all(tasks).await;

    trace!("client_out={:?}", out)
}

async fn client(remote: RemoteConnection) -> anyhow::Result<()> {
    trace!(remote.url);
    let url = url::Url::parse(&remote.url).unwrap();
    let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");
    trace!("Handshake success");

    let (ws_sender, ws_receiver) = ws_stream.split();

    let (to_remote_tx, to_remote_rx) = tokio::sync::mpsc::unbounded_channel();

    tokio::spawn(async move {
        let client = reqwest::Client::new();
        let body: BinanceSnapshot = client
            .get("https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=1000")
            .send()
            .await?
            .json()
            .await?;
        // let v = parse_json(body.into_bytes())?;

        let last_update_id = body.last_update_id;
        trace!(
            "depth_snapshot last_update_id={} bids={:?} asks={:?}",
            last_update_id,
            body.bids,
            body.asks
        );
        Ok::<(), anyhow::Error>(())
    });

    if let Some(sub_msg) = remote.sub_msg {
        to_remote_tx.send(Message::Text(sub_msg)).unwrap()
    }

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
                        match handle_msg(&remote.market, msg).await {
                            Err(err) => break Err(anyhow::anyhow!(err)),
                            Ok(()) => continue
                        }
                    }
                    None => {
                       break Err(anyhow::anyhow!(Error::ConnectionClosed))
                    }
                }
            }
        }
    }
}

async fn handle_msg(market: &Market, msg: Result<Message, Error>) -> Result<(), Error> {
    let msg = msg?;
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
            trace!("binance_msg={}", v);
            Ok(())
        }
    }
}
