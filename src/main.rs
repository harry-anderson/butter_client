use futures_util::{future::join_all, pin_mut, SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use structopt::StructOpt;
use tokio::select;

use serde_json::Value;
use simd_json;

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
    for remote in cfg.remotes {
        let task = tokio::spawn(async { client(remote).await });
        tasks.push(task)
    }

    let out = join_all(tasks).await;

    trace!("client_out={:?}", out)
}

async fn client(remote: RemoteConnection) -> Result<(), Error> {
    trace!(remote.url);
    let url = url::Url::parse(&remote.url).unwrap();
    let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");
    trace!("Handshake success");

    let (ws_sender, ws_receiver) = ws_stream.split();

    let (to_remote_tx, to_remote_rx) = tokio::sync::mpsc::unbounded_channel();

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
                            Err(err) => break Err(err),
                            Ok(()) => continue
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
                    trace!("pared_value={:?}", v);
                    read_msg(market, v)?;
                }
                Err(err) => error!("parse_err={}", err),
            }
            Ok(())
        }
        Message::Binary(bytes) => {
            match parse_json(bytes) {
                Ok(v) => {
                    trace!("pared_value={:?}", v);
                    read_msg(market, v)?;
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

fn parse_json(mut b: Vec<u8>) -> Result<Value, Box<dyn std::error::Error>> {
    let v: Value = simd_json::serde::from_slice(&mut b)?;
    Ok(v)
}

fn read_msg(market: &Market, v: Value) -> Result<(), Error> {
    match market {
        Market::Binance => {
            trace!("binance_msg={}", v);
            Ok(())
        }
    }
}
