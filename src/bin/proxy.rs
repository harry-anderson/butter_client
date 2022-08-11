use butter_client::{BinanceSnapshot, ConnectionManager};
use futures_util::{future::join_all, pin_mut, SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, time::Duration};
use structopt::StructOpt;
use tokio::join;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::time::{sleep_until, Instant};
use tokio::{select, sync::broadcast};

use tokio_tungstenite::{
    accept_async, connect_async,
    tungstenite::protocol::Message,
    tungstenite::{Error, Result},
};
use tracing::{error, trace};

#[derive(Debug, Serialize, Deserialize, Clone)]
struct RemoteConnection {
    url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    sub_msg: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Config {
    remote: RemoteConnection,
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

    let (proxy_tx, _proxy_rx) = broadcast::channel::<Message>(256);

    // let proxy_tx_c = proxy_tx.clone();
    let mut mgr = ConnectionManager::new(cfg.remote.url);
    let remote_client = async move {
        mgr.connect().await;
    };

    let proxy_server = async move {
        let addr = "127.0.0.1:9002";
        let listener = TcpListener::bind(&addr).await.expect("Can't listen");
        trace!("Listening on: {}", addr);

        while let Ok((stream, _)) = listener.accept().await {
            let peer = stream
                .peer_addr()
                .expect("connected streams should have a peer address");
            trace!("Peer address: {}", peer);

            tokio::spawn(accept_connection(peer, stream, proxy_tx.subscribe()));
        }
    };
    pin_mut!(remote_client, proxy_server);

    select! {
        _ = remote_client => {}
        _ = proxy_server => {}
    }
}

async fn accept_connection(
    peer: SocketAddr,
    stream: TcpStream,
    proxy_rx: broadcast::Receiver<Message>,
) {
    if let Err(e) = handle_connection(peer, stream, proxy_rx).await {
        match e {
            Error::ConnectionClosed | Error::Protocol(_) | Error::Utf8 => (),
            err => error!("Error processing connection: {}", err),
        }
    }
}

async fn handle_connection(
    peer: SocketAddr,
    stream: TcpStream,
    mut proxy_rx: broadcast::Receiver<Message>,
) -> Result<()> {
    let ws_stream = accept_async(stream).await.expect("Failed to accept");
    trace!("New WebSocket connection: {}", peer);
    let (mut ws_sender, mut ws_receiver) = ws_stream.split();
    let mut interval = tokio::time::interval(Duration::from_millis(1000));

    // Echo incoming WebSocket messages and send a message periodically every second.

    loop {
        tokio::select! {
            msg = ws_receiver.next() => {
                match msg {
                    Some(msg) => {
                        let msg = msg?;
                        if msg.is_text() ||msg.is_binary() {
                            ws_sender.send(msg).await?;
                        } else if msg.is_close() {
                            break;
                        }
                    }
                    None => break,
                }
            }
            msg = proxy_rx.recv() => {
                match msg {
                    Ok(msg) => {
                        ws_sender.send(msg).await?;
                    }
                    Err(err) => {
                        error!("error: {:?}", err)
                    }
                }
            }

            _ = interval.tick() => {
                ws_sender.send(Message::Text("tick".to_owned())).await?;
            }
        }
    }

    Ok(())
}
