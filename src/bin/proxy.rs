use butter_client::BinanceSnapshot;
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

    let proxy_tx_c = proxy_tx.clone();
    let remote_client = async move {
        let mut reties = 30;
        let mut backoff = 1;
        while reties > 0 {
            reties = reties - 1;
            backoff = backoff * 2;
            let (tx, rx) = tokio::sync::oneshot::channel::<usize>();

            let remote = cfg.remote.clone();
            trace!(
                "connecting to {:?} in {}s reties {}",
                remote,
                backoff,
                reties
            );
            sleep_until(Instant::now() + Duration::from_secs(backoff)).await;

            match client(remote, proxy_tx_c.clone(), tx).await {
                Ok(()) => continue,
                Err(err) => {
                    error!("client disconnected: {:?}", err);
                }
            }
        }
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

async fn client(
    remote: RemoteConnection,
    proxy_tx: broadcast::Sender<Message>,
    reset_backoff: oneshot::Sender<usize>,
) -> anyhow::Result<()> {
    trace!(remote.url);
    let url = url::Url::parse(&remote.url).unwrap();
    let (ws_stream, _) = connect_async(url).await?;
    trace!("Handshake success");
    reset_backoff.send(0).unwrap();

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
                        match msg {
                            Err(err) => error!("{:?}", err),
                            Ok(msg) => {
                                proxy_tx.send(msg)?;
                            }
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
