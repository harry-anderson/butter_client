use std::str::from_utf8;

use futures_util::{pin_mut, StreamExt};
use structopt::StructOpt;
use tokio::select;

use serde_json::Value;
use simd_json;

use tokio_tungstenite::{connect_async, tungstenite::protocol::Message, tungstenite::Error};
use tracing::{instrument, trace};

#[derive(Debug, StructOpt)]
struct Opt {
    #[structopt(short)]
    connect_addr: String,
}

#[tokio::main]
async fn main() {
    let opt = Opt::from_args();
    tracing_subscriber::fmt::init();

    let out = tokio::spawn(client(opt)).await;
    trace!("client_out={:?}", out)

    //
}

#[instrument]
async fn client(opt: Opt) -> Result<(), Error> {
    let url = url::Url::parse(&opt.connect_addr).unwrap();
    let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");
    trace!("ws_handshake success");

    let (ws_sender, ws_receiver) = ws_stream.split();

    // let ws_to_stdout = {
    //     read.for_each(|message| async {
    //         let data = message.unwrap().into_data();
    //         tokio::io::stdout().write_all(&data).await.unwrap();
    //     })
    // };
    //
    // pin_mut!(stdin_to_ws, ws_to_stdout);
    pin_mut!(ws_receiver);
    // future::select(stdin_to_ws, ws_to_stdout).await;
    loop {
        select! {
            msg = ws_receiver.next() => {
                match msg {
                    Some(msg) => {
                        match handle_msg(msg).await {
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

async fn handle_msg(msg: Result<Message, Error>) -> Result<(), Error> {
    let msg = msg?;
    match msg {
        Message::Close(c) => {
            if let Some(c) = c {
                trace!("close_frame={:?}", c);
            }
            Err(Error::ConnectionClosed)
        }
        Message::Text(text) => {
            trace!("text_msg={:?}", text);
            let mut text = text.as_bytes().to_vec();
            let v: Value = simd_json::serde::from_slice(&mut text).unwrap();
            trace!("pared_value={}", v);
            Ok(())
        }
        Message::Binary(bytes) => {
            trace!("bin_msg={:?}", bytes);
            let text = from_utf8(&bytes)?;
            trace!("bin_msg_txt={:?}", text);
            Ok(())
        }
        _ => {
            trace!("misc_msg={:?}", msg);
            Ok(())
        }
    }
}
