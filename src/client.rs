use au::{AuKind, AuPayload};
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use futures::pin_mut;
use futures::{select, FutureExt};
use futures_timer::Delay;
use log::info;
use matchbox_socket::{PeerState, WebRtcSocket};
use std::iter::Iterator;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{broadcast, oneshot, watch};

pub async fn start_server(
    port: u16,
    mut rx: broadcast::Receiver<Bytes>,
) -> Result<
    (
        oneshot::Receiver<()>,
        oneshot::Receiver<()>,
        watch::Sender<()>,
    ),
    Box<dyn std::error::Error + Send + Sync>,
> {
    let (shutdown_tx, mut shutdown_rx) = watch::channel(());
    let (up_tx, up_rx) = oneshot::channel();
    let (fin_tx, fin_rx) = oneshot::channel();

    let srv = async move {
        let (mut socket, loop_fut) =
            WebRtcSocket::new_unreliable(format!("ws://localhost:{}", port));

        let loop_fut = loop_fut.fuse();
        futures::pin_mut!(loop_fut);

        let timeout = Delay::new(Duration::from_millis(100));
        futures::pin_mut!(timeout);

        up_tx.send(());

        loop {
            let shutdown_changed = shutdown_rx.changed().fuse();
            pin_mut!(shutdown_changed);

            // Handle any new peers
            for (peer, state) in socket.update_peers() {
                match state {
                    PeerState::Connected => {
                        info!("Peer joined: {peer}");
                    }
                    PeerState::Disconnected => {
                        info!("Peer left: {peer}");
                    }
                }
            }

            let peers = socket.connected_peers().collect::<Vec<_>>();

            // Accept any messages incoming
            for (peer, packet) in socket.receive() {
                let message = String::from_utf8_lossy(&packet);
                info!("Message from {peer}: {message:?}");
            }

            select! {
                data = rx.recv().fuse() => match data {
                    Ok(data) => {
                        let pkt = Bytes::from(data.to_vec());
                        for peer in peers {
                            socket.send(pkt.clone(), peer);
                        }
                    }
                    Err(e) => {
                        dbg!(e);
                    }
                },
                // Restart this loop every 100ms
                _ = (&mut timeout).fuse() => {
                    timeout.reset(Duration::from_millis(100));
                }

                // Or break if the message loop ends (disconnected, closed, etc.)
                _ = &mut loop_fut => {
                    break;
                }

                _ = &mut shutdown_changed => {
                    info!("Received shutdown signal, exiting...");
                    break;
                }
            }
        }

        info!("webrtc server shutdown!");
        fin_tx.send(());
    };

    tokio::spawn(srv);

    Ok((up_rx, fin_rx, shutdown_tx))
}
