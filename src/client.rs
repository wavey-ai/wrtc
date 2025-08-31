use bytes::{Bytes, BytesMut};
use futures::{pin_mut, select, FutureExt};
use futures_timer::Delay;
use log::{error, info};
use matchbox_socket::{PeerId, PeerState, WebRtcSocket};
use playlists::fmp4_cache::Fmp4Cache;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{sleep, Instant};
use tokio::{
    self,
    sync::{mpsc, oneshot, watch},
};
use xmpegts::define::epsi_stream_type;
use xmpegts::ts::TsMuxer;

struct SocketMessage {
    data: Bytes,
    peer_id: PeerId,
}

struct PeerContext {
    tx: mpsc::Sender<SocketMessage>,
    task_handle: tokio::task::JoinHandle<()>,
}

pub async fn start(
    port: u16,
    fmp4_cache: Arc<Fmp4Cache>,
) -> Result<
    (
        oneshot::Receiver<()>,
        oneshot::Receiver<()>,
        watch::Sender<()>,
    ),
    Box<dyn std::error::Error + Send + Sync>,
> {
    let (shutdown_tx, shutdown_rx) = watch::channel(());
    let (up_tx, up_rx) = oneshot::channel();
    let (fin_tx, fin_rx) = oneshot::channel();

    // Channel for sending messages to the WebSocket
    let (socket_tx, mut socket_rx) = mpsc::channel::<SocketMessage>(128);

    let srv = async move {
        let (mut socket, loop_fut) =
            WebRtcSocket::new_unreliable(format!("ws://localhost:{}", port));
        let loop_fut = loop_fut.fuse();
        futures::pin_mut!(loop_fut);

        let timeout = Delay::new(Duration::from_millis(100));
        futures::pin_mut!(timeout);

        let mut peer_contexts: HashMap<PeerId, PeerContext> = HashMap::new();

        let _ = up_tx.send(());

        let mut shutdown_watch = shutdown_rx.clone();
        loop {
            select! {
            msg = socket_rx.recv().fuse() => {
                            match msg {
                Some(msg) => {
                  socket.send(msg.data, msg.peer_id);
                }
                None => {
                  error!("Socket channel closed unexpectedly");
                  break;
                }
              }
            }

            _ = (& mut timeout).fuse() => {
              for (peer, state) in socket.update_peers() {
                                match state {
                  PeerState:: Connected => {
                    info!("Peer joined: {peer}");
                  }
                  PeerState:: Disconnected => {
                    info!("Peer left: {peer}");
                    if let Some(context) = peer_contexts.remove(& peer) {
                      context.task_handle.abort();
                    }
                  }
                }
              }

              for (peer, packet) in socket.receive() {
                if packet.len() == 8 {
                  if let Ok(id) = packet[..8].try_into().map(u64:: from_le_bytes) {
                    info!("Received ID from peer {}: {}", peer, id);

                    if !peer_contexts.contains_key(& peer) {
                      let tx = socket_tx.clone();
                      let peer_id = peer;
                      let fmp4_cache = fmp4_cache.clone();
                      let peer_shutdown = shutdown_rx.clone();

                      let handle = tokio:: spawn(async move {
                        handle_peer_cache(
                          peer_id,
                          id,
                          tx,
                          fmp4_cache,
                          peer_shutdown,
                        ).await;
                      });

                      peer_contexts.insert(peer, PeerContext {
                        tx: socket_tx.clone(),
                        task_handle: handle,
                      });
                    }
                  }
                }
              }

              timeout.reset(Duration:: from_millis(100));
            }

            _ = & mut loop_fut => break,

              _ = shutdown_watch.changed().fuse() => {
                info!("Received shutdown signal, exiting...");
                // Abort all peer tasks
                for (_, context) in peer_contexts.drain() {
                  context.task_handle.abort();
                }
                break;
              }
            }
        }

        info!("WebRTC server shutdown!");
        let _ = fin_tx.send(());
    };

    tokio::spawn(srv);
    Ok((up_rx, fin_rx, shutdown_tx))
}

async fn handle_peer_cache(
    peer: PeerId,
    id: u64,
    socket_tx: mpsc::Sender<SocketMessage>,
    fmp4_cache: Arc<Fmp4Cache>,
    mut shutdown: watch::Receiver<()>,
) {
    let mut muxer = TsMuxer::new();
    let pid = match muxer.add_stream(epsi_stream_type::PSI_STREAM_AAC, BytesMut::new()) {
        Ok(pid) => pid,
        Err(e) => {
            error!("Failed to initialize TS muxer for peer {}: {}", peer, e);
            return;
        }
    };

    let mut last = match fmp4_cache.last(id as usize) {
        Some(last) => last,
        None => {
            error!("No last position found for peer {}", peer);
            return;
        }
    };

    loop {
        tokio::select! {
            _ = shutdown.changed() => {
              break;
            }
                  else => {
              last += 1;
                      match get_part(fmp4_cache.clone(), id, last).await {
                Some((data, _)) => {
                  if let Err(e) = muxer.write(
                    pid,
                    0,  // pts
                    0,  // dts
                    0,  // pcr
                    data.try_into().unwrap(),
                  ) {
                    error!("Failed to write to muxer for peer {}: {}", peer, e);
                    continue;
                  }

                  let muxed_data = muxer.get_data();
                  for chunk in muxed_data.chunks(188 * 6) {
                    let msg = SocketMessage {
                      data: Bytes:: from(chunk.to_vec()),
                        peer_id: peer,
                                  };
                  if let Err(e) = socket_tx.send(msg).await {
                    error!("Failed to send to socket task for peer {}: {}", peer, e);
                    return;
                  }
                }
              }
              None => {
                sleep(Duration:: from_millis(100)).await;
              }
            }
          }
        }
    }

    info!("Peer cache handler ended for peer {}", peer);
}

async fn get_part(fmp4_cache: Arc<Fmp4Cache>, path: u64, id: usize) -> Option<(Bytes, u64)> {
    let timeout = Duration::from_secs(3);
    let start_time = Instant::now();
    let poll_interval = Duration::from_millis(1);

    while start_time.elapsed() < timeout {
        if let Some(data) = fmp4_cache.get(path as usize, id).await {
            return Some(data.clone());
        }
        sleep(poll_interval).await;
    }
    None
}
