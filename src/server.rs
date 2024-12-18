use crate::{
    state::{RequestedRoom, RoomId, ServerState},
    topology::MatchmakingDemoTopology,
};
use matchbox_signaling::SignalingServerBuilder;
use std::net::SocketAddr;
use tokio::{
    self,
    sync::{oneshot, watch},
};
use tracing::info;

pub async fn start(
    port: u16,
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

    tokio::task::spawn(async move {
        let addr: SocketAddr = ([0, 0, 0, 0], port).into();
        let mut state = ServerState::default();
        let server = SignalingServerBuilder::new(addr, MatchmakingDemoTopology, state.clone())
            .on_connection_request({
                let mut state = state.clone();
                move |connection| {
                    let room_id = RoomId(connection.path.clone().unwrap_or_default());
                    let next = connection
                        .query_params
                        .get("next")
                        .and_then(|next| next.parse::<usize>().ok());
                    let room = RequestedRoom { id: room_id, next };
                    state.add_waiting_client(connection.origin, room);
                    Ok(true) // allow all clients
                }
            })
            .on_id_assignment({
                move |(origin, peer_id)| {
                    info!("Client connected {origin:?}: {peer_id:?}");
                    state.assign_id_to_waiting_client(origin, peer_id);
                }
            })
            .cors()
            .trace()
            .build();
        server
            .serve()
            .await
            .expect("Unable to run signaling server, is it already running?")
    });

    Ok((up_rx, fin_rx, shutdown_tx))
}
