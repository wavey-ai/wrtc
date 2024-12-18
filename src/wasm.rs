use bytes::Bytes;
use futures::{select, FutureExt};
use futures_timer::Delay;
use js_sys::Uint8Array;
use log::info;
use matchbox_socket::{PeerState, WebRtcSocket};
use std::sync::Mutex;
use std::time::Duration;
use wasm_bindgen::prelude::*;
use web_sys::Worker;

#[wasm_bindgen]
pub struct WebRtcConnection {
    host: String,
}

#[wasm_bindgen]
impl WebRtcConnection {
    #[wasm_bindgen]
    pub fn new(host: String) -> Self {
        Self { host }
    }

    #[wasm_bindgen]
    pub async fn start(&self, cb: &js_sys::Function) -> Result<(), JsValue> {
        let (mut socket, loop_fut) = WebRtcSocket::new_unreliable(self.host.to_string());
        let loop_fut = loop_fut.fuse();
        futures::pin_mut!(loop_fut);
        let timeout = Delay::new(Duration::from_millis(2));
        futures::pin_mut!(timeout);
        loop {
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
            // Collect peers first to avoid borrowing conflict
            let connected_peers: Vec<_> = socket.connected_peers().collect();
            for peer in connected_peers {
                let id = Bytes::from((1234 as u64).to_le_bytes().to_vec());
                socket.send(id, peer);
            }

            // Accept any messages incoming
            for (peer, packet) in socket.receive() {
                let uint8_array = Uint8Array::new_with_length(packet.len() as u32);
                uint8_array.copy_from(&packet);
                let this = JsValue::NULL;
                cb.call1(&this, &uint8_array)?;
            }

            select! {
                _ = (&mut timeout).fuse() => {
                    timeout.reset(Duration::from_millis(10));
                }
                _ = &mut loop_fut => {
                    break;
                }
            }
        }

        Ok(())
    }
}

#[wasm_bindgen]
pub fn startup(path: String) -> Worker {
    let worker_handle = Worker::new(&path).unwrap();
    worker_handle
}
