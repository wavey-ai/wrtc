pub mod client;

use futures::{select, FutureExt};
use futures_timer::Delay;
use js_sys::Uint8Array;
use log::info;
use matchbox_socket::{PeerState, WebRtcSocket};
use std::time::Duration;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use web_sys::Worker;

#[wasm_bindgen]
struct WebRtcConnection {
    host: String,
}

#[wasm_bindgen]
impl WebRtcConnection {
    #[wasm_bindgen]
    pub fn new(host: String) -> Self {
        Self { host }
    }

    #[wasm_bindgen]
    pub async fn start(&self, cb: &js_sys::Function) {
        let (mut socket, loop_fut) = WebRtcSocket::new_reliable(self.host.to_string());

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

            // Accept any messages incoming
            for (peer, packet) in socket.receive() {
                let this = JsValue::NULL;
                let uint8_array = Uint8Array::new_with_length(packet.len() as u32);
                uint8_array.copy_from(&packet);

                // Prepare the callback invocation
                let this = JsValue::NULL;
                cb.call1(&this, &uint8_array);
            }

            select! {
                // Restart this loop every 100ms
                _ = (&mut timeout).fuse() => {
                    timeout.reset(Duration::from_millis(2));
                }

                // Or break if the message loop ends (disconnected, closed, etc.)
                _ = &mut loop_fut => {
                    break;
                }
            }
        }
    }
}

/// Run entry point for the main thread.
#[wasm_bindgen]
pub fn startup(path: String) -> Worker {
    let worker_handle = Worker::new(&path).unwrap();
    worker_handle
}
