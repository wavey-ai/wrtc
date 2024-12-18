#[cfg(not(target_arch = "wasm32"))]
pub mod client;

#[cfg(not(target_arch = "wasm32"))]
pub mod server;

#[cfg(not(target_arch = "wasm32"))]
mod topology;

#[cfg(not(target_arch = "wasm32"))]
mod state;

#[cfg(target_arch = "wasm32")]
pub mod wasm;
