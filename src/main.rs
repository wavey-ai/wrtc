use bytes::{BufMut, Bytes, BytesMut};
use tokio::sync::broadcast;
use wrtc::server::start_server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Create a broadcast channel for sending packets
    let (tx, rx) = broadcast::channel::<Bytes>(32);

    // Start the WebRTC server
    let (up_rx, fin_rx, shutdown_tx) = start_server(3536, rx).await?;

    // Wait for the server to start
    up_rx.await?;
    println!("Server is up and running!");

    // Send test packets every second
    let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(1000));

    let mut i = 0;
    loop {
        i += 1;
        interval.tick().await;

        // Create a test packet
        let mut packet = BytesMut::with_capacity(64);
        packet.put_u32(i); // Packet number

        // Send the packet
        if let Err(e) = tx.send(packet.freeze()) {
            eprintln!("Failed to send packet: {}", e);
        } else {
            println!("Sent test packet #{}", i);
        }
    }

    // Signal shutdown after sending all packets
    shutdown_tx.send(())?;

    // Wait for the server to finish
    fin_rx.await?;
    println!("Server has shut down.");

    Ok(())
}
