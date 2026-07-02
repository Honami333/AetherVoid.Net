## AetherVoid.Net
A decentralized P2P networking library written in Rust. It features strict type-safe messaging, compile-time type hashing for automatic packet routing, and zero-copy serialization.

## Features

* Zero-copy serialization via rkyv 0.8
* Async background packet routing with zero global network locks
* Isolated, typed channels for every message processor
* Dynamic worker registration based on compile-time type name hashing

## Architecture

* Sorter: A single background loop that reads raw UDP packets, extracts the first 8 bytes as a u64 type hash, and routes the packet to the correct channel.
* Workers: Isolated async tasks spawned per data type. Workers read exclusively from their own dedicated channel without intercepting other traffic.

## Quick Start

use aether_void_net::node::NetNode;use aether_void_net::options::{NetAddr, NetPort};use aether_void_net::options::{SessionRequest, Id};use aether_void_net::hub::{PeerEndpoints, SessionHub};
use aether_void_net::utils::create_addr;
const MY_IP: &str = "127.0.0.1";


```rust
#[tokio::main]async fn main() {
    let net_node = NetNode::default_arc().await.unwrap();

    let hub = SessionHub::default_arc();

    let net_node_clone = net_node.share();

    tokio::spawn(async move {
        if let Err(error) = net_node_clone.channel_run().await {
            println!("{error}");
        }
    });

    let net_node_clone = net_node.share();

    tokio::spawn(async move {
        let mut rx = net_node_clone.create_message_channel::<SessionRequest>().await;

        loop {
            if let Some(message) = 
                net_node_clone.pop_message_as::<SessionRequest>(&mut rx).await {
                
                match message.payload {
                    SessionRequest::CreatePeer { id } => {
                        let status = hub.create_peer(id, message.sender).await;

                        println!("{:?}", status);
                    },
                    SessionRequest::Connect { id } => {
                        let status = hub.connect_peer(&net_node_clone, id, message.sender).await;

                        println!("{:?}", status);
                    }
                };

                println!("Message: {:?} from: {}", message.payload, message.sender);
            };
        }
    });

    let net_node_clone = net_node.share();

    tokio::spawn(async move {
        let mut rx = net_node_clone.create_message_channel::<PeerEndpoints>().await;

        loop {
            if let Some(message) = 
                net_node_clone.pop_message_as::<PeerEndpoints>(&mut rx).await {

                println!("PeerEndpoints: {:?} from: {}", message.payload, message.sender);
            };
        }
    });

    let my_addr = create_addr(
        NetAddr::New(MY_IP.to_string()),
        NetPort::Default
    );

    let message_string = SessionRequest::CreatePeer { id: Id::new(1) };
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&message_string).unwrap();

    net_node.send_to(&bytes, my_addr, message_string).await.unwrap();
    
    let message_string = SessionRequest::Connect { id: Id::new(1) };
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&message_string).unwrap();

    loop {
        net_node.send_to(&bytes, my_addr, message_string).await.unwrap();

        tokio::time::sleep(std::time::Duration::from_secs(1)).await; 
    };
}
```

## Roadmap## Phase 1: Lobby and High-Level API

* Implement fast and slow packet processing layers for lobby communication
* Design high-level developer wrappers to encapsulate raw rkyv serialization calls

## Phase 2: NAT Hole Punching

* Add basic UDP hole punching methods for standard routers using PeerEndpoints data
* Design advanced hole punching strategies to bypass symmetric NAT architectures

## Phase 3: Security and Peer Validation

* Implement state-validation security to prevent malicious packets without a voting system
* Integrate cryptographic digital signatures into packet headers to verify peer identity

## Phase 4: Infrastructure Integration

* Build an async database connector interface for storing session history and hub state