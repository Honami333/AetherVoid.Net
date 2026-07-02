use tokio::sync::Mutex as TokioMutex;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::iter::zip;

use crate::options::*;
use crate::hub::*;

pub const DEF_BUFFER_SIZE: usize = 65536;
pub const DEF_NET_ADDR: &str = "0.0.0.0";
pub const DEF_NET_PORT: u16 = 8080;
pub const DEF_CHANNEL_CAP: usize = 256;


pub fn create_node_config(net_config: NetConfig) -> NodeConfig {
    let target_port = match_port(net_config.net_port);

    let net_addr = match_addr(net_config.net_addr, target_port);

    let channel_cap = match_channel_cap(net_config.channel_cap);

    let buf_size = match_buf_size(net_config.buf_size);

    NodeConfig::new(net_addr, channel_cap, buf_size)
}

pub fn create_addr(addr: NetAddr, port: NetPort) -> SocketAddr {
    let target_port = match_port(port);

    let target_addr = match_addr(addr, target_port);

    target_addr.parse().unwrap()
}

pub fn create_sessions_map(
    id: &[Id],
    endpoints: &[PeerEndpoints]
) -> TokioMutex<HashMap<Id, PeerEndpoints>> {
    let mut sessions_map = HashMap::new();

    for (&i, &ep) in zip(id, endpoints) {
        sessions_map.insert(i, ep);
    };

    TokioMutex::new(sessions_map)
}

pub fn match_port(port: NetPort) -> u16 {
    match port {
        NetPort::Default => DEF_NET_PORT,
        NetPort::New(p) => p
    }
}

pub fn match_addr(addr: NetAddr, port: u16) -> String {
    match addr {
        NetAddr::Default => format!("{}:{}", DEF_NET_ADDR, port ),
        NetAddr::New(ad) => format!("{}:{}", ad, port ),
    }
}

pub fn match_buf_size(buf_size: BufSize) -> usize {
    match buf_size {
        BufSize::Default => DEF_BUFFER_SIZE,
        BufSize::New(size) => size,
    }
}

pub fn match_channel_cap(channel: ChannelCap) -> usize {
    match channel {
        ChannelCap::Default => DEF_CHANNEL_CAP,
        ChannelCap::New(c) => c,
    }
}

pub fn mark_packet(msg: &[u8], packet_type: u64) -> Vec<u8> {
    let mut final_msg = Vec::with_capacity(8 + msg.len());
    final_msg.extend_from_slice(&packet_type.to_le_bytes());
    final_msg.extend_from_slice(msg);

    final_msg
}