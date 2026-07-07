use std::net::SocketAddr;

use rkyv::Archive;


pub trait PacketType {
    fn packet_hash() -> u64;
    fn packet_hash_self(&self) -> u64;
}

impl<T: Archive> PacketType for T {
    fn packet_hash() -> u64 {
        let name = std::any::type_name::<T>();

        let mut hash = 0xcbf29ce484222325;

        for byte in name.as_bytes() {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        };

        hash
    }

    fn packet_hash_self(&self) -> u64 {
        Self::packet_hash()
    }
}

pub struct Packet {
    pub payload: bytes::Bytes,
    pub sender: SocketAddr,
}

impl Packet {
    pub fn new(payload: bytes::Bytes, sender: SocketAddr) -> Self {
        Self { payload, sender }
    }
}

pub struct UnpackedPacket<T> {
    pub payload: T,
    pub sender: SocketAddr,
}

impl<T> UnpackedPacket<T> {
    pub fn new(payload: T, sender: SocketAddr) -> Self {
        Self { payload, sender }
    }
}
