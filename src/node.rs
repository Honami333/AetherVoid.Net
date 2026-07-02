use tokio::sync::RwLock as TokioRwLock;
use tokio::sync::mpsc::Receiver;
use tokio::net::UdpSocket;

use tokio::sync::mpsc;

use std::net::SocketAddr;
use std::sync::Arc;
use std::collections::HashMap;

use rkyv::{Archive, Deserialize};
use rkyv::api::high::HighDeserializer;
use rkyv::api::high::HighValidator;
use rkyv::bytecheck::CheckBytes;
use rkyv::rancor::Error;

use anyhow::{Result, anyhow};

use crate::options::*;
use crate::packet::*;
use crate::utils::*;


pub struct NetNode {
    socket: Arc<UdpSocket>,
    channels: Arc<TokioRwLock<HashMap<u64, mpsc::Sender<Packet>>>>,
    buffer_size: usize,
}

impl NetNode {
    pub async fn default_arc() -> Result<Arc<Self>> {
        let net_node = NetNode::new_arc(NetConfig::default()).await?;

        Ok(net_node)
    }

    pub async fn default() -> Result<Self> {
        let net_node = NetNode::new(NetConfig::default()).await?;

        Ok(net_node)
    }

    pub async fn new_arc(
        net_config: NetConfig
    ) -> Result<Arc<Self>> {
        let net_node = Self::new(net_config).await?;

        Ok(Arc::new(net_node))
    }

    pub async fn new(
        net_config: NetConfig
    ) -> Result<Self> {
        let node_config = create_node_config(net_config);

        let channels_map = TokioRwLock::new(HashMap::new());

        let socket = UdpSocket::bind(node_config.net_addr).await?;

        let net_node = Self {
            socket: Arc::new(socket),
            channels: Arc::new(channels_map),
            buffer_size: node_config.buf_size
        };

        Ok(net_node)
    }
}

impl NetNode {
    pub async fn channel_run(&self) -> Result<()> {
        let mut buf = vec![0_u8; self.buffer_size];

        loop {
            let (len, addr) = self.socket.recv_from(&mut buf).await?;

            let incoming_data = &buf[..len];

            if len < 8 { continue; };

            let (packet_hash, packet_data) = incoming_data.split_at(8);

            let packet_type = u64::from_le_bytes(packet_hash.try_into()?);

            {
                let mut channels = 
                    self.channels.write().await;
                
                if let Some(sender) = channels.get_mut(&packet_type) {
                    let packet = Packet::new(packet_data, addr);

                    sender.try_send(packet)
                        .map_err(
                            |e|
                            anyhow!("Rx receiver not found or closed: {}", e)
                        )?;
                };
            };
        };
    }

    pub async fn create_message_channel<T>(&self) -> Receiver<Packet> 
    where 
        T: Archive + PacketType,
        T::Archived: 
            for<'a> CheckBytes<HighValidator<'a, Error>> 
                + Deserialize<T, HighDeserializer<Error>>,
    {
        let hash: u64 = T::packet_hash();
        let mut channels = self.channels.write().await;

        let (tx, rx) =
            mpsc::channel::<Packet>(self.buffer_size);

        channels.insert(hash, tx);

        rx
    }

    pub async fn pop_message_as<T>(
        &self,
        rx: &mut Receiver<Packet>
    ) -> Option<UnpackedPacket<T>>
    where 
        T: Archive + PacketType,
        T::Archived: 
            for<'a> CheckBytes<HighValidator<'a, Error>> 
                + Deserialize<T, HighDeserializer<Error>>,
    {
        let packet = rx.recv().await?;

        let deserialized: T = rkyv::from_bytes::<T, Error>(&packet.payload).ok()?;

        let unpacked_packet = UnpackedPacket::new(
            deserialized,
            packet.sender
        );

        Some(unpacked_packet)
    }

    pub async fn send_to<T>(&self, msg: &[u8], addr: SocketAddr, msg_type: T) -> Result<()> 
    where 
        T: PacketType
    {
        let message = mark_packet(msg, msg_type.packet_hash_self());

        self.socket.send_to(&message, addr).await?;
        Ok(())
    }
}

impl NetNode {
    pub fn share(self: &Arc<Self>) -> Arc<Self> {
        Arc::clone(self)
    }

    pub fn share_channels(&self) -> Arc<TokioRwLock<HashMap<u64, mpsc::Sender<Packet>>>> {
        Arc::clone(&self.channels)
    }
}