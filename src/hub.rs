use tokio::sync::Mutex as TokioMutex;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::iter::zip;

use anyhow::{anyhow, Result};

use crate::options::*;
use crate::node::NetNode;


pub struct SessionHub {
    sessions: TokioMutex<HashMap<Id, PeerEndpoints>>
}

impl Default for SessionHub {
    fn default() -> Self {
        Self::new(HashMap::new())
    }
}

impl SessionHub {
    pub fn default_arc() -> Arc<Self> {
        Self::new_arc(HashMap::new())
    }

    pub fn new_arc(sessions: HashMap<Id, PeerEndpoints>) -> Arc<Self> {
        Arc::new(Self::new(sessions))
    }

    pub fn new(sessions: HashMap<Id, PeerEndpoints>) -> Self {
        Self {
            sessions: TokioMutex::new(sessions)
        }
    }
}

impl SessionHub {
    pub async fn add_peer(&self, id: Id, endpoints: PeerEndpoints) {
        let sessions = &mut self.sessions.lock().await;

        sessions.insert(id, endpoints);
    }

    pub async fn add_peer_group(&self, id: &[Id], endpoints: &[PeerEndpoints]) {
        let sessions = &mut self.sessions.lock().await;

        for (&i, &ep) in zip(id, endpoints) {
            sessions.insert(i, ep);
        };
    }

    pub async fn get_peer(&self, id: Id) -> Option<PeerEndpoints> {
        let sessions = &self.sessions.lock().await;

        sessions.get(&id).copied()
    }

    pub async fn get_peer_group(&self, id: &[Id], buffer: &mut Vec<Option<PeerEndpoints>>) {
        let sessions = &self.sessions.lock().await;

        for i in id {
            buffer.push(sessions.get(i).copied());
        };
    }
}

impl SessionHub {
    pub fn share(self: &Arc<Self>) -> Arc<Self> {
        Arc::clone(self)
    }
}

impl SessionHub {
    pub async fn create_peer(&self, id: Id, addr: SocketAddr) -> Result<String> {
        let endpoints = PeerEndpoints::new(Some(addr), None);

        if self.get_peer(id).await.is_none() {
            self.add_peer(id, endpoints).await;
            Ok(format!("Peer {:?} successfully created", id))
        } else {
            Err( anyhow!("The peer {:?} already exists.", id) )
        }
    }

    pub async fn connect_peer(
        &self,
        net_node: &Arc<NetNode>,
        id: Id,
        addr: SocketAddr
    ) -> Result<String> {
        let endpoints = self.get_peer(id).await;

        if let Some(peer) = endpoints {
            let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&peer)?;
            net_node.send_to(&bytes, addr, peer).await?;

            Ok(format!("Successful connection to the peer {:?}", id))
        } else {
            Err( anyhow!("Peer {:?} not found", id) )
        }
    }
}

#[derive(Debug, Clone, Copy)]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct PeerEndpoints {
    pub main_peer_one: Option<SocketAddr>,
    pub main_peer_two: Option<SocketAddr>
}

impl PeerEndpoints {
    pub fn new(one: Option<SocketAddr>, two: Option<SocketAddr>) -> Self {
        Self {
            main_peer_one: one,
            main_peer_two: two
        }
    }
}

