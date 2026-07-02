pub enum NetAddr {
    Default,
    New(String)
}

pub enum BufSize {
    Default,
    New(usize)
}

pub enum NetPort {
    Default,
    New(u16),
}

pub enum ChannelCap {
    Default,
    New(usize),
}

pub struct NodeConfig {
    pub net_addr: String,
    pub channel_cap: usize,
    pub buf_size: usize,
}

impl NodeConfig {
    pub fn new(
        net_addr: String,
        channel_cap: usize,
        buf_size: usize
    ) -> Self {
        Self {
            net_addr,
            channel_cap,
            buf_size
        }
    }
}

pub struct NetConfig {
    pub net_port: NetPort,
    pub channel_cap: ChannelCap,
    pub net_addr: NetAddr,
    pub buf_size: BufSize
}

impl Default for NetConfig {
    fn default() -> Self {
        Self::new(
            NetPort::Default,
            ChannelCap::Default,
            NetAddr::Default,
            BufSize::Default
        )
    }
}

impl NetConfig {
    pub fn new(
        net_port: NetPort,
        channel_cap: ChannelCap,
        net_addr: NetAddr,
        buf_size: BufSize
    ) -> Self {
        Self {
            net_port,
            channel_cap,
            net_addr,
            buf_size,
        }
    }
}

#[derive(PartialEq, Eq, Hash, Debug, Clone, Copy)]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct Id(pub u32);

impl Id {
    pub fn new(id: u32) -> Self {
        Self(id)
    }
}

#[derive(PartialEq, Eq, Hash, Debug, Clone, Copy)]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub enum SessionRequest {
    CreatePeer { id: Id },
    Connect { id: Id },
    
}