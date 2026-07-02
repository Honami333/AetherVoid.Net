use aether_void_net::node::NetNode;
use aether_void_net::options::{NetAddr, NetPort};
use aether_void_net::options::{SessionRequest, Id};
use aether_void_net::hub::{PeerEndpoints, SessionHub};

use aether_void_net::utils::create_addr;

const MY_IP: &str = "127.0.0.1";


#[tokio::main]
async fn main() {
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