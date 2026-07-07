use aether_void_net::node::NetNode;
use aether_void_net::options::{NetAddr, NetPort, ChannelCap, BufSize, NetConfig};
use aether_void_net::options::{SessionRequest, Id};
use aether_void_net::hub::PeerEndpoints;

use aether_void_net::utils::create_addr;

const MY_IP: &str = "127.0.0.1";


#[tokio::main]
async fn main() {
    let net_config = NetConfig::new(
        NetPort::Default,
        ChannelCap::Default,
        NetAddr::New(MY_IP.to_string()),
        BufSize::Default
    );

    let net_node = NetNode::new_arc(net_config).await.unwrap();

    let net_node_clone = net_node.share();

    tokio::spawn(async move {
        if let Err(error) = net_node_clone.channel_run().await {
            println!("{error}");
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

    let message = SessionRequest::CreatePeer { id: Id::new(1) };

    net_node.send_to(&message, my_addr).await.unwrap();
    
    let message = SessionRequest::Connect { id: Id::new(1) };

    loop {
        net_node.send_to(&message, my_addr).await.unwrap();

        tokio::time::sleep(std::time::Duration::from_secs(1)).await; 
    };
}