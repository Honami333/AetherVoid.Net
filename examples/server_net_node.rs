use aether_void_net::node::NetNode;
use aether_void_net::options::SessionRequest;
use aether_void_net::hub::SessionHub;


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

    loop {
        continue;
    };
}