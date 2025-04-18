use barter::run_barter_mode;
use chat_room::{ChatCliArgs, ChatRoom, ChatRoomExit};
use clap::Parser;
use rendezvous_server::RendezvousCliArgs;
use std::error::Error;

mod barter;
mod chat_room;
mod rendezvous_server;

#[derive(Parser, Debug)]
#[clap(name = "p2p-file-swapping")]
struct Cli {
    #[arg(long)]
    pub port: Option<String>,

    #[arg(long)]
    pub peer: Option<String>,

    #[arg(long)]
    pub username: Option<String>,

    #[arg(long)]
    pub rendezvous_server: Option<String>, // Will parse as Multiaddr

    #[arg(long)]
    pub discovery_server: Option<String>,

    #[arg(long)]
    pub run_rendezvous_server: bool,

    #[arg(long)]
    pub run_discovery_server: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();

    // Extract the command-line arguments
    let port = cli.port;
    let peer = cli
        .peer
        .map(|addr| addr.parse().expect("Failed to parse peer address"));
    let username = cli.username.unwrap_or_else(|| "Anonymous".to_string());

    // Parse rendezvous_server as Multiaddr if provided
    let rendezvous_server = cli.rendezvous_server.map(|addr| {
        addr.parse()
            .expect("Failed to parse rendezvous server multiaddress")
    });

    // Run as a rendezvous server if requested
    if cli.run_rendezvous_server || cli.run_discovery_server {
        println!("💻 Starting rendezvous server...");

        let rendezvous_args = RendezvousCliArgs {
            port: port.map(|p| p.parse().unwrap_or(rendezvous_server::RENDEZVOUS_PORT)),
            server: true,
            rendezvous_server: None,
        };

        return rendezvous_server::run_rendezvous_server(rendezvous_args).await;
    }

    // Otherwise, run as a chat client
    println!("🔑 Your peer ID: {}", libp2p::PeerId::random());
    println!("💬 Starting chat room...");

    // Convert CLI args to ChatCliArgs - use the rendezvous_server as the server
    let chat_args = ChatCliArgs {
        port: port.clone(),
        peer,
        rendezvous_server,
        username: Some(username.clone()),
    };

    // Start in the chat room
    let mut chat_room = ChatRoom::new(username, chat_args).await?;

    match chat_room.run().await? {
        ChatRoomExit::Quit => {
            println!("👋 Goodbye!");
        }
        ChatRoomExit::AcceptBarter { peer_id, addr } => {
            // If we have a multiaddr, use it, otherwise try to connect to the PeerId directly
            let peer_addr = match addr {
                Some(addr_str) => {
                    // The addr_str might be a comma-separated list of addresses
                    if addr_str.contains(',') {
                        let addrs: Vec<&str> = addr_str.split(',').collect();
                        Some(addrs[0].parse().expect("Failed to parse peer address"))
                    } else {
                        Some(addr_str.parse().expect("Failed to parse peer address"))
                    }
                }
                None => None,
            };

            println!(
                "🔄 Switching to barter mode with peer {} at {:?}",
                peer_id, peer_addr
            );

            // Run the barter mode
            run_barter_mode(port, peer_addr).await?;
        }
        ChatRoomExit::AcceptBarterAsAcceptor { peer_id, peer_name } => {
            println!("🔄 Accepting barter from {} ({})", peer_name, peer_id);

            // Run the barter mode without specifying a peer to connect to
            // We'll be listening for incoming connections
            run_barter_mode(Some("50001".to_string()), None).await?;
        }
    }

    Ok(())
}
