use clap::Parser;
use futures::StreamExt;
use libp2p::{
    Multiaddr, PeerId, identify, noise, rendezvous,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, yamux,
};
use std::{error::Error, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, BufReader, stdin},
    select,
};

// Simple protocol version
const PROTOCOL_VERSION: &str = "/p2p-file-barter/1.0.0";
const RENDEZVOUS_NAMESPACE: &str = "p2p-file-barter";

// Port for the rendezvous server
pub const RENDEZVOUS_PORT: u16 = 50002;

// Server CLI arguments
#[derive(Parser, Debug)]
#[clap(name = "Rendezvous Server")]
pub struct RendezvousCliArgs {
    #[arg(long, help = "Port to listen on")]
    pub port: Option<u16>,

    #[arg(long, help = "Run in server mode")]
    pub server: bool,

    #[arg(long, help = "Server peer ID to connect to")]
    pub rendezvous_server: Option<String>,
}

// Custom NetworkBehaviour for the rendezvous server
#[derive(NetworkBehaviour)]
struct RendezvousServerBehaviour {
    rendezvous: rendezvous::server::Behaviour,
    identify: identify::Behaviour,
}

// Run a rendezvous server using libp2p's built-in protocol
pub async fn run_rendezvous_server(args: RendezvousCliArgs) -> Result<(), Box<dyn Error>> {
    // Create a random PeerId
    let id_keys = libp2p::identity::Keypair::generate_ed25519();
    let local_peer_id = PeerId::from(id_keys.public());
    println!("🔑 Local peer id: {local_peer_id}");

    // Create the rendezvous server behaviour
    let mut swarm = libp2p::SwarmBuilder::with_existing_identity(id_keys)
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_behaviour(|key| {
            // Set up rendezvous protocol
            let rendezvous =
                rendezvous::server::Behaviour::new(rendezvous::server::Config::default());

            // Set up identify protocol
            let identify = identify::Behaviour::new(identify::Config::new(
                PROTOCOL_VERSION.to_string(),
                key.public(),
            ));

            RendezvousServerBehaviour {
                rendezvous,
                identify,
            }
        })?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(7200)))
        .build();

    // Listen on the provided port or default
    let port = args.port.unwrap_or(RENDEZVOUS_PORT);
    let addr = format!("/ip4/0.0.0.0/tcp/{}", port);
    swarm.listen_on(addr.parse()?)?;

    println!("💻 Rendezvous server starting...");
    println!("📡 Listening on port {}", port);
    println!("ℹ️ Peers can connect to this rendezvous server using this peer ID:");
    println!("   {}", local_peer_id);
    println!("⌨️  Type 'quit' to exit");

    let mut stdin = BufReader::new(stdin()).lines();

    loop {
        select! {
            Ok(Some(line)) = stdin.next_line() => {
                match line.trim() {
                    "quit" => {
                        println!("👋 Shutting down rendezvous server...");
                        break;
                    }
                    _ => println!("Unknown command. Type 'quit' to exit or 'list' to show peers."),
                }
            }
            event = swarm.select_next_some() => match event {
                SwarmEvent::NewListenAddr { address, .. } => {
                    println!("📡 Rendezvous server listening on {}", address);
                }
                SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                    println!("🔗 Connection established with {}", peer_id);
                }
                SwarmEvent::ConnectionClosed { peer_id, .. } => {
                    println!("❌ Connection closed with {}", peer_id);
                }
                SwarmEvent::Behaviour(RendezvousServerBehaviourEvent::Rendezvous(e)) => {
                    match e {
                        rendezvous::server::Event::PeerRegistered { peer, registration } => {
                            let namespace = registration.namespace;
                            println!("✅ Peer {} registered in namespace: {}", peer, namespace);
                        },
                        rendezvous::server::Event::PeerUnregistered { peer, namespace } => {
                            println!("❌ Peer {} unregistered from namespace: {}", peer, namespace);
                        },
                        rendezvous::server::Event::DiscoverServed { enquirer, .. } => {
                            println!("🔍 Discovery request served for enquirer: {}", enquirer);
                        },
                        _ => {}
                    }
                }
                _ => {}
            }
        }
    }

    Ok(())
}
