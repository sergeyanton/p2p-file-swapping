use clap::Parser;
use futures::StreamExt;
use libp2p::{
    Multiaddr, PeerId, identify, noise, rendezvous,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, yamux,
};
use std::{collections::HashMap, error::Error, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, BufReader, stdin},
    select,
};

// Simple protocol version
const PROTOCOL_VERSION: &str = "/p2p-file-barter/1.0.0";
const RENDEZVOUS_NAMESPACE: &str = "p2p-file-barter"; // Same namespace as used in chat_room.rs

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
            // Create a custom rendezvous server config
            let rendezvous_config = rendezvous::server::Config::default()
                .with_min_ttl(rendezvous::MIN_TTL) // 2 hours minimum TTL
                .with_max_ttl(rendezvous::MAX_TTL); // 72 hours maximum TTL

            // Set up rendezvous protocol with our custom config
            let rendezvous = rendezvous::server::Behaviour::new(rendezvous_config);

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
    println!("🏷️ Using namespace: {}", RENDEZVOUS_NAMESPACE);
    println!("ℹ️ Peers can connect to this rendezvous server using this peer ID:");
    println!("   {}", local_peer_id);
    println!("ℹ️ Connect with the full multiaddr:");
    println!("   /ip4/<SERVER_IP>/tcp/{}/p2p/{}", port, local_peer_id);
    println!("⌨️  Type 'list' to show registered peers");
    println!("⌨️  Type 'quit' to exit");

    // Track registered peers for the 'list' command
    let mut registered_peers: HashMap<PeerId, String> = HashMap::new();

    let mut stdin = BufReader::new(stdin()).lines();

    loop {
        select! {
            Ok(Some(line)) = stdin.next_line() => {
                match line.trim() {
                    "quit" => {
                        println!("👋 Shutting down rendezvous server...");
                        break;
                    }
                    "list" => {
                        println!("\n=== Registered Peers ===");
                        if registered_peers.is_empty() {
                            println!("No peers currently registered");
                        } else {
                            for (i, (peer_id, namespace)) in registered_peers.iter().enumerate() {
                                println!("{}. {} in namespace: {}", i+1, peer_id, namespace);
                            }
                        }
                        println!("======================\n");
                    }
                    _ => println!("Unknown command. Type 'list' to show peers or 'quit' to exit."),
                }
            }
            event = swarm.select_next_some() => match event {
                SwarmEvent::NewListenAddr { address, .. } => {
                    println!("📡 Rendezvous server listening on {}", address);
                }
                SwarmEvent::ConnectionEstablished { peer_id, endpoint, .. } => {
                    println!("🔗 Connection established with {} via {:?}", peer_id, endpoint);
                }
                SwarmEvent::ConnectionClosed { peer_id, cause, .. } => {
                    println!("❌ Connection closed with {}, cause: {:?}", peer_id, cause);
                    registered_peers.remove(&peer_id); // Remove peer from our tracking when connection closes
                }
                SwarmEvent::Behaviour(RendezvousServerBehaviourEvent::Rendezvous(e)) => {
                    match e {
                        rendezvous::server::Event::PeerRegistered { peer, registration } => {
                            let namespace = registration.namespace.clone();
                            println!("✅ Peer {} registered in namespace: {}", peer, namespace);
                            println!("   TTL: {} seconds", registration.ttl);
                            println!("   Addresses: {}", registration.record.addresses().iter()
                                .map(|a| a.to_string())
                                .collect::<Vec<_>>()
                                .join(", "));

                            // Track this registration
                            registered_peers.insert(peer, namespace.to_string());
                        },
                        rendezvous::server::Event::PeerNotRegistered {peer,error, namespace } => {
                            println!("⚠️ Peer {} ({}) not registered: {:?}",namespace, peer, error);
                        },
                        rendezvous::server::Event::PeerUnregistered { peer, namespace } => {
                            println!("❌ Peer {} unregistered from namespace: {}", peer, namespace);
                            registered_peers.remove(&peer);
                        },
                        rendezvous::server::Event::DiscoverServed { enquirer, registrations, .. } => {
                            println!("🔍 Discovery request from {} - returned {} registration(s)",
                                enquirer, registrations.len());
                        },
                        rendezvous::server::Event::DiscoverNotServed { enquirer, error, .. } => {
                            println!("⚠️ Failed to serve discovery request from {}: {:?}", enquirer, error);
                        },
                        rendezvous::server::Event::RegistrationExpired(registration) => {
                            println!("⏱️ Registration expired for peer {} in namespace: {}",
                                registration.record.peer_id(), registration.namespace);
                            registered_peers.remove(&registration.record.peer_id());
                        },
                    }
                }
                SwarmEvent::Behaviour(RendezvousServerBehaviourEvent::Identify(e)) => {
                    if let identify::Event::Received { peer_id, info, connection_id } = e {
                        println!("ℹ️  Identified peer {}", peer_id);
                        println!("    Protocol version: {}", info.protocol_version);
                        println!("    Agent version: {}", info.agent_version);
                        println!("    Protocols: {}", info.protocols.iter()
                            .map(|p| p.to_string())
                            .collect::<Vec<_>>()
                            .join(", "));
                    }
                }
                _ => {}
            }
        }
    }

    Ok(())
}
