use clap::Parser;
use futures::StreamExt;
use libp2p::{
    Multiaddr, PeerId, StreamProtocol, gossipsub, identify, identity, kad, noise, rendezvous,
    request_response,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, yamux,
};
use request_response::{Config as RpConfig, Message, ProtocolSupport, ResponseChannel};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, error::Error, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, BufReader, stdin},
    select,
};

const CHAT_TOPIC: &str = "p2p_file_barter_chat";
const RENDEZVOUS_NAMESPACE: &str = "p2p-file-barter";
const MIN_DISCOVERY_INTERVAL: Duration = Duration::from_secs(30); // Minimum interval between discoveries

#[derive(Parser, Debug)]
#[clap(name = "libp2p chat room")]
pub struct ChatCliArgs {
    #[arg(long)]
    pub port: Option<String>,

    #[arg(long)]
    pub peer: Option<libp2p::Multiaddr>,

    #[arg(long)]
    pub rendezvous_server: Option<libp2p::Multiaddr>,

    #[arg(long)]
    pub username: Option<String>,
}

// Message types for direct peer-to-peer barter negotiation
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BarterNegotiationRequest {
    ProposeBarterSession,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BarterNegotiationResponse {
    Accept { addrs: Vec<String> },
    Decline,
}

// PeerInfo struct for tracking peer information
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerInfo {
    pub peer_id: String,
    pub username: String,
    pub addrs: Vec<String>,
}

#[derive(NetworkBehaviour)]
pub struct ChatBehaviour {
    pub gossipsub: gossipsub::Behaviour,
    pub barter_protocol:
        request_response::cbor::Behaviour<BarterNegotiationRequest, BarterNegotiationResponse>,
    pub identify: identify::Behaviour,
    pub rendezvous: rendezvous::client::Behaviour,
    pub kad: kad::Behaviour<kad::store::MemoryStore>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ChatMessage {
    UserMessage {
        sender_id: String,
        sender_name: String,
        content: String,
        timestamp: u64,
    },
    UserPresence {
        action: PresenceAction,
        user_id: String,
        user_name: String,
    },
    BarterRequest {
        from_id: String,
        from_name: String,
        to_id: String,
    },
    BarterAccepted {
        from_id: String,
        from_name: String,
        to_id: String,
        addrs: Vec<String>, // List of Multiaddrs of the acceptor
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum PresenceAction {
    Join,
    Leave,
}

pub enum ChatRoomExit {
    Quit,
    AcceptBarter {
        peer_id: String,
        addr: Option<String>,
    },
    AcceptBarterAsAcceptor {
        peer_id: String,
        peer_name: String,
    },
}

pub struct ChatRoom {
    swarm: libp2p::Swarm<ChatBehaviour>,
    connected_peers: HashMap<PeerId, String>, // PeerId -> Username
    user_name: String,
    chat_topic: gossipsub::IdentTopic,
    last_heartbeat: std::time::Instant,
    pending_barter_request: Option<(String, String)>, // (peer_id, peer_name)
    pending_barter_channel: Option<ResponseChannel<BarterNegotiationResponse>>, // Response channel for pending barter
    rendezvous_server: Option<PeerId>, // Rendezvous server peer ID if connected
    discovered_peers: HashMap<PeerId, Vec<Multiaddr>>, // Peers discovered via rendezvous
    last_discovery_time: Option<std::time::Instant>,
}

impl ChatRoom {
    pub async fn new(username: String, args: ChatCliArgs) -> Result<Self, Box<dyn Error>> {
        // Create a random PeerId
        let id_keys = identity::Keypair::generate_ed25519();
        let local_peer_id = PeerId::from(id_keys.public());
        println!("Local peer id: {local_peer_id}");

        // Set up gossipsub configuration
        let gossipsub_config = gossipsub::ConfigBuilder::default()
            .heartbeat_interval(Duration::from_secs(5)) // Faster heartbeats
            .validation_mode(gossipsub::ValidationMode::Permissive)
            .flood_publish(true) // Enable flood publishing for better propagation
            .history_length(20) // Keep more history
            .history_gossip(10) // Gossip more history
            .message_id_fn(|message: &gossipsub::Message| {
                // Use the message data to create a unique ID
                let mut s = DefaultHasher::new();
                message.data.hash(&mut s);
                message.source.hash(&mut s);
                gossipsub::MessageId::from(s.finish().to_string())
            })
            .build()
            .expect("Valid gossipsub config");

        // Create a gossipsub topic for our chat room
        let chat_topic = gossipsub::IdentTopic::new(CHAT_TOPIC);
        println!("📢 Creating chat room topic: {}", CHAT_TOPIC);

        // Configure request-response for barter negotiation with CBOR encoding
        let barter_protocols = vec![(
            StreamProtocol::new("/barter-negotiation/1.0.0"),
            ProtocolSupport::Full,
        )];
        let barter_config = RpConfig::default()
            .with_request_timeout(Duration::from_secs(30))
            .with_max_concurrent_streams(100);

        // Build the swarm
        let mut swarm = libp2p::SwarmBuilder::with_existing_identity(id_keys.clone())
            .with_tokio()
            .with_tcp(
                tcp::Config::default(),
                noise::Config::new,
                yamux::Config::default,
            )?
            .with_behaviour(|key| {
                // Set up gossipsub
                let gossipsub = gossipsub::Behaviour::new(
                    gossipsub::MessageAuthenticity::Signed(key.clone()),
                    gossipsub_config,
                )
                .expect("Correct configuration");

                // Set up barter negotiation with request-response protocol
                let barter_protocol =
                    request_response::cbor::Behaviour::new(barter_protocols, barter_config);

                // Set up identity protocol
                let identify = identify::Behaviour::new(identify::Config::new(
                    "/p2p-file-barter/1.0.0".to_string(),
                    key.public(),
                ));

                // Set up rendezvous client
                let rendezvous = rendezvous::client::Behaviour::new(key.clone());

                // Set up Kademlia DHT
                let kad = kad::Behaviour::new(
                    PeerId::from(key.public()),
                    kad::store::MemoryStore::new(PeerId::from(key.public())),
                );

                ChatBehaviour {
                    gossipsub,
                    barter_protocol,
                    identify,
                    rendezvous,
                    kad,
                }
            })?
            .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(7200)))
            .build();

        // Subscribe to the chat topic - this is critical for the gossipsub messaging
        match swarm.behaviour_mut().gossipsub.subscribe(&chat_topic) {
            Ok(_) => println!("✅ Successfully subscribed to chat topic: {}", CHAT_TOPIC),
            Err(e) => return Err(format!("Failed to subscribe to chat topic: {}", e).into()),
        }

        // Listen on the given port or a random one
        let listen_port = args.port.unwrap_or("0".to_string());
        let multiaddr = format!("/ip4/0.0.0.0/tcp/{listen_port}");
        swarm.listen_on(multiaddr.parse()?)?;

        // Connect to the rendezvous server if specified
        let rendezvous_server = if let Some(server_addr) = args.rendezvous_server {
            // Extract the PeerId from the multiaddress
            // Multiaddresses with peer IDs end with /p2p/{peer-id}
            let addr_str = server_addr.to_string();
            if let Some(pos) = addr_str.rfind("/p2p/") {
                let peer_id_str = &addr_str[pos + 5..]; // Skip the "/p2p/" prefix
                match peer_id_str.parse::<PeerId>() {
                    Ok(peer_id) => {
                        println!("🔄 Will connect to rendezvous server: {}", peer_id);
                        // Dial the full multiaddress
                        if let Err(e) = swarm.dial(server_addr.clone()) {
                            println!("⚠️ Failed to dial rendezvous server: {}", e);
                        } else {
                            println!("🔌 Connecting to rendezvous server at {}", server_addr);
                        }
                        Some(peer_id)
                    }
                    Err(e) => {
                        println!("⚠️ Invalid rendezvous server peer ID: {}", e);
                        None
                    }
                }
            } else {
                println!("⚠️ Rendezvous server address doesn't contain a peer ID");
                None
            }
        } else {
            None
        };

        // If the user supplied a peer to connect to, connect to it
        if let Some(peer) = args.peer {
            println!("🔗 Connecting to peer: {}", peer);
            swarm.dial(peer)?;
        }

        println!("🚀 Chat room initialized and ready");

        Ok(ChatRoom {
            swarm,
            connected_peers: HashMap::new(),
            user_name: username,
            chat_topic,
            last_heartbeat: std::time::Instant::now(),
            pending_barter_request: None,
            pending_barter_channel: None,
            rendezvous_server,
            discovered_peers: HashMap::new(),
            last_discovery_time: None,
        })
    }

    // Register with the rendezvous server
    async fn register_with_rendezvous(&mut self) -> Result<(), Box<dyn Error>> {
        if let Some(server_id) = self.rendezvous_server {
            println!("🔄 Registering with rendezvous server: {}", server_id);

            // Create a namespace for the chat room
            if let Ok(namespace) = rendezvous::Namespace::new(RENDEZVOUS_NAMESPACE.to_string()) {
                // Make sure we have external addresses before registration
                // External addresses are essential for other peers to connect to us
                if self.swarm.listeners().count() == 0 {
                    println!("⚠️ No listening addresses available for registration");
                    return Err("No listening addresses available".into());
                }

                // First collect all listener addresses into a Vec
                // This avoids the borrowing conflict
                let listen_addrs: Vec<_> = self.swarm.listeners().cloned().collect();

                // Clear any existing external addresses to avoid duplicates
                let external_addrs: Vec<_> = self.swarm.external_addresses().cloned().collect();
                for addr in external_addrs {
                    self.swarm.remove_external_address(&addr);
                }

                println!("🔍 Adding external addresses for registration:");

                // First add all listener addresses as external addresses
                for addr in &listen_addrs {
                    // Skip loopback addresses as they're not useful for external connections
                    if !addr.to_string().contains("/ip4/127.0.0.1/") {
                        self.swarm.add_external_address(addr.clone());
                        println!("    ✅ Adding listener address: {}", addr);
                    } else {
                        println!("    ❌ Skipping loopback address: {}", addr);
                    }
                }

                // Get the local IP address and add it as external address
                if let Ok(ip) = local_ip_address::local_ip() {
                    println!("🌐 Detected local network IP: {}", ip);

                    // Add external addresses with the real IP for all listening ports
                    for addr in &listen_addrs {
                        if let Some(port) = extract_port_from_multiaddr(addr) {
                            // Only add if it's not a loopback address
                            if !ip.is_loopback() {
                                let external_addr: Multiaddr =
                                    format!("/ip4/{}/tcp/{}", ip, port).parse()?;
                                self.swarm.add_external_address(external_addr.clone());
                                println!(
                                    "    ✅ Adding explicit external address: {}",
                                    external_addr
                                );
                            }
                        }
                    }
                } else {
                    println!("⚠️ Could not detect local IP address");
                }

                // Print all external addresses for debugging
                let external_addr_count = self.swarm.external_addresses().count();
                println!(
                    "🔍 [DEBUG] External addresses for registration ({}): ",
                    external_addr_count
                );
                for addr in self.swarm.external_addresses() {
                    println!("    {}", addr.clone());
                }

                if external_addr_count == 0 {
                    println!("❌ ERROR: No external addresses available for registration!");
                    println!("   The rendezvous server requires external addresses to register.");
                    println!("   This may be due to network configuration issues.");
                    return Err("No external addresses available for registration".into());
                }

                // Check if we're connected to the rendezvous server
                let mut connected_to_server = false;

                // Check if we're already connected to the server
                for peer_id in self.swarm.connected_peers() {
                    if *peer_id == server_id {
                        connected_to_server = true;
                        println!("✅ Already connected to rendezvous server: {}", server_id);
                        break;
                    }
                }

                // If not connected yet, try to connect and wait
                if !connected_to_server {
                    println!("🔄 Connecting to rendezvous server: {}", server_id);

                    // Try dialing with multiaddr format: /p2p/{peer_id}
                    let p2p_addr = format!("/p2p/{}", server_id).parse::<Multiaddr>()?;
                    match self.swarm.dial(p2p_addr) {
                        Ok(_) => println!("🔄 Dial request sent to rendezvous server"),
                        Err(e) => println!("⚠️ Failed to dial rendezvous server: {}", e),
                    }

                    // Wait for connection to establish
                    println!("⏳ Waiting for connection to establish...");
                    tokio::time::sleep(Duration::from_secs(2)).await;

                    // Check again if we're connected
                    for peer_id in self.swarm.connected_peers() {
                        if *peer_id == server_id {
                            connected_to_server = true;
                            println!(
                                "✅ Connection established to rendezvous server: {}",
                                server_id
                            );
                            break;
                        }
                    }

                    if !connected_to_server {
                        println!(
                            "⚠️ Not connected to rendezvous server yet. Will attempt registration anyway."
                        );
                    }
                }

                // Register with the rendezvous server
                match self.swarm.behaviour_mut().rendezvous.register(
                    namespace.clone(),
                    server_id,
                    Some(libp2p::rendezvous::DEFAULT_TTL), // Use the default TTL (2 hours)
                ) {
                    Ok(()) => {
                        println!("📤 Registration request sent to rendezvous server");
                    }
                    Err(e) => {
                        println!("❌ Failed to register with rendezvous server: {}", e);

                        // Show detailed error information
                        if e.to_string().contains("no external addresses") {
                            println!(
                                "   This error indicates that the client has no valid external addresses."
                            );
                            println!(
                                "   Make sure your network allows incoming connections and your"
                            );
                            println!("   external addresses are properly configured.");

                            // Print current external addresses to help diagnose
                            println!("\n   Current external addresses:");
                            for addr in self.swarm.external_addresses() {
                                println!("      {}", addr);
                            }
                        }

                        return Err(format!("Registration request failed: {}", e).into());
                    }
                }
            } else {
                println!("⚠️ Failed to create namespace for rendezvous");
                return Err("Invalid namespace".into());
            }
        } else {
            return Err("No rendezvous server specified".into());
        }

        Ok(())
    }

    // Announce presence in the chat room
    pub async fn announce_presence(&mut self) -> Result<(), Box<dyn Error>> {
        let presence_msg = ChatMessage::UserPresence {
            action: PresenceAction::Join,
            user_id: self.swarm.local_peer_id().to_string(),
            user_name: self.user_name.clone(),
        };

        let serialized = serde_json::to_string(&presence_msg)?;
        self.swarm
            .behaviour_mut()
            .gossipsub
            .publish(self.chat_topic.clone(), serialized.as_bytes())?;

        Ok(())
    }

    // Send a chat message to the room
    pub async fn send_message(&mut self, content: String) -> Result<(), Box<dyn Error>> {
        let chat_msg = ChatMessage::UserMessage {
            sender_id: self.swarm.local_peer_id().to_string(),
            sender_name: self.user_name.clone(),
            content,
            timestamp: chrono::Utc::now().timestamp() as u64,
        };

        let serialized = serde_json::to_string(&chat_msg)?;
        match self
            .swarm
            .behaviour_mut()
            .gossipsub
            .publish(self.chat_topic.clone(), serialized.as_bytes())
        {
            Ok(_) => Ok(()),
            Err(e) => {
                if self.connected_peers.is_empty() {
                    println!(
                        "\n⚠️ Message couldn't be sent: no peers connected. Try using /discover to find peers."
                    );
                    Ok(())
                } else {
                    Err(e.into())
                }
            }
        }
    }

    // Direct request to barter with a peer using the request-response protocol
    pub async fn send_barter_negotiation_request(
        &mut self,
        peer_id_str: &str,
    ) -> Result<(), Box<dyn Error>> {
        // Parse the peer ID string to a PeerId
        let peer_id = peer_id_str.parse::<PeerId>()?;

        println!(
            "[DEBUG] Sending direct barter negotiation request to {:?}",
            peer_id
        );

        // Send the request using the proper request-response protocol
        self.swarm
            .behaviour_mut()
            .barter_protocol
            .send_request(&peer_id, BarterNegotiationRequest::ProposeBarterSession);

        Ok(())
    }

    // Collect our listen addresses for the barter session
    pub async fn collect_barter_addresses(&self, port: u16) -> Vec<String> {
        let mut addrs: Vec<String> = Vec::new();

        // Try to get local IP address - direct connection only, no NAT traversal
        if let Ok(ip) = local_ip_address::local_ip() {
            addrs.push(format!("/ip4/{}/tcp/{}", ip, port));
        }

        // Add localhost
        addrs.push(format!("/ip4/127.0.0.1/tcp/{}", port));

        addrs
    }

    // Discover peers through rendezvous server
    async fn discover_peers(&mut self) -> Result<(), Box<dyn Error>> {
        if let Some(server_id) = self.rendezvous_server {
            if let Ok(namespace) = rendezvous::Namespace::new(RENDEZVOUS_NAMESPACE.to_string()) {
                println!("🔍 Discovering peers via rendezvous server: {}", server_id);

                // Store the current time to track when discovery was last initiated
                self.last_discovery_time = Some(std::time::Instant::now());

                // Use cookie for efficient discovery if we have one (gets only updates since last discovery)
                // This significantly reduces network traffic and server load
                let cookie = None; // For a more advanced implementation, store and reuse cookies
                let limit = Some(50); // Reasonable limit to avoid overwhelming the network

                self.swarm.behaviour_mut().rendezvous.discover(
                    Some(namespace),
                    cookie,
                    limit,
                    server_id,
                );

                println!("🔍 Started peer discovery via rendezvous");
            } else {
                println!("⚠️ Failed to create namespace for rendezvous");
                return Err("Invalid namespace".into());
            }
        } else {
            return Err("No rendezvous server specified".into());
        }

        Ok(())
    }

    // Run the chat room, handle events
    pub async fn run(&mut self) -> Result<ChatRoomExit, Box<dyn Error>> {
        // First announce our presence
        let _ = self.announce_presence().await;

        let mut stdin = BufReader::new(stdin()).lines();

        let heartbeat_interval = Duration::from_secs(30);
        let discovery_interval = Duration::from_secs(300); // 5 minutes fallback
        let mut last_discovery = std::time::Instant::now();

        // Track if we've registered with rendezvous server
        let mut rendezvous_registered = false;

        // Track when we start listening
        let mut listening = false;

        // Track if registration has been attempted
        let mut registration_attempted = false;

        // Track active connections to the rendezvous server
        let mut connected_to_rendezvous = false;

        println!("\n===== P2P Chat Room =====\n");
        println!("Commands:");
        println!("  /help - Show available commands");
        println!("  /peers - Show connected peers");
        println!("  /barter <peer_id> - Request to barter with a peer");
        println!("  /accept - Accept pending barter request");
        println!("  /discover - Discover peers through rendezvous");
        println!("  /quit - Leave the chat room");
        println!("  Just type to send a message to everyone\n");

        loop {
            let now = std::time::Instant::now();

            // If we're listening but not registered with rendezvous server, attempt registration
            if listening && !rendezvous_registered && self.rendezvous_server.is_some() {
                if !connected_to_rendezvous {
                    // Only try to connect if not already connected
                    if let Some(server_id) = self.rendezvous_server {
                        // Try to dial the rendezvous server if not already connected
                        match self
                            .swarm
                            .dial(format!("/p2p/{}", server_id).parse::<Multiaddr>()?)
                        {
                            Ok(_) => println!("🔄 Connecting to rendezvous server: {}", server_id),
                            Err(e) => println!("⚠️ Failed to dial rendezvous server: {}", e),
                        }
                    }
                } else if !registration_attempted {
                    // Only try to register if we have a connection but haven't tried registration yet
                    registration_attempted = true;
                    if let Some(server_id) = self.rendezvous_server {
                        // Register with the native libp2p rendezvous protocol
                        if let Err(e) = self.register_with_rendezvous().await {
                            println!("⚠️ Failed to register with rendezvous server: {}", e);
                        } else {
                            println!(
                                "✅ Registration request sent to rendezvous server (awaiting confirmation)"
                            );
                        }
                    }
                }
            }

            if now.duration_since(self.last_heartbeat) > heartbeat_interval {
                let _ = self.announce_presence().await;
                self.last_heartbeat = now;
            }

            // Periodically discover peers if using rendezvous (fallback)
            if rendezvous_registered
                && self.rendezvous_server.is_some()
                && now.duration_since(last_discovery) > discovery_interval
            {
                let _ = self.discover_peers().await;
                last_discovery = now;
            }

            select! {
                Ok(Some(line)) = stdin.next_line() => {
                    if line.starts_with('/') {
                        // Handle commands
                        let parts: Vec<&str> = line.splitn(2, ' ').collect();
                        match parts[0] {
                            "/help" => {
                                println!("\n===== Commands =====");
                                println!("  /help - Show available commands");
                                println!("  /peers - Show connected peers");
                                println!("  /barter <peer_id> - Request to barter with a peer");
                                println!("  /accept - Accept pending barter request");
                                println!("  /discover - Discover peers through rendezvous");
                                println!("  /quit - Leave the chat room");
                                println!("  Just type to send a message to everyone\n");
                            },
                            "/peers" => {
                                if self.connected_peers.is_empty() {
                                    println!("\n👥  No other peers in the chat room\n");
                                } else {
                                    println!("\n👥  Peers in chat room:");
                                    for (i, (peer_id, username)) in self.connected_peers.iter().enumerate() {
                                        println!("    {}. {} ({})", i+1, username, peer_id);
                                    }
                                    println!();
                                }
                            },
                            "/barter" => {
                                if parts.len() < 2 {
                                    println!("\n⚠️  Usage: /barter <peer_id>\n");
                                    continue;
                                }
                                let peer_id = parts[1].trim();
                                // Accept only PeerId, not username
                                if self.connected_peers.keys().any(|k| k.to_string() == peer_id) {
                                    println!("\n📤  Sending direct barter request to {}\n", peer_id);
                                    // Use the new direct request-response protocol instead of gossipsub
                                    match self.send_barter_negotiation_request(peer_id).await {
                                        Ok(_) => println!("\n💰  Direct barter request sent. Waiting for response...\n"),
                                        Err(e) => println!("\n⚠️  Failed to send barter request: {}\n", e),
                                    }
                                } else {
                                    println!("\n⚠️  Peer {} not found in chat room\n", peer_id);
                                }
                            },
                            "/discover" => {
                                if self.rendezvous_server.is_some() {
                                    match self.discover_peers().await {
                                        Ok(_) => println!("\n🔍  Discovering peers through rendezvous...\n"),
                                        Err(e) => println!("\n⚠️  Failed to discover peers: {}\n", e),
                                    }
                                    last_discovery = std::time::Instant::now();
                                } else {
                                    println!("\n⚠️  No rendezvous server specified. Use --rendezvous-server when starting.\n");
                                }
                            },
                            "/accept" => {
                                if let Some((peer_id, peer_name)) = &self.pending_barter_request {
                                    // Parse the peer ID string back to a PeerId
                                    if let Ok(peer_id_obj) = peer_id.parse::<PeerId>() {
                                        // Collect our listen addresses for the barter session
                                        let barter_port = 50001; // Use the constant port for bartering
                                        let addrs = self.collect_barter_addresses(barter_port).await;

                                        println!("\n🔄  Switching to barter mode with peer {}\n", peer_id);

                                        // Send acceptance with addresses
                                        if let Some(channel) = self.pending_barter_channel.take() {
                                            let _ = self.swarm.behaviour_mut().barter_protocol.send_response(
                                                channel,
                                                BarterNegotiationResponse::Accept { addrs }
                                            );
                                        }

                                        // Return to transition to barter mode
                                        return Ok(ChatRoomExit::AcceptBarterAsAcceptor {
                                            peer_id: peer_id.clone(),
                                            peer_name: peer_name.clone(),
                                        });
                                    } else {
                                        println!("\n⚠️  Failed to parse peer ID. Cannot accept barter.\n");
                                    }
                                } else {
                                    println!("\n⚠️  No pending barter request to accept.\n");
                                }
                            },
                            "/quit" => {
                                // Send leave message
                                let leave_msg = ChatMessage::UserPresence {
                                    action: PresenceAction::Leave,
                                    user_id: self.swarm.local_peer_id().to_string(),
                                    user_name: self.user_name.clone(),
                                };
                                let serialized = serde_json::to_string(&leave_msg)?;
                                self.swarm.behaviour_mut().gossipsub.publish(
                                    self.chat_topic.clone(),
                                    serialized.as_bytes(),
                                )?;

                                // Unregister from rendezvous server if connected
                                if let Some(server_id) = self.rendezvous_server {
                                    if let Ok(namespace) = rendezvous::Namespace::new(RENDEZVOUS_NAMESPACE.to_string()) {
                                        let _ = self.swarm.behaviour_mut().rendezvous.unregister(namespace, server_id);
                                    }
                                }

                                println!("\n👋  Leaving chat room...\n");
                                return Ok(ChatRoomExit::Quit);
                            }
                            _ => println!("\n⚠️  Unknown command. Type '/help' for available commands.\n"),
                        }
                    } else {
                        // Regular chat message
                        self.send_message(line).await?;
                    }
                }
                event = self.swarm.select_next_some() => match event {
                    SwarmEvent::NewListenAddr { address, .. } => {
                        println!("\n📡  Listening on {:?}\n", address);
                        listening = true;  // Mark that we have at least one listening address
                    }
                    SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                        println!("\n🔗  Connection established to {:?}\n", peer_id);
                        let _ = self.announce_presence().await;
                        if !self.connected_peers.contains_key(&peer_id) {
                            self.connected_peers.insert(peer_id, "Connected (awaiting username)".to_string());
                        }

                        // Check if this is a connection to our rendezvous server
                        if let Some(server_id) = self.rendezvous_server {
                            if peer_id == server_id {
                                connected_to_rendezvous = true;
                                println!("✅ Connected to rendezvous server: {}", server_id);

                                // Wait a moment before registration to ensure the connection is fully ready
                                tokio::time::sleep(Duration::from_millis(500)).await;

                                // Now register with the server
                                if !registration_attempted {
                                    registration_attempted = true;
                                    if let Err(e) = self.register_with_rendezvous().await {
                                        println!("⚠️ Failed to register with rendezvous server: {}", e);
                                    } else {
                                        println!("✅ Registration request sent to rendezvous server (awaiting confirmation)");
                                    }
                                }
                            }
                        }
                    }
                    SwarmEvent::ConnectionClosed { peer_id, .. } => {
                        if let Some(username) = self.connected_peers.remove(&peer_id) {
                            println!("\n❌  {} ({:?}) has left the chat room\n", username, peer_id);
                        } else {
                            println!("\n❌  Connection closed with {:?}\n", peer_id);
                        }

                        // Check if this is our rendezvous server
                        if let Some(server_id) = self.rendezvous_server {
                            if peer_id == server_id {
                                connected_to_rendezvous = false;
                                rendezvous_registered = false;
                                println!("❌ Disconnected from rendezvous server: {}", server_id);
                            }
                        }
                    }
                    SwarmEvent::Behaviour(ChatBehaviourEvent::Rendezvous(event)) => match event {
                        rendezvous::client::Event::Discovered { rendezvous_node, registrations, cookie } => {
                            println!("\n🔍 Discovered {} peers via rendezvous server {}", registrations.len(), rendezvous_node);

                            for registration in registrations {
                                // Skip ourselves
                                if registration.record.peer_id() == *self.swarm.local_peer_id() {
                                    continue;
                                }

                                println!("   - Found peer: {} in namespace {}",
                                    registration.record.peer_id(),
                                    registration.namespace);
                                println!("   - TTL: {} seconds", registration.ttl);

                                // Check if we already have this peer in our connected peers
                                if !self.connected_peers.contains_key(&registration.record.peer_id()) {
                                    // Add to discovered peers
                                    self.discovered_peers.insert(
                                        registration.record.peer_id(),
                                        registration.record.addresses().to_vec()
                                    );

                                    // Try to connect to the discovered peer
                                    if !registration.record.addresses().is_empty() {
                                        // Try each address until one works
                                        for addr in registration.record.addresses() {
                                            match self.swarm.dial(addr.clone()) {
                                                Ok(_) => {
                                                    println!("   - Dialing discovered peer {} at {}",
                                                        registration.record.peer_id(), addr);
                                                    break; // Use only the first valid address
                                                }
                                                Err(e) => {
                                                    println!("   - Failed to dial discovered peer at {}: {}", addr, e);
                                                }
                                            }
                                        }
                                    } else {
                                        println!("   - No addresses found for peer {}", registration.record.peer_id());
                                    }
                                }
                            }
                        },
                        rendezvous::client::Event::DiscoverFailed { rendezvous_node, namespace, error } => {
                            println!("\n⚠️ Discovery failed from server {}: {:?}", rendezvous_node, error);
                            println!("   Namespace: {:?}", namespace);

                            // Implement retry logic for transient errors
                            if let rendezvous::ErrorCode::Unavailable = error {
                                println!("   Server may be temporarily unavailable. Will retry later.");
                            }
                        },
                        rendezvous::client::Event::Registered { rendezvous_node, ttl, namespace } => {
                            rendezvous_registered = true;
                            println!("\n✅ Successfully registered with rendezvous server {}", rendezvous_node);
                            println!("   Namespace: {}", namespace);
                            println!("   Registration valid for {} seconds", ttl);

                            // Registration was successful, now we can start discovery
                            self.swarm.behaviour_mut().rendezvous.discover(
                                Some(namespace.clone()),
                                None,
                                Some(50),
                                rendezvous_node,
                            );
                            println!("🔍 Starting peer discovery after successful registration");
                            last_discovery = std::time::Instant::now();
                        },
                        rendezvous::client::Event::RegisterFailed { rendezvous_node, namespace, error } => {
                            println!("\n❌ Registration failed with rendezvous server {}: {:?}", rendezvous_node, error);
                            println!("   Namespace: {}", namespace);

                            // Reset registration flag to allow retries
                            registration_attempted = false;

                            match error {
                                rendezvous::ErrorCode::InvalidNamespace => {
                                    println!("   The namespace is invalid. Please check your namespace configuration.");
                                },
                                rendezvous::ErrorCode::InvalidTtl => {
                                    println!("   The TTL is invalid. Using a value between {} and {} seconds.",
                                        rendezvous::MIN_TTL, rendezvous::MAX_TTL);
                                },
                                rendezvous::ErrorCode::NotAuthorized => {
                                    println!("   Not authorized. The server may require authentication.");
                                },
                                rendezvous::ErrorCode::Unavailable => {
                                    println!("   Server is temporarily unavailable. Will retry in 5 seconds.");
                                    // Schedule a retry after a delay
                                    // tokio::spawn({
                                    //     let rendezvous_server = self.rendezvous_server;
                                    //     let mut this = self.clone();
                                    //     async move {
                                    //         tokio::time::sleep(Duration::from_secs(5)).await;
                                    //         if let Some(server_id) = rendezvous_server {
                                    //             if let Err(e) = this.register_with_rendezvous().await {
                                    //                 println!("⚠️ Retry registration failed: {}", e);
                                    //             }
                                    //         }
                                    //     }
                                    // });
                                },
                                _ => {
                                    println!("   Unknown error occurred during registration.");
                                }
                            }
                        },
                        rendezvous::client::Event::Expired { peer } => {
                            // A peer's registration has expired
                            println!("\n⏱️ Registration for peer {} has expired", peer);

                            // Remove from our discovered peers map
                            self.discovered_peers.remove(&peer);

                            // If we were connected, we might want to check if they're still reachable
                            if self.connected_peers.contains_key(&peer) {
                                println!("   This peer was in our connected peers list. Connection may still be active.");
                            }
                        },
                    },
                    SwarmEvent::Behaviour(ChatBehaviourEvent::Gossipsub(gossipsub::Event::Message {
                        propagation_source: _source,
                        message_id: _message_id,
                        message,
                    })) => {
                        // Handle received chat messages
                        if let Ok(chat_msg) = serde_json::from_slice::<ChatMessage>(&message.data) {
                            match chat_msg {
                                ChatMessage::UserMessage { sender_id, sender_name, content, timestamp } => {
                                    let time = chrono::DateTime::<chrono::Utc>::from_timestamp(timestamp as i64, 0)
                                        .map(|dt| dt.format("%H:%M:%S").to_string())
                                        .unwrap_or_else(|| "??:??:??".to_string());

                                    // Only display messages from others
                                    if sender_id != self.swarm.local_peer_id().to_string() {
                                        println!("\n[{}] {}: {}\n", time, sender_name, content);
                                    }
                                },
                                ChatMessage::UserPresence { action, user_id, user_name } => {
                                    // Skip own presence messages
                                    if user_id == self.swarm.local_peer_id().to_string() {
                                        continue;
                                    }

                                    match action {
                                        PresenceAction::Join | PresenceAction::Leave => {
                                            // Hybrid approach for discovery, better safe than sorry. Discover when someone leaves/joins AND every 5 minutes
                                            if now.duration_since(last_discovery) > MIN_DISCOVERY_INTERVAL {
                                                let _ = self.discover_peers().await;
                                                last_discovery = now;
                                            }
                                        },
                                        _ => {}
                                    }

                                    match action {
                                        PresenceAction::Join => {
                                            // Try to convert the string back to a PeerId
                                            match user_id.parse::<PeerId>() {
                                                Ok(peer_id) => {
                                                    // Check if this is a new peer or an update
                                                    if !self.connected_peers.contains_key(&peer_id) {
                                                        println!("\n👋  {} ({}) joined the chat room\n", user_name, peer_id);
                                                    }
                                                    // Update or insert the peer info
                                                    self.connected_peers.insert(peer_id, user_name.clone());

                                                    // Re-announce our own presence when we receive a new join
                                                    // This helps ensure everyone knows about everyone else
                                                    if self.connected_peers.len() == 1 {  // Only do this for the first peer
                                                        let _ = self.announce_presence().await;
                                                    }
                                                },
                                                Err(e) => {
                                                    println!("\n⚠️  Received presence message with invalid peer ID: {}\n", e);
                                                }
                                            }
                                        },
                                        PresenceAction::Leave => {
                                            if let Ok(peer_id) = user_id.parse::<PeerId>() {
                                                if self.connected_peers.remove(&peer_id).is_some() {
                                                    println!("\n👋  {} ({}) left the chat room\n", user_name, peer_id);
                                                }
                                            }
                                        }
                                    }
                                }

                                ChatMessage::BarterRequest { from_id, from_name, to_id } => {
                                    // Check if this barter request is for us
                                    if to_id == self.swarm.local_peer_id().to_string() {
                                        println!("\n💰  Barter request received from {} ({})", from_name, from_id);
                                        println!("    Type '/accept' to automatically switch to barter mode or");
                                        println!("    Switch manually with 'barter' command in main app\n");

                                        self.pending_barter_request = Some((from_id.clone(), from_name.clone()));
                                    }
                                    // Else if we're the sender, do nothing (we already know we sent it)
                                    // Else show as an info message (X wants to barter with Y)
                                    else if from_id != self.swarm.local_peer_id().to_string() {
                                        // Try to get the username of the recipient
                                        let to_name = match to_id.parse::<PeerId>() {
                                            Ok(peer_id) => self.connected_peers.get(&peer_id)
                                                .cloned()
                                                .unwrap_or_else(|| "Unknown".to_string()),
                                            Err(_) => "Unknown".to_string(),
                                        };

                                        println!("\n💬  {} wants to barter with {}\n", from_name, to_name);
                                    }
                                },
                                ChatMessage::BarterAccepted { from_id, from_name, to_id, addrs } => {
                                    if to_id == self.swarm.local_peer_id().to_string() {
                                        println!("\n✅  {} accepted your barter request! Switching to barter mode...\n", from_name);
                                        println!("    Available addresses: {:?}\n", addrs);

                                        // Join all addresses into a single comma-separated string
                                        let addr_str = addrs.join(",");

                                        // Return AcceptBarter with peer_id and addresses
                                        return Ok(ChatRoomExit::AcceptBarter {
                                            peer_id: from_id,
                                            addr: Some(addr_str)
                                        });
                                    }
                                }
                            }
                        }
                    }
                    SwarmEvent::Behaviour(ChatBehaviourEvent::BarterProtocol(event)) => match event {
                        request_response::Event::Message { peer, message, .. } => match message {
                            Message::Request { request, channel, .. } => match request {
                                BarterNegotiationRequest::ProposeBarterSession => {
                                    // Store peer information for when user accepts
                                    let peer_name = self.connected_peers.get(&peer)
                                        .cloned()
                                        .unwrap_or_else(|| "Unknown User".to_string());

                                    println!("\n💰  Direct barter negotiation request from {} ({})", peer_name, peer);
                                    println!("    Type '/accept' to barter with this peer, or ignore to decline\n");

                                    // Store this pending request along with the response channel
                                    self.pending_barter_request = Some((peer.to_string(), peer_name));
                                    self.pending_barter_channel = Some(channel);

                                    // We'll send the response when the user accepts via "/accept" command
                                    // Don't send the response now
                                }
                            },
                            Message::Response { response, .. } => match response {
                                BarterNegotiationResponse::Accept { addrs } => {
                                    println!("\n✅  Barter request accepted!");
                                    println!("🔄 Press Enter to join the barter session...");

                                    // Wait for the user to press Enter before transitioning
                                    if let Ok(Some(_)) = stdin.next_line().await {
                                        println!("🔄 Entering barter session...");
                                    }

                                    // Return to transition to barter mode
                                    return Ok(ChatRoomExit::AcceptBarter {
                                        peer_id: peer.to_string(),
                                        addr: Some(addrs.join(","))
                                    });
                                },
                                BarterNegotiationResponse::Decline => {
                                    println!("\n❌  Barter request was declined\n");
                                }
                            }
                        },
                        _ => {} // Ignore other request-response events
                    },
                    _ => {}
                }
            }
        }
    }
}

// Need to import this for message ID generation
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

// Utility function to extract port from multiaddr
fn extract_port_from_multiaddr(addr: &Multiaddr) -> Option<u16> {
    use libp2p::multiaddr::Protocol;

    for proto in addr.iter() {
        if let Protocol::Tcp(port) = proto {
            return Some(port);
        }
    }

    None
}
