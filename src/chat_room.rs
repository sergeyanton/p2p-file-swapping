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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DiscoveryRequest {
    Register {
        username: String,
        addrs: Vec<String>,
    },
    GetPeers,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DiscoveryResponse {
    Registered,
    Peers { peers: Vec<PeerInfo> },
    Error(String),
}

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
    pub discovery_protocol: request_response::cbor::Behaviour<DiscoveryRequest, DiscoveryResponse>,
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

        // Configure request-response for discovery protocol
        let discovery_protocols = vec![(
            StreamProtocol::new("/discovery/1.0.0"),
            ProtocolSupport::Full,
        )];
        let discovery_config = RpConfig::default().with_request_timeout(Duration::from_secs(10));

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

                // Set up discovery protocol
                let discovery_protocol =
                    request_response::cbor::Behaviour::new(discovery_protocols, discovery_config);

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
                    discovery_protocol,
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

            // Print all listening addresses before registration
            println!("🔍 [DEBUG] Client listening addresses before registration:");
            for addr in self.swarm.listeners() {
                println!("    {}", addr);
            }

            // Create a namespace for the chat room
            if let Ok(namespace) = rendezvous::Namespace::new(RENDEZVOUS_NAMESPACE.to_string()) {
                // Register with the rendezvous server
                let result = self.swarm.behaviour_mut().rendezvous.register(
                    namespace.clone(),
                    server_id,
                    None,
                );

                match &result {
                    Ok(()) => println!("✅ Registered with rendezvous server"),
                    Err(e) => {
                        println!("⚠️ Failed to register with rendezvous server: {}", e);
                        // Optionally, print more debug info here
                    }
                }

                // Print the addresses again after registration attempt
                println!("🔍 [DEBUG] Client listening addresses after registration attempt:");
                for addr in self.swarm.listeners() {
                    println!("    {}", addr);
                }

                // Start peer discovery
                self.swarm.behaviour_mut().rendezvous.discover(
                    Some(namespace),
                    None,
                    None,
                    server_id,
                );

                println!("🔍 Discovering peers via rendezvous server");
            } else {
                println!("⚠️ Failed to create namespace for rendezvous");
            }
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

                self.swarm.behaviour_mut().rendezvous.discover(
                    Some(namespace),
                    None,
                    None,
                    server_id,
                );

                println!("🔍 Started peer discovery via rendezvous");
            } else {
                println!("⚠️ Failed to create namespace for rendezvous");
            }
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
            if listening {
                // Register with rendezvous server if available and not yet registered
                if !rendezvous_registered && self.rendezvous_server.is_some() {
                    if let Some(server_id) = self.rendezvous_server {
                        // Try to dial the rendezvous server if not already connected
                        match self
                            .swarm
                            .dial(format!("/p2p/{}", server_id).parse::<Multiaddr>()?)
                        {
                            Ok(_) => println!("🔄 Connecting to rendezvous server: {}", server_id),
                            Err(e) => println!("⚠️ Failed to dial rendezvous server: {}", e),
                        }

                        // Wait a moment for the connection to establish
                        tokio::time::sleep(Duration::from_millis(1000)).await;

                        // Register with the native libp2p rendezvous protocol
                        if let Err(e) = self.register_with_rendezvous().await {
                            println!("⚠️ Failed to register with rendezvous server: {}", e);
                        } else {
                            rendezvous_registered = true;
                            println!("✅ Successfully registered with rendezvous server");
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
                        if !registration_attempted {
                            registration_attempted = true;
                            // Wait 2 seconds to allow all addresses to be discovered
                            tokio::time::sleep(Duration::from_secs(2)).await;
                            // Print all addresses before registration
                            println!("[DEBUG] All client listening addresses before registration:");
                            for addr in self.swarm.listeners() {
                                println!("    {}", addr);
                            }
                            // Register with rendezvous server if available and not yet registered
                            if !rendezvous_registered && self.rendezvous_server.is_some() {
                                if let Some(server_id) = self.rendezvous_server {
                                    // Try to dial the rendezvous server if not already connected
                                    match self
                                        .swarm
                                        .dial(format!("/p2p/{}", server_id).parse::<Multiaddr>()?)
                                    {
                                        Ok(_) => println!("🔄 Connecting to rendezvous server: {}", server_id),
                                        Err(e) => println!("⚠️ Failed to dial rendezvous server: {}", e),
                                    }

                                    // Wait a moment for the connection to establish
                                    tokio::time::sleep(Duration::from_millis(1000)).await;

                                    // Register with the native libp2p rendezvous protocol
                                    if let Err(e) = self.register_with_rendezvous().await {
                                        println!("⚠️ Failed to register with rendezvous server: {}", e);
                                    } else {
                                        rendezvous_registered = true;
                                        println!("✅ Successfully registered with rendezvous server");
                                    }
                                }
                            }
                        }
                    }
                    SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                        println!("\n🔗  Connection established to {:?}\n", peer_id);
                        let _ = self.announce_presence().await;
                        if !self.connected_peers.contains_key(&peer_id) {
                            self.connected_peers.insert(peer_id, "Connected (awaiting username)".to_string());
                        }
                    }
                    SwarmEvent::ConnectionClosed { peer_id, .. } => {
                        if let Some(username) = self.connected_peers.remove(&peer_id) {
                            println!("\n❌  {} ({:?}) has left the chat room\n", username, peer_id);
                        } else {
                            println!("\n❌  Connection closed with {:?}\n", peer_id);
                        }
                    }
                    SwarmEvent::Behaviour(ChatBehaviourEvent::Rendezvous(event)) => match event {
                        rendezvous::client::Event::Discovered { registrations, .. } => {
                            println!("\n🔍 Discovered {} peers via rendezvous", registrations.len());

                            for registration in registrations {
                                // Skip ourselves
                                if registration.record.peer_id() == *self.swarm.local_peer_id() {
                                    continue;
                                }

                                println!("   - Found peer: {}", registration.record.peer_id());

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
                        rendezvous::client::Event::DiscoverFailed { error, .. } => {
                            println!("\n⚠️ Discovery failed: {:?}\n", error);
                        },
                        _ => {}  // Handle other rendezvous events if needed
                    },
                    SwarmEvent::Behaviour(ChatBehaviourEvent::DiscoveryProtocol(e)) => match e {
                        request_response::Event::Message { peer, message, .. } => {
                            match message {
                                request_response::Message::Response { response, .. } => {
                                    match response {
                                        DiscoveryResponse::Registered => {
                                            println!("\n✅  Successfully registered with discovery server\n");
                                        },
                                        DiscoveryResponse::Peers { peers } => {
                                            println!("\n🔍  Received list of {} peers from discovery server", peers.len());

                                            for peer_info in peers {
                                                // Skip ourselves
                                                if peer_info.peer_id == self.swarm.local_peer_id().to_string() {
                                                    continue;
                                                }

                                                println!("   - Found peer: {} ({})", peer_info.username, peer_info.peer_id);

                                                // Parse the peer ID
                                                if let Ok(peer_id) = peer_info.peer_id.parse::<PeerId>() {
                                                    // Parse the addresses
                                                    let mut addrs = Vec::new();
                                                    for addr_str in &peer_info.addrs {
                                                        if let Ok(addr) = addr_str.parse::<Multiaddr>() {
                                                            addrs.push(addr);
                                                        }
                                                    }

                                                    // Store the peer and its addresses
                                                    self.discovered_peers.insert(peer_id, addrs.clone());

                                                    // Try to connect if we're not already connected
                                                    if !self.connected_peers.contains_key(&peer_id) {
                                                        for addr in addrs {
                                                            match self.swarm.dial(addr.clone()) {
                                                                Ok(_) => {
                                                                    println!("   - Dialing peer {} at {}", peer_id, addr);
                                                                    break; // Just try one address
                                                                },
                                                                Err(e) => {
                                                                    println!("   - Failed to dial {}: {}", addr, e);
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                            println!();
                                        },
                                        DiscoveryResponse::Error(msg) => {
                                            println!("\n⚠️  Error from discovery server: {}\n", msg);
                                        }
                                    }
                                },
                                _ => {}
                            }
                        },
                        _ => {}
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
                                            // Hybrid apporach for discovery, better safe than sorry. Discover when someone leaves/joins AND every 5 minutes
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
                            Message::Response { request_id: _, response } => match response {
                                BarterNegotiationResponse::Accept { addrs } => {
                                    println!("\n✅  Barter request accepted!");
                                    println!("[DEBUG] Received barter addresses: {:?}", addrs);

                                    // Join the addresses into a comma-separated string
                                    let addr_str = addrs.join(",");

                                    // Return AcceptBarter with peer_id and addresses
                                    return Ok(ChatRoomExit::AcceptBarter {
                                        peer_id: peer.to_string(),
                                        addr: Some(addr_str)
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
