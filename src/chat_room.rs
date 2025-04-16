use clap::Parser;
use futures::StreamExt;
use libp2p::{
    Multiaddr, PeerId, gossipsub, identity, noise,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, yamux,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    error::Error,
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, BufReader, stdin},
    select,
};

const CHAT_TOPIC: &str = "p2p_file_barter_chat";

#[derive(Parser, Debug)]
#[clap(name = "libp2p chat room")]
pub struct ChatCliArgs {
    #[arg(long)]
    pub port: Option<String>,

    #[arg(long)]
    pub peer: Option<libp2p::Multiaddr>,
}

#[derive(NetworkBehaviour)]
pub struct ChatBehaviour {
    gossipsub: gossipsub::Behaviour,
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
}

#[derive(Debug, Serialize, Deserialize)]
pub enum PresenceAction {
    Join,
    Leave,
}

pub struct ChatRoom {
    swarm: libp2p::Swarm<ChatBehaviour>,
    connected_peers: HashMap<PeerId, String>, // PeerId -> Username
    user_name: String,
    chat_topic: gossipsub::IdentTopic,
    last_heartbeat: std::time::Instant,
    pending_barter_request: Option<(String, String)>,
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

        // Build the swarm
        let mut swarm = libp2p::SwarmBuilder::with_existing_identity(id_keys)
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

                ChatBehaviour { gossipsub }
            })?
            .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(7200)))
            .build();

        // Subscribe to the chat topic
        swarm.behaviour_mut().gossipsub.subscribe(&chat_topic)?;

        // Listen on the given port or a random one
        let listen_port = args.port.unwrap_or("0".to_string());
        let multiaddr = format!("/ip4/0.0.0.0/tcp/{listen_port}");
        swarm.listen_on(multiaddr.parse()?)?;

        // If the user supplied a peer to connect to, connect to it
        if let Some(peer) = args.peer {
            swarm.dial(peer)?;
        }

        Ok(ChatRoom {
            swarm,
            connected_peers: HashMap::new(),
            user_name: username,
            chat_topic,
            last_heartbeat: std::time::Instant::now(),
            pending_barter_request: None,
        })
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
        self.swarm
            .behaviour_mut()
            .gossipsub
            .publish(self.chat_topic.clone(), serialized.as_bytes())?;

        Ok(())
    }

    // Request to barter with a specific peer
    pub async fn request_barter_with(&mut self, peer_id: String) -> Result<(), Box<dyn Error>> {
        let barter_msg = ChatMessage::BarterRequest {
            from_id: self.swarm.local_peer_id().to_string(),
            from_name: self.user_name.clone(),
            to_id: peer_id,
        };

        let serialized = serde_json::to_string(&barter_msg)?;
        self.swarm
            .behaviour_mut()
            .gossipsub
            .publish(self.chat_topic.clone(), serialized.as_bytes())?;

        Ok(())
    }

    // Get list of peers in the chat
    pub fn get_chat_peers(&self) -> Vec<(String, String)> {
        self.connected_peers
            .iter()
            .map(|(peer_id, username)| (peer_id.to_string(), username.clone()))
            .collect()
    }

    // Run the chat room, handle events
    pub async fn run(&mut self) -> Result<(), Box<dyn Error>> {
        // First announce our presence
        let _ = self.announce_presence().await;

        let mut stdin = BufReader::new(stdin()).lines();

        let heartbeat_interval = Duration::from_secs(30);

        println!("\n===== P2P Chat Room =====\n");
        println!("Commands:");
        println!("  /help - Show available commands");
        println!("  /peers - Show connected peers");
        println!("  /barter <peer_id> - Request to barter with a peer");
        println!("  /quit - Leave the chat room");
        println!("  Just type to send a message to everyone\n");

        loop {
            let now = std::time::Instant::now();
            if now.duration_since(self.last_heartbeat) > heartbeat_interval {
                let _ = self.announce_presence().await;
                self.last_heartbeat = now;
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
                                if self.connected_peers.values().any(|u| u == peer_id) ||
                                   self.connected_peers.keys().any(|k| k.to_string() == peer_id) {
                                    println!("\n📤  Sending barter request to {}\n", peer_id);
                                    self.request_barter_with(peer_id.to_string()).await?;
                                } else {
                                    println!("\n⚠️  Peer {} not found in chat room\n", peer_id);
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

                                println!("\n👋  Leaving chat room...\n");
                                break;
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
                    SwarmEvent::Behaviour(ChatBehaviourEvent::Gossipsub(gossipsub::Event::Message {
                        propagation_source: source,
                        message_id,
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
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        Ok(())
    }
}

// Need to import this for message ID generation
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
