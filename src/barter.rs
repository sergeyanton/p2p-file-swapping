use clap::Parser;
use futures::StreamExt;
use libp2p::{
    Multiaddr, PeerId, StreamProtocol, noise,
    request_response::{self, *},
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, yamux,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, error::Error, io, time::Duration};
use tokio::{
    fs::File,
    io::{self as other_io, AsyncBufReadExt, AsyncReadExt, BufReader, stdin},
    select,
};

#[derive(Parser, Debug)]
#[clap(name = "libp2p file bartering app")]
struct Cli {
    #[arg(long)]
    pub port: Option<String>,

    #[arg(long)]
    pub peer: Option<Multiaddr>,
}

#[derive(NetworkBehaviour)]
struct BarterBehaviour {
    barter_protocol: request_response::cbor::Behaviour<BarterMessage, BarterResponse>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BarterMessage {
    BarterRequest {
        offering: String,   // The file A offers to give
        requesting: String, // The file A wants from B
    },
    FileDelivery {
        filename: String,   // Original filename for reference
        file_data: Vec<u8>, // The file's contents
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BarterResponse {
    Accepted(Vec<u8>), // Accepted with the requested file contents
    Declined,          // Request declined
    FileReceived,      // Acknowledgment that file was received
}

// For tracking pending requests
struct PendingBarter {
    offering: String,   // The file we're offering
    requesting: String, // The file we're requesting
}

pub async fn run_barter_mode(
    port: Option<String>,
    peer: Option<Multiaddr>,
) -> Result<(), Box<dyn Error>> {
    let mut swarm = libp2p::SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_behaviour(|_key| BarterBehaviour {
            barter_protocol: request_response::cbor::Behaviour::new(
                [(
                    StreamProtocol::new("/barter-protocol/1"),
                    ProtocolSupport::Full,
                )],
                request_response::Config::default(),
            ),
        })?
        .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(Duration::from_secs(7200)))
        .build();

    let listen_port = port.unwrap_or("0".to_string());
    let multiaddr = format!("/ip4/0.0.0.0/tcp/{listen_port}");
    let _ = swarm.listen_on(multiaddr.parse()?)?;

    if let Some(ref peer_addr) = peer {
        match swarm.dial(peer_addr.clone()) {
            Ok(_) => println!("Dial attempt initiated successfully"),
            Err(e) => println!("Error initiating dial: {:?}", e),
        }
    }

    let mut stdin = BufReader::new(stdin()).lines();
    let mut connected_peers: Vec<PeerId> = Vec::new();
    let mut pending_barters: HashMap<PeerId, PendingBarter> = HashMap::new();
    let mut pending_requests: HashMap<PeerId, request_response::ResponseChannel<BarterResponse>> =
        HashMap::new();

    println!("\n===== File Bartering CLI =====\n");
    println!("Commands:");
    println!("  /barter <offering_file> <requesting_file> - Propose a file barter");
    println!("  /accept - Accept the latest barter proposal");
    println!("  /decline - Decline the latest barter proposal");
    println!("  /peers - List connected peers");
    println!("  /help - Show this help\n");

    loop {
        select! {
            Ok(Some(line)) = stdin.next_line() => {
                if !line.starts_with('/') {
                    println!("\n⚠️  Commands must start with '/' (e.g., /help)\n");
                    continue;
                }

                let parts: Vec<&str> = line.split_whitespace().collect();

                if parts.is_empty() {
                    continue;
                }

                match parts[0] {
                    "/barter" => {
                        if parts.len() < 3 {
                            println!("\n⚠️  Usage: /barter <offering_file> <requesting_file>\n");
                            continue;
                        }
                        // Check if we have any connected peers
                        if connected_peers.is_empty() {
                            println!("\n⚠️  No peers connected. Wait for a connection or connect to a peer.\n");
                            continue;
                        }

                        let offering = parts[1].to_string();
                        let requesting = parts[2].to_string();

                        // Check if offering file exists
                        if tokio::fs::metadata(&offering).await.is_err() {
                            println!("\n⚠️  File '{}' does not exist or cannot be accessed\n", offering);
                            continue;
                        }

                        let peer_id = connected_peers[0]; // Use the first connected peer for simplicity
                        let request = BarterMessage::BarterRequest {
                            offering: offering.clone(),
                            requesting: requesting.clone(),
                        };

                        // Track our outbound request
                        pending_barters.insert(peer_id, PendingBarter {
                            offering,
                            requesting,
                        });

                        println!("\n📤  Sending barter request to {:?}\n", peer_id);
                        swarm.behaviour_mut().barter_protocol.send_request(&peer_id, request);
                    },
                    "/accept" => {
                        if pending_requests.is_empty() {
                            println!("\n⚠️  No pending barter requests to accept\n");
                            continue;
                        }

                        for (peer_id, channel) in pending_requests.drain() {
                            if let Some(barter) = pending_barters.get(&peer_id) {
                                let filename = &barter.requesting; // What they're requesting from us
                                println!("\n🤝  Accepting barter request from {:?}...", peer_id);

                                // Read the file they want
                                match File::open(filename).await {
                                    Ok(mut file) => {
                                        let mut buffer = Vec::new();
                                        match file.read_to_end(&mut buffer).await {
                                            Ok(_) => {
                                                println!("📂  Read {} bytes from {}", buffer.len(), filename);
                                                println!("📤  Sending file as acceptance response...\n");

                                                // Send acceptance with the file
                                                let _  = swarm.behaviour_mut().barter_protocol.send_response(
                                                    channel,
                                                    BarterResponse::Accepted(buffer)
                                                );
                                            },
                                            Err(e) => {
                                                eprintln!("\n❌  Failed to read file: {}\n", e);
                                                println!("❌  Declining barter due to file read error\n");
                                                let _  = swarm.behaviour_mut().barter_protocol.send_response(
                                                    channel,
                                                    BarterResponse::Declined
                                                );
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        eprintln!("\n❌  Failed to open file {}: {}\n", filename, e);
                                        println!("❌  Declining barter because file not found\n");
                                        let _  = swarm.behaviour_mut().barter_protocol.send_response(
                                            channel,
                                            BarterResponse::Declined
                                        );
                                    }
                                };
                            }
                        }
                    },
                    "/decline" => {
                        if pending_requests.is_empty() {
                            println!("\n⚠️  No pending barter requests to decline\n");
                            continue;
                        }

                        for (peer_id, channel) in pending_requests.drain() {
                            println!("\n❌  Declining barter request from {:?}\n", peer_id);
                            let _ = swarm.behaviour_mut().barter_protocol.send_response(
                                channel,
                                BarterResponse::Declined
                            );
                        }
                    },
                    "/peers" => {
                        if connected_peers.is_empty() {
                            println!("\n👥  No peers connected\n");
                        } else {
                            println!("\n👥  Connected peers:");
                            for (i, peer) in connected_peers.iter().enumerate() {
                                println!("    {}. {:?}", i+1, peer);
                            }
                            println!();
                        }
                    },
                    "/help" => {
                        println!("\n===== Commands =====");
                        println!("  /barter <offering_file> <requesting_file> - Propose a file barter");
                        println!("  /accept - Accept the latest barter proposal");
                        println!("  /decline - Decline the latest barter proposal");
                        println!("  /peers - List connected peers");
                        println!("  /help - Show this help\n");
                    },
                    _ => println!("\n⚠️  Unknown command. Type '/help' for available commands.\n"),
                }
            }
            event = swarm.select_next_some() => match event {
                SwarmEvent::NewListenAddr { address, .. } => {
                    println!("📡  Listening on {:?}", address);
                }
                SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                    connected_peers.push(peer_id);
                    println!("\n🔗  Established connection to {:?}\n", peer_id);
                }
                SwarmEvent::ConnectionClosed { peer_id, .. } => {
                    connected_peers.retain(|&p| p != peer_id);
                    pending_barters.remove(&peer_id);
                    pending_requests.remove(&peer_id);
                    println!("\n❌  Connection closed with {:?}\n", peer_id);
                }
                SwarmEvent::Behaviour(BarterBehaviourEvent::BarterProtocol(event)) => match event {
                    request_response::Event::Message { peer, message, .. } => match message {
                        request_response::Message::Request { request, channel, .. } => match request {
                            BarterMessage::BarterRequest { offering, requesting } => {
                                println!("\n📥  Received barter request from {:?}:", peer);
                                println!("    They offer: '{}'", offering);
                                println!("    They want:  '{}'", requesting);

                                // Store the request details and channel
                                pending_barters.insert(
                                    peer,
                                    PendingBarter {
                                        offering: offering.to_string(),     // What they're offering us
                                        requesting: requesting.to_string(), // What they want from us
                                    },
                                );
                                pending_requests.insert(peer, channel);

                                println!("\n💬  Type '/accept' to accept the barter or '/decline' to reject it\n");
                            },
                            BarterMessage::FileDelivery { filename, file_data } => {
                                println!("\n📥  Received file delivery from {:?}:", peer);
                                println!("    Filename: {}", filename);
                                println!("    Size: {} bytes", file_data.len());

                                // Save the received file
                                let received_filename = format!("received_{}", filename);
                                match tokio::fs::write(&received_filename, &file_data).await {
                                    Ok(_) => {
                                        println!("💾  Saved received file to {}", received_filename);
                                        println!("📤  Sending file receipt confirmation...\n");
                                        let _ = swarm.behaviour_mut().barter_protocol.send_response(
                                            channel,
                                            BarterResponse::FileReceived
                                        );
                                    },
                                    Err(e) => {
                                        eprintln!("\n❌  Failed to save file: {}\n", e);
                                        let _ = swarm.behaviour_mut().barter_protocol.send_response(
                                            channel,
                                            BarterResponse::Declined
                                        );
                                    }
                                }
                            }
                        },
                        request_response::Message::Response { response, .. } => {
                            match response {
                                BarterResponse::Accepted(file_bytes) => {
                                    println!("\n🎉  Barter accepted by {:?}!", peer);
                                    println!("📥  Received {} bytes of file data", file_bytes.len());

                                    // Get the details of our request
                                    if let Some(barter) = pending_barters.get(&peer) {
                                        // Save the received file
                                        let received_filename = format!("received_{}", barter.requesting);
                                        match tokio::fs::write(&received_filename, &file_bytes).await {
                                            Ok(_) => println!("💾  Saved received file to {}", received_filename),
                                            Err(e) => eprintln!("❌  Failed to save file: {}", e),
                                        }

                                        // Now send the file we offered
                                        println!("📂  Reading our offering file: {}", barter.offering);
                                        match File::open(&barter.offering).await {
                                            Ok(mut file) => {
                                                let mut buffer = Vec::new();
                                                match file.read_to_end(&mut buffer).await {
                                                    Ok(_) => {
                                                        println!("📤  Sending our offered file ({} bytes)...\n", buffer.len());

                                                        // Send the file as a FileDelivery message
                                                        let delivery = BarterMessage::FileDelivery {
                                                            filename: barter.offering.clone(),
                                                            file_data: buffer,
                                                        };

                                                        swarm.behaviour_mut().barter_protocol.send_request(&peer, delivery);
                                                    },
                                                    Err(e) => eprintln!("\n❌  Failed to read our offering file: {}\n", e),
                                                }
                                            }
                                            Err(e) => eprintln!("\n❌  Failed to open our offering file: {}\n", e),
                                        }
                                    }
                                }
                                BarterResponse::Declined => {
                                    println!("\n❌  Barter declined by {:?}.\n", peer);
                                    pending_barters.remove(&peer);
                                }
                                BarterResponse::FileReceived => {
                                    println!("\n✅  Peer {:?} confirmed receipt of our file.", peer);
                                    println!("🎉  Barter completed successfully!\n");
                                    pending_barters.remove(&peer);
                                }
                            }
                        }
                    },
                    _ => {}
                }
                _ => {}
            }
        }
    }
}

pub async fn run_barter_mode_with_keypair(
    port: Option<String>,
    peer: Option<Multiaddr>,
    keypair: libp2p::identity::Keypair,
) -> Result<(), Box<dyn Error>> {
    // Create the swarm with our keypair
    let mut swarm = libp2p::SwarmBuilder::with_existing_identity(keypair)
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_behaviour(|_key| BarterBehaviour {
            barter_protocol: request_response::cbor::Behaviour::new(
                [(
                    StreamProtocol::new("/barter-protocol/1"),
                    ProtocolSupport::Full,
                )],
                request_response::Config::default(),
            ),
        })?
        .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(Duration::from_secs(7200)))
        .build();

    // Listen on the given port
    let listen_port = port.clone().unwrap_or("0".to_string());
    let multiaddr = format!("/ip4/0.0.0.0/tcp/{listen_port}");
    swarm.listen_on(multiaddr.parse()?)?;

    // Connect to peer if provided
    if let Some(ref peer_addr) = peer {
        if let Err(e) = swarm.dial(peer_addr.clone()) {
            println!("❌ Failed to dial peer: {:?}", e);
        }
    }

    let mut stdin = BufReader::new(stdin()).lines();
    let mut connected_peers: Vec<PeerId> = Vec::new();
    let mut pending_barters: HashMap<PeerId, PendingBarter> = HashMap::new();
    let mut pending_requests: HashMap<PeerId, request_response::ResponseChannel<BarterResponse>> =
        HashMap::new();

    println!("\n===== File Bartering CLI =====\n");
    println!("Commands:");
    println!("  /barter <offering_file> <requesting_file> - Propose a file barter");
    println!("  /accept - Accept the latest barter proposal");
    println!("  /decline - Decline the latest barter proposal");
    println!("  /peers - List connected peers");
    println!("  /help - Show this help\n");

    loop {
        select! {
            Ok(Some(line)) = stdin.next_line() => {
                if !line.starts_with('/') {
                    println!("\n⚠️  Commands must start with '/' (e.g., /help)\n");
                    continue;
                }

                let parts: Vec<&str> = line.split_whitespace().collect();

                if parts.is_empty() {
                    continue;
                }

                match parts[0] {
                    "/barter" => {
                        if parts.len() < 3 {
                            println!("\n⚠️  Usage: /barter <offering_file> <requesting_file>\n");
                            continue;
                        }
                        // Check if we have any connected peers
                        if connected_peers.is_empty() {
                            println!("\n⚠️  No peers connected. Wait for a connection or connect to a peer.\n");
                            continue;
                        }

                        let offering = parts[1].to_string();
                        let requesting = parts[2].to_string();

                        // Check if offering file exists
                        if tokio::fs::metadata(&offering).await.is_err() {
                            println!("\n⚠️  File '{}' does not exist or cannot be accessed\n", offering);
                            continue;
                        }

                        let peer_id = connected_peers[0]; // Use the first connected peer for simplicity
                        let request = BarterMessage::BarterRequest {
                            offering: offering.clone(),
                            requesting: requesting.clone(),
                        };

                        // Track our outbound request
                        pending_barters.insert(peer_id, PendingBarter {
                            offering,
                            requesting,
                        });

                        println!("\n📤  Sending barter request to {:?}\n", peer_id);
                        swarm.behaviour_mut().barter_protocol.send_request(&peer_id, request);
                    },
                    "/accept" => {
                        if pending_requests.is_empty() {
                            println!("\n⚠️  No pending barter requests to accept\n");
                            continue;
                        }

                        for (peer_id, channel) in pending_requests.drain() {
                            if let Some(barter) = pending_barters.get(&peer_id) {
                                let filename = &barter.requesting; // What they're requesting from us
                                println!("\n🤝  Accepting barter request from {:?}...", peer_id);

                                // Read the file they want
                                match File::open(filename).await {
                                    Ok(mut file) => {
                                        let mut buffer = Vec::new();
                                        match file.read_to_end(&mut buffer).await {
                                            Ok(_) => {
                                                println!("📂  Read {} bytes from {}", buffer.len(), filename);
                                                println!("📤  Sending file as acceptance response...\n");

                                                // Send acceptance with the file
                                                let _  = swarm.behaviour_mut().barter_protocol.send_response(
                                                    channel,
                                                    BarterResponse::Accepted(buffer)
                                                );
                                            },
                                            Err(e) => {
                                                eprintln!("\n❌  Failed to read file: {}\n", e);
                                                println!("❌  Declining barter due to file read error\n");
                                                let _  = swarm.behaviour_mut().barter_protocol.send_response(
                                                    channel,
                                                    BarterResponse::Declined
                                                );
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        eprintln!("\n❌  Failed to open file {}: {}\n", filename, e);
                                        println!("❌  Declining barter because file not found\n");
                                        let _  = swarm.behaviour_mut().barter_protocol.send_response(
                                            channel,
                                            BarterResponse::Declined
                                        );
                                    }
                                };
                            }
                        }
                    },
                    "/decline" => {
                        if pending_requests.is_empty() {
                            println!("\n⚠️  No pending barter requests to decline\n");
                            continue;
                        }

                        for (peer_id, channel) in pending_requests.drain() {
                            println!("\n❌  Declining barter request from {:?}\n", peer_id);
                            let _ = swarm.behaviour_mut().barter_protocol.send_response(
                                channel,
                                BarterResponse::Declined
                            );
                        }
                    },
                    "/peers" => {
                        if connected_peers.is_empty() {
                            println!("\n👥  No peers connected\n");
                        } else {
                            println!("\n👥  Connected peers:");
                            for (i, peer) in connected_peers.iter().enumerate() {
                                println!("    {}. {:?}", i+1, peer);
                            }
                            println!();
                        }
                    },
                    "/help" => {
                        println!("\n===== Commands =====");
                        println!("  /barter <offering_file> <requesting_file> - Propose a file barter");
                        println!("  /accept - Accept the latest barter proposal");
                        println!("  /decline - Decline the latest barter proposal");
                        println!("  /peers - List connected peers");
                        println!("  /help - Show this help\n");
                    },
                    _ => println!("\n⚠️  Unknown command. Type '/help' for available commands.\n"),
                }
            },
            event = swarm.select_next_some() => match event {
                SwarmEvent::NewListenAddr { address, .. } => {
                    println!("\n📡  Listening on {:?}\n", address);
                }
                SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                    connected_peers.push(peer_id);
                    println!("\n🔗  Established connection to {:?}\n", peer_id);
                }
                SwarmEvent::ConnectionClosed { peer_id, .. } => {
                    connected_peers.retain(|&p| p != peer_id);
                    pending_barters.remove(&peer_id);
                    pending_requests.remove(&peer_id);
                    println!("\n❌  Connection closed with {:?}\n", peer_id);
                }
                SwarmEvent::Behaviour(BarterBehaviourEvent::BarterProtocol(event)) => match event {
                    request_response::Event::Message { peer, message, .. } => match message {
                        request_response::Message::Request {
                            request, channel, ..
                        } => match request {
                            BarterMessage::BarterRequest {
                                offering,
                                requesting,
                            } => {
                                println!("\n📥  Received barter request from {:?}:", peer);
                                println!("    They offer: '{}'", offering);
                                println!("    They want:  '{}'", requesting);

                                // Store the request details and channel
                                pending_barters.insert(
                                    peer,
                                    PendingBarter {
                                        offering: offering.to_string(),     // What they're offering us
                                        requesting: requesting.to_string(), // What they want from us
                                    },
                                );
                                pending_requests.insert(peer, channel);

                                println!(
                                    "\n💬  Type '/accept' to accept the barter or '/decline' to reject it\n"
                                );
                            }
                            BarterMessage::FileDelivery {
                                filename,
                                file_data,
                            } => {
                                println!("\n📥  Received file delivery from {:?}:", peer);
                                println!("    Filename: {}", filename);
                                println!("    Size: {} bytes", file_data.len());
                                // Save the received file
                                let received_filename = format!("received_{}", filename);
                                match tokio::fs::write(&received_filename, &file_data).await {
                                    Ok(_) => {
                                        println!(
                                            "💾  Saved received file to {}",
                                            received_filename
                                        );
                                        println!("📤  Sending file receipt confirmation...\n");
                                        let _ = swarm
                                            .behaviour_mut()
                                            .barter_protocol
                                            .send_response(
                                                channel,
                                                BarterResponse::FileReceived,
                                            );
                                    }
                                    Err(e) => {
                                        eprintln!("\n❌  Failed to save file: {}\n", e);
                                        let _ = swarm
                                            .behaviour_mut()
                                            .barter_protocol
                                            .send_response(channel, BarterResponse::Declined);
                                    }
                                }
                            }
                        },
                        request_response::Message::Response { response, .. } => {
                            match response {
                                BarterResponse::Accepted(file_bytes) => {
                                    println!("\n🎉  Barter accepted by {:?}!", peer);
                                    println!(
                                        "📥  Received {} bytes of file data",
                                        file_bytes.len()
                                    );

                                    // Get the details of our request
                                    if let Some(barter) = pending_barters.get(&peer) {
                                        // Save the received file
                                        let received_filename =
                                            format!("received_{}", barter.requesting);
                                        match tokio::fs::write(&received_filename, &file_bytes)
                                            .await
                                        {
                                            Ok(_) => println!(
                                                "💾  Saved received file to {}",
                                                received_filename
                                            ),
                                            Err(e) => {
                                                eprintln!("❌  Failed to save file: {}", e)
                                            }
                                        }

                                        // Now send the file we offered
                                        println!(
                                            "📂  Reading our offering file: {}",
                                            barter.offering
                                        );
                                        match File::open(&barter.offering).await {
                                            Ok(mut file) => {
                                                let mut buffer = Vec::new();
                                                match file.read_to_end(&mut buffer).await {
                                                    Ok(_) => {
                                                        println!(
                                                            "📤  Sending our offered file ({} bytes)...\n",
                                                            buffer.len()
                                                        );

                                                        // Send the file as a FileDelivery message
                                                        let delivery =
                                                            BarterMessage::FileDelivery {
                                                                filename: barter
                                                                    .offering
                                                                    .clone(),
                                                                file_data: buffer,
                                                            };

                                                        swarm
                                                            .behaviour_mut()
                                                            .barter_protocol
                                                            .send_request(&peer, delivery);
                                                    }
                                                    Err(e) => eprintln!(
                                                        "\n❌  Failed to read our offering file: {}\n",
                                                        e
                                                    ),
                                                }
                                            }
                                            Err(e) => eprintln!(
                                                "\n❌  Failed to open our offering file: {}\n",
                                                e
                                            ),
                                        }
                                    }
                                }
                                BarterResponse::Declined => {
                                    println!("\n❌  Barter declined by {:?}.\n", peer);
                                    pending_barters.remove(&peer);
                                }
                                BarterResponse::FileReceived => {
                                    println!(
                                        "\n✅  Peer {:?} confirmed receipt of our file.",
                                        peer
                                    );
                                    println!("🎉  Barter completed successfully!\n");
                                    pending_barters.remove(&peer);
                                }
                            }
                        }
                    },
                    _ => {}
                },
                _ => {}
            }
        }
    }
}
