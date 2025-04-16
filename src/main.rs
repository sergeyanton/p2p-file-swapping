use clap::Parser;
use futures::StreamExt;
use libp2p::{
    Multiaddr, PeerId, StreamProtocol, noise,
    request_response::{self, ProtocolSupport},
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, yamux,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, error::Error, time::Duration};
use tokio::{
    fs::File,
    io::{self, AsyncBufReadExt, AsyncReadExt, BufReader, stdin},
    select,
};

#[derive(Parser, Debug)]
#[clap(name = "libp2p file bartering app")]
struct Cli {
    #[arg(long)]
    port: Option<String>,

    #[arg(long)]
    peer: Option<Multiaddr>,
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();

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

    let listen_port = cli.port.unwrap_or("0".to_string());
    let multiaddr = format!("/ip4/0.0.0.0/tcp/{listen_port}");
    let _ = swarm.listen_on(multiaddr.parse()?)?;

    if let Some(peer) = cli.peer {
        swarm.dial(peer)?;
    }

    let mut stdin = BufReader::new(stdin()).lines();
    let mut connected_peers: Vec<PeerId> = Vec::new();
    let mut pending_barters: HashMap<PeerId, PendingBarter> = HashMap::new();
    let mut pending_requests: HashMap<PeerId, request_response::ResponseChannel<BarterResponse>> =
        HashMap::new();

    println!("Commands:");
    println!("barter <offering_file> <requesting_file> - Propose a file barter");
    println!("accept - Accept the latest barter proposal");
    println!("decline - Decline the latest barter proposal");
    println!("peers - List connected peers");
    println!("help - Show this help");

    loop {
        select! {
            Ok(Some(line)) = stdin.next_line() => {
                let parts: Vec<&str> = line.split_whitespace().collect();

                if parts.is_empty() {
                    continue;
                }

                match parts[0] {
                    "barter" => {
                        if parts.len() < 3 {
                            println!("Usage: barter <offering_file> <requesting_file>");
                            continue;
                        }
                        if connected_peers.is_empty() {
                            println!("No peers connected. Wait for a connection or connect to a peer.");
                            continue;
                        }

                        let offering = parts[1].to_string();
                        let requesting = parts[2].to_string();

                        // Check if offering file exists
                        if tokio::fs::metadata(&offering).await.is_err() {
                            println!("File '{}' does not exist or cannot be accessed", offering);
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

                        println!("Sending barter request to {:?}", peer_id);
                        swarm.behaviour_mut().barter_protocol.send_request(&peer_id, request);
                    },
                    "accept" => {
                        for (peer_id, channel) in pending_requests.drain() {
                            if let Some(barter) = pending_barters.get(&peer_id) {
                                let filename = &barter.requesting; // What they're requesting from us

                                // Read the file they want
                                match File::open(filename).await {
                                    Ok(mut file) => {
                                        let mut buffer = Vec::new();
                                        match file.read_to_end(&mut buffer).await {
                                            Ok(_) => {
                                                println!("Read {} bytes from {}", buffer.len(), filename);
                                                println!("Accepting barter request and sending file");

                                                // Send acceptance with the file
                                                let _  = swarm.behaviour_mut().barter_protocol.send_response(
                                                    channel,
                                                    BarterResponse::Accepted(buffer)
                                                );
                                            },
                                            Err(e) => {
                                                eprintln!("Failed to read file: {}", e);
                                                println!("Declining barter due to file read error");
                                                let _  = swarm.behaviour_mut().barter_protocol.send_response(
                                                    channel,
                                                    BarterResponse::Declined
                                                );
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        eprintln!("Failed to open file {}: {}", filename, e);
                                        println!("Declining barter because file not found");
                                        let _  = swarm.behaviour_mut().barter_protocol.send_response(
                                            channel,
                                            BarterResponse::Declined
                                        );
                                    }
                                };
                            }
                        }

                        if pending_requests.is_empty() {
                            println!("No pending barter requests to accept");
                        }
                    },
                    "decline" => {
                        for (_, channel) in pending_requests.drain() {
                            println!("Declining barter request");
                            let _ = swarm.behaviour_mut().barter_protocol.send_response(
                                channel,
                                BarterResponse::Declined
                            );
                        }

                        if pending_requests.is_empty() {
                            println!("No pending barter requests to decline");
                        }
                    },
                    "peers" => {
                        println!("Connected peers: {:?}", connected_peers);
                    },
                    "help" => {
                        println!("\nCommands:");
                        println!("barter <offering_file> <requesting_file> - Propose a file barter");
                        println!("accept - Accept the latest barter proposal");
                        println!("decline - Decline the latest barter proposal");
                        println!("peers - List connected peers");
                        println!("help - Show this help\n");
                    },
                    _ => println!("Unknown command. Type 'help' for available commands."),
                }
            }
            event = swarm.select_next_some() => match event {
                SwarmEvent::NewListenAddr { address, .. } => {
                    println!("Listening on {:?}", address);
                }
                SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                    connected_peers.push(peer_id);
                    println!("\nEstablished connection to {:?}", peer_id);
                }
                SwarmEvent::ConnectionClosed { peer_id, .. } => {
                    connected_peers.retain(|&p| p != peer_id);
                    pending_barters.remove(&peer_id);
                    pending_requests.remove(&peer_id);
                    println!("Connection closed with {:?}", peer_id);
                }
                SwarmEvent::Behaviour(BarterBehaviourEvent::BarterProtocol(event)) => match event {
                    request_response::Event::Message { peer, message, .. } => match message {
                        request_response::Message::Request { request, channel, .. } => match request {
                            BarterMessage::BarterRequest { offering, requesting } => {
                                println!("Received barter request: peer offers '{}' and wants '{}'",
                                        offering, requesting);

                                // Store the request details and channel
                                pending_barters.insert(peer, PendingBarter {
                                    offering,    // What they're offering us
                                    requesting, // What they want from us
                                });
                                pending_requests.insert(peer, channel);

                                println!("Type 'accept' to accept the barter or 'decline' to reject it");
                            },
                            BarterMessage::FileDelivery { filename, file_data } => {
                                println!("Received file delivery: {} ({} bytes)", filename, file_data.len());

                                // Save the received file
                                let received_filename = format!("received_{}", filename);
                                match tokio::fs::write(&received_filename, &file_data).await {
                                    Ok(_) => {
                                        println!("\nSaved received file to {}", received_filename);
                                        let _ = swarm.behaviour_mut().barter_protocol.send_response(
                                            channel,
                                            BarterResponse::FileReceived
                                        );
                                    },
                                    Err(e) => {
                                        eprintln!("Failed to save file: {}", e);
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
                                    println!("Barter accepted! Received {} bytes", file_bytes.len());

                                    // Get the details of our request
                                    if let Some(barter) = pending_barters.get(&peer) {
                                        // Save the received file
                                        let received_filename = format!("received_{}", barter.requesting);
                                        match tokio::fs::write(&received_filename, &file_bytes).await {
                                            Ok(_) => println!("Saved received file to {}", received_filename),
                                            Err(e) => eprintln!("Failed to save file: {}", e),
                                        }

                                        // Now send the file we offered
                                        println!("Reading our offering file: {}", barter.offering);
                                        match File::open(&barter.offering).await {
                                            Ok(mut file) => {
                                                let mut buffer = Vec::new();
                                                match file.read_to_end(&mut buffer).await {
                                                    Ok(_) => {
                                                        println!("Sending our offered file ({} bytes)", buffer.len());

                                                        // Send the file as a FileDelivery message
                                                        let delivery = BarterMessage::FileDelivery {
                                                            filename: barter.offering.clone(),
                                                            file_data: buffer,
                                                        };

                                                        swarm.behaviour_mut().barter_protocol.send_request(&peer, delivery);
                                                    },
                                                    Err(e) => eprintln!("Failed to read our offering file: {}", e),
                                                }
                                            }
                                            Err(e) => eprintln!("Failed to open our offering file: {}", e),
                                        }
                                    }
                                }
                                BarterResponse::Declined => {
                                    println!("Barter declined by peer.");
                                    pending_barters.remove(&peer);
                                }
                                BarterResponse::FileReceived => {
                                    println!("Peer confirmed receipt of our file. Barter completed successfully!");
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
