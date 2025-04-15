use clap::Parser;
use futures::StreamExt;
use libp2p::{
    Multiaddr,
    PeerId,
    StreamProtocol,
    noise,
    // Import ResponseChannel and RequestId if needed for failure handling
    request_response::{self, ProtocolSupport, ResponseChannel},
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp,
    yamux,
};
use serde::{Deserialize, Serialize};
// Import HashMap
use std::{collections::HashMap, error::Error, time::Duration};
use tokio::{
    // Keep File for now, though read/write might be simpler
    fs::File,
    // Add AsyncWriteExt (needed for write)
    io::{self, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, stdin},
    select,
};
// Import Path (useful for filenames)
use std::path::Path;

// --- Configuration ---

const STORAGE_DIR: &str = "barter_received_files"; // Directory for received files

// Define protocol names as constants
const BARTER_PROTOCOL_NAME: StreamProtocol = StreamProtocol::new("/file-barter-confirm/1"); // Incremented version
const POST_ACCEPT_PROTOCOL_NAME: StreamProtocol = StreamProtocol::new("/post-acceptance/1"); // Incremented version

// --- CLI Arguments ---

#[derive(Parser, Debug)]
#[clap(name = "libp2p file barter with confirmation v2")]
struct Cli {
    #[arg(long)]
    port: Option<String>,

    #[arg(long)]
    peer: Option<Multiaddr>,
}

// --- Network Behaviours ---

#[derive(NetworkBehaviour)]
struct ReqResBehaviour {
    initial_barter: request_response::cbor::Behaviour<FileRequest, FileResponse>,
    post_acceptance:
        request_response::cbor::Behaviour<PostAcceptanceRequest, PostAcceptanceResponse>,
}
// --- Protocol Message Structs ---

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileRequest {
    offer_file_name: String,
    offer_file_data: Vec<u8>,
    requested_file_name: String,
}
// Response to initial offer (currently unused unless rejection sends this)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileResponse(Vec<u8>); // Empty Vec could mean rejection

// Stage 2: Request sent *after* user accepts the initial offer
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostAcceptanceRequest {
    original_offer_name: String, // Let original sender know which offer was accepted
    file_now_sending: String,    // Re-state the file needed from the original sender
}

// Stage 2: Response containing the data originally requested by Peer B
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostAcceptanceResponse(Vec<u8>); // Just the bytes

// --- Main Application Logic ---

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Initialize logging (optional but helpful)
    // env_logger::init(); // Uncomment if you add env_logger to Cargo.toml

    let cli = Cli::parse();

    // Create storage directory
    tokio::fs::create_dir_all(STORAGE_DIR).await?;
    println!("Storing received files in: {}", STORAGE_DIR);

    let mut swarm = libp2p::SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_behaviour(|_key| {
            // Configure with proper timeout settings
            let initial_barter_config = request_response::Config::default()
                .with_request_timeout(Duration::from_secs(30))
                .with_max_request_size(1024 * 1024 * 10); // 10MB max

            let post_acceptance_config = request_response::Config::default()
                .with_request_timeout(Duration::from_secs(30))
                .with_max_request_size(1024 * 1024 * 10); // 10MB max

            ReqResBehaviour {
                initial_barter: request_response::cbor::Behaviour::new(
                    [(BARTER_PROTOCOL_NAME, ProtocolSupport::Full)],
                    initial_barter_config,
                ),
                post_acceptance: request_response::cbor::Behaviour::new(
                    [(POST_ACCEPT_PROTOCOL_NAME, ProtocolSupport::Full)],
                    post_acceptance_config,
                ),
            }
        })?
        .build();

    // --- Listening and Dialing ---
    let listen_port = cli.port.unwrap_or("0".to_string());
    let listen_addr_str = format!("/ip4/0.0.0.0/tcp/{listen_port}");
    swarm.listen_on(listen_addr_str.parse()?)?;

    let mut was_peer_provided = false;
    if let Some(peer_addr) = cli.peer {
        swarm.dial(peer_addr.clone())?;
        println!("Attempting to dial: {}", peer_addr);
        was_peer_provided = true;
    } else {
        println!("No specific peer provided via --peer. Waiting for connections.");
    }

    // --- State Variables ---
    let mut stdin = BufReader::new(stdin()).lines();
    let mut connected_peer_id: Option<PeerId> = None; // Track primary peer
    // Store requests waiting for *our* confirmation
    // Key: Request ID (String), Value: (PeerId of requester, Original FileRequest data)
    let mut pending_confirmations: HashMap<String, (PeerId, FileRequest)> = HashMap::new();
    // Track the filename we expect back after *we* send a PostAcceptanceRequest
    let mut expecting_post_accept_response_for: Option<String> = None;

    println!("\n--- Barter with Confirmation (v2) ---");
    println!("To offer: <your_file_to_offer> <peer_file_to_request>");
    println!("To accept an offer: accept <request_id>");
    println!("To reject an offer (locally): reject <request_id>"); // Rejection doesn't notify peer
    println!("---------------------------------------\n");

    // --- Main Event Loop ---
    loop {
        select! {
            // --- Handle User Input ---
            Ok(Some(line)) = stdin.next_line() => {
                let trimmed_line = line.trim();

                // --- Accept Command ---
                if trimmed_line.starts_with("accept ") {
                    let parts: Vec<&str> = trimmed_line.splitn(2, ' ').collect();
                    if parts.len() == 2 {
                        let request_id = parts[1].to_string();
                        // Find and remove the pending confirmation
                        if let Some((requester_peer_id, original_request)) = pending_confirmations.remove(&request_id) {
                            println!("-> Accepting request '{}' from peer {}", request_id, requester_peer_id);

                            // 1. Save the offered data immediately
                            let offer_save_path = Path::new(STORAGE_DIR).join(format!("received_offer_{}", original_request.offer_file_name));
                            match tokio::fs::write(&offer_save_path, &original_request.offer_file_data).await {
                                Ok(_) => {
                                    println!("   Saved offered file to '{}'", offer_save_path.display());

                                    // 2. Send the NEW PostAcceptanceRequest back to the original sender
                                    let post_accept_req = PostAcceptanceRequest {
                                        original_offer_name: original_request.offer_file_name.clone(),
                                        file_now_sending: original_request.requested_file_name.clone(),
                                    };

                                    println!("-> Sending PostAcceptanceRequest to {}: asking for '{}'", requester_peer_id, post_accept_req.file_now_sending);
                                    // Track the file we are now expecting back
                                    expecting_post_accept_response_for = Some(post_accept_req.file_now_sending.clone());
                                    // Send via the post_acceptance behaviour
                                    swarm.behaviour_mut().post_acceptance.send_request(&requester_peer_id, post_accept_req);

                                }
                                Err(e) => {
                                    eprintln!("   ! Failed to save offered file '{}' when accepting request '{}': {}", offer_save_path.display(), request_id, e);
                                    println!("   ! Acceptance cancelled locally due to save error. Peer was not notified.");
                                }
                            }
                        } else {
                            eprintln!("! Request ID '{}' not found or already handled.", request_id);
                        }
                    } else {
                        eprintln!("! Invalid accept command. Use: accept <request_id>");
                    }
                // --- Reject Command (Local Only) ---
                } else if trimmed_line.starts_with("reject ") {
                    let parts: Vec<&str> = trimmed_line.splitn(2, ' ').collect();
                    if parts.len() == 2 {
                        let request_id = parts[1].to_string();
                        // Just remove the pending confirmation locally
                        if pending_confirmations.remove(&request_id).is_some() {
                            println!("-> Rejected request '{}' locally. Peer will not be notified.", request_id);
                        } else {
                            eprintln!("! Request ID '{}' not found or already handled.", request_id);
                        }
                    } else {
                         eprintln!("! Invalid reject command. Use: reject <request_id>");
                     }
                // --- Initiate Barter Offer ---
                } else {
                    let parts: Vec<&str> = trimmed_line.split_whitespace().collect();
                    if parts.len() == 2 {
                        let offer_file_path_str = parts[0];
                        let requested_file_name_str = parts[1];

                        if let Some(peer_id) = connected_peer_id {
                            println!("Attempting barter: Offering '{}', requesting '{}'", offer_file_path_str, requested_file_name_str);
                            match tokio::fs::read(offer_file_path_str).await {
                                Ok(offer_data) => {
                                    let offer_filename = Path::new(offer_file_path_str)
                                        .file_name().map(|n| n.to_string_lossy().into_owned())
                                        .unwrap_or_else(|| offer_file_path_str.to_string());

                                    let request = FileRequest {
                                        offer_file_name: offer_filename,
                                        offer_file_data: offer_data,
                                        requested_file_name: requested_file_name_str.to_string(),
                                    };

                                    // Send the *initial* request using the initial_barter behaviour
                                    // Note: We don't store filename_we_requested anymore, as the final data comes
                                    // via PostAcceptanceResponse identified by expecting_post_accept_response_for
                                    swarm.behaviour_mut().initial_barter.send_request(&peer_id, request);
                                    println!("-> Sent initial barter request (FileRequest) to {}", peer_id);
                                }
                                Err(e) => {
                                    eprintln!("! Error reading offer file '{}': {}", offer_file_path_str, e);
                                    println!("! Barter initiation cancelled.");
                                }
                            }
                        } else {
                            println!("! No peer connected. Cannot initiate barter.");
                        }
                    } else if !trimmed_line.is_empty() {
                        println!("! Invalid input. Use 'offer <f1> <f2>', 'accept <id>', or 'reject <id>'.");
                    }
                }
            } // End stdin handling

            // --- Handle Network Events ---
            event = swarm.select_next_some() => match event {
                SwarmEvent::NewListenAddr { address, .. } => {
                    let full_address = address.with(libp2p::multiaddr::Protocol::P2p(*swarm.local_peer_id()));
                    println!("Listening on: {}", full_address);
                    if !was_peer_provided {
                        println!("Run another instance with: --peer {}", full_address);
                    }
                }

                SwarmEvent::Behaviour(event) => match event {
                    // --- Handle Initial Barter Protocol Events ---
                    ReqResBehaviourEvent::InitialBarter(rr_event) => { // Capture the whole request_response::Event
                        // Now match specifically on the variants of request_response::Event
                        match rr_event {
                            request_response::Event::Message { message, peer, .. } => {
                                // This is your existing code for Message
                                match message {
                                    request_response::Message::Request { request, channel, .. } => {
                                        let peer_prefix = peer.to_base58();
                                        let request_id = format!("{}-{}", &peer_prefix[..6], request.offer_file_name);
                                        println!(/* ... print prompt ... */);
                                        if pending_confirmations.insert(request_id.clone(), (peer, request)).is_some() {
                                            eprintln!("! Warning: Overwrote pending request ID: {}", request_id);
                                        }
                                        let _ = channel; // Mark used
                                    }
                                    request_response::Message::Response { request_id, .. } => {
                                        eprintln!("! Received unexpected FileResponse (protocol changed) for request ID: {:?}", request_id);
                                    }
                                     _ => {} // Ignore other Message types if any added later
                                }
                            }
                            // **** ADD WILDCARD HERE for other request_response::Event variants ****
                            // Optionally log these failures if needed for debugging
                            request_response::Event::OutboundFailure { request_id, error, .. } => {
                                eprintln!("! InitialBarter OutboundFailure for request {:?}: {}", request_id, error);
                            }
                            request_response::Event::InboundFailure { error, .. } => {
                                eprintln!("! InitialBarter InboundFailure: {}", error);
                            }
                             request_response::Event::ResponseSent { .. } => {
                                 // Usually ignored, response sending is fire-and-forget in this logic
                             }
                           // _ => {} // Catch-all for any other request_response::Event variants
                        }
                    } // End of InitialBarter event handling

                    ReqResBehaviourEvent::PostAcceptance(rr_event) => { // Capture the whole request_response::Event
                        // Now match specifically on the variants of request_response::Event
                        match rr_event {
                            request_response::Event::Message { message, peer, .. } => {
                                // This is your existing code for Message
                                match message {
                                    request_response::Message::Request { request, channel, .. } => {
                                        println!(/* ... print received PostAcceptanceRequest ... */);
                                        let mut response_bytes = Vec::new();
                                        match tokio::fs::read(&request.file_now_sending).await {
                                            Ok(data) => { /* ... read ok ... */ response_bytes = data; }
                                            Err(e) => { /* ... read error ... */ }
                                        }
                                        println!("-> Sending PostAcceptanceResponse ({} bytes) to {}", response_bytes.len(), peer);
                                        if let Err(resp_err) = swarm.behaviour_mut().post_acceptance.send_response(channel, PostAcceptanceResponse(response_bytes)) {
                                            eprintln!("! Error sending PostAcceptanceResponse (tried {} bytes): {:?}", resp_err.0.len(), resp_err);
                                        }
                                    }
                                    request_response::Message::Response { request_id, response, .. } => {
                                        println!("<- Received PostAcceptanceResponse for our request ID: {:?}", request_id);
                                        let received_data = response.0;
                                        if let Some(expected_filename) = expecting_post_accept_response_for.take() {
                                            if received_data.is_empty() { /* ... handle empty response ... */ }
                                            else { /* ... handle successful response ... */ }
                                        } else { /* ... handle unexpected response ... */ }
                                    }
                                     _ => {} // Ignore other Message types if any added later
                                }
                            }
                            // **** ADD WILDCARD HERE for other request_response::Event variants ****
                             // Optionally log these failures if needed for debugging
                            request_response::Event::OutboundFailure { request_id, error, .. } => {
                                eprintln!("! PostAcceptance OutboundFailure for request {:?}: {}", request_id, error);
                                // If an outbound request fails, we might no longer be expecting a response
                                expecting_post_accept_response_for = None;
                            }
                            request_response::Event::InboundFailure { error, .. } => {
                                eprintln!("! PostAcceptance InboundFailure: {}", error);
                            }
                             request_response::Event::ResponseSent { .. } => {
                                 // Usually ignored
                             }
                            //_ => {} // Catch-all for any other request_response::Event variants
                        }
                    } // End of PostAcceptance event handling
                } // End SwarmEvent::Behaviour

                // --- Handle Connections ---
                 SwarmEvent::ConnectionEstablished { peer_id, endpoint, .. } => {
                    println!("Established connection to {} via {}", peer_id, endpoint.get_remote_address());
                     if connected_peer_id.is_none() {
                         connected_peer_id = Some(peer_id);
                         println!("   Ready to barter with {}.", peer_id);
                     } else {
                          println!("   Already connected to {}. Ignoring new peer {} for primary barter.", connected_peer_id.unwrap(), peer_id);
                     }
                }
                 SwarmEvent::ConnectionClosed { peer_id, cause, .. } => {
                     println!("Connection to {} closed. Cause: {:?}", peer_id, cause);
                     if connected_peer_id == Some(peer_id) {
                         println!("   Lost connection to the primary barter peer. Waiting for a new connection...");
                         connected_peer_id = None;
                         // Clear pending confirmations related to this peer?
                         pending_confirmations.retain(|_id, (p, _req)| *p != peer_id);
                         // If we were expecting a response from this peer, clear that too
                         if expecting_post_accept_response_for.is_some() {
                             // More complex logic might be needed if multiple peers are handled
                             // to check if the expected response was specifically from this peer.
                             println!("   Cleared expectation for post-acceptance response due to disconnection.");
                             expecting_post_accept_response_for = None;
                         }
                     }
                 }
                 SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                    if let Some(p) = peer_id {
                        println!("! Failed to dial peer {}: {}", p, error);
                    } else {
                        println!("! Failed to dial peer (unknown ID): {}", error);
                    }
                 }
                 SwarmEvent::Dialing { peer_id, .. } => {
                    if let Some(p) = peer_id {
                       println!("Dialing peer {}...", p);
                    } else {
                       println!("Dialing peer (unknown ID)...");
                    }
                }


                _ => { } // Ignore other swarm events
            } // End SwarmEvent handling
        } // End select!
    } // End loop
}
