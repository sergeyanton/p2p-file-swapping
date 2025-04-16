mod barter;
mod chat_room;

use clap::Parser;
use std::error::Error;

#[derive(Parser, Debug)]
#[clap(name = "P2P File Bartering with Chat")]
struct Cli {
    #[arg(long)]
    port: Option<String>,

    #[arg(long)]
    peer: Option<String>,

    #[arg(long)]
    username: Option<String>,

    #[arg(long)]
    mode: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();

    // Get username
    let username = cli.username.unwrap_or_else(|| {
        println!("Enter your username:");
        let mut input = String::new();
        std::io::stdin()
            .read_line(&mut input)
            .expect("Failed to read username");
        input.trim().to_string()
    });

    // Determine which mode to run
    let mode = cli.mode.unwrap_or_else(|| {
        println!("Select mode (chat/barter):");
        let mut input = String::new();
        std::io::stdin()
            .read_line(&mut input)
            .expect("Failed to read mode");
        input.trim().to_string()
    });

    match mode.to_lowercase().as_str() {
        "chat" => {
            // Create and run chat room
            let chat_args = chat_room::ChatCliArgs {
                port: cli.port,
                peer: cli.peer.as_ref().map(|addr_str| {
                    addr_str
                        .parse::<libp2p::Multiaddr>()
                        .expect("Invalid multiaddr")
                }),
            };

            let mut chat = chat_room::ChatRoom::new(username, chat_args).await?;
            chat.run().await?;
        }

        "barter" => {
            // Run the existing barter functionality
            barter::run_barter_mode(
                cli.port,
                cli.peer
                    .map(|addr| addr.parse().expect("Invalid multiaddr")),
            )
            .await?;
        }
        _ => {
            println!("Invalid mode. Choose 'chat' or 'barter'.");
        }
    }

    Ok(())
}
