# Rust SwapBytes - P2P File Swapping Application

A secure peer-to-peer file swapping application built with Rust and libp2p that enables users to discover peers, chat in a global room, establish direct messaging connections, and securely exchange files with end-to-end encryption.

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Command Line Parameters](#command-line-parameters)
- [Running the Application](#running-the-application)
  - [Starting the Rendezvous Server](#starting-the-rendezvous-server)
  - [Starting Client Applications](#starting-client-applications)
  - [Multiple Computer Setup](#multiple-computer-setup)
- [Application Usage](#application-usage)
  - [Global Chat Commands](#global-chat-commands)
  - [Direct Messaging](#direct-messaging)
  - [File Bartering Process](#file-bartering-process)
- [Step-by-Step Examples](#step-by-step-examples)
  - [Scenario 1: File Swapping on Local Machine](#scenario-1-file-swapping-between-two-users-on-the-same-network)
  - [Scenario 2: File Swapping Between Computers](#scenario-2-file-swapping-between-computers)
- [Troubleshooting](#troubleshooting)

## Features

- **Peer-to-Peer Communication**: Built on libp2p for direct, serverless communication
- **Rendezvous Discovery**: Easily find other peers on the network
- **Global Chat Room**: Message with all connected peers
- **Private Messaging**: Establish direct connections with specific peers
- **Secure File Exchange**: End-to-end encryption using age cryptography
- **File Bartering**: Trade files with other users in a negotiated exchange

## Requirements

- Rust
- Cargo package manager

## Installation

1. Clone the repository:

   ```
   git clone https://eng-git.canterbury.ac.nz/san177/p2p-file-swapping-assignment2.git
   cd p2p-file-swapping-assignment2
   ```

2. Build the application:

   ```
   cargo build
   ```

## Command Line Parameters

| Parameter                    | Description                                                                         |
| ---------------------------- | ----------------------------------------------------------------------------------- |
| `--run-rendezvous-server`    | Start the application as a rendezvous server for peer discovery                     |
| `--port <PORT>`              | Specify a custom port to listen on (OPTIONAL, default: random available port)       |
| `--username <NAME>`          | Set your display name in the network (default: "Anonymous")                         |
| `--peer <MULTIADDR>`         | Connect directly to a peer using their multiaddress (OPTIONAL used for testing)     |
| `--rendezvous-server <ADDR>` | Connect to a specific rendezvous server by multiaddress (OPTIONAL used for testing) |

## Running the Application

### Starting the Rendezvous Server

The rendezvous server facilitates peer discovery and must be running for peers to find each other:

```
cargo run -- --run-rendezvous-server
```

You can specify a custom port:

```
cargo run -- --run-rendezvous-server --port 50002
```

When the server starts, it automatically creates an `.env` file containing its connection details that is used by the clinet to connect to the server.

### Starting Client Applications

To start a client and join the network:

```
cargo run -- --username YourName
```

By default, the client looks for a rendezvous server address in the `.env` file. You can specify a server manually:

```
cargo run -- --username YourName --rendezvous-server /ip4/192.168.1.5/tcp/50002/p2p/ExamplePeerId
```

### Multiple Computer Setup (Same Network)

To connect peers across different computers:

1. Start the rendezvous server on Computer A:

   ```
   cargo run -- --run-rendezvous-server
   ```

2. Note the server's multiaddress in the console output or check the `.env` file on Computer A.

3. Copy the `.env` file from Computer A to Computer B, or manually specify the server's multiaddress when starting the client on Computer B.

4. Start clients on both computers:
   ```
   cargo run -- --username ComputerAUser  # On Computer A
   cargo run -- --username ComputerBUser  # On Computer B
   ```

## Application Usage

### Global Chat Commands

Once connected, you'll be placed in the global chat room with these commands:

- `/help` - Show available commands
- `/peers` - List connected peers
- `/dm <username or peer_id>` - Request to direct message with a peer
- `/accept` - Accept pending direct message request
- `/discover` - Manually discover peers through the rendezvous server (used for testing purposes)
- `/quit` - Leave the chat room and exit the application

To send a message to everyone, simply type your text and press Enter.

### Direct Messaging

Direct messaging enables private conversations and file exchanges:

1. **Starting a DM Session**:

   - From the global chat room, use `/dm <username or peer_id>`
   - The recipient must accept with `/accept`
   - The initiator needs to press Enter to join the session

2. **DM Session Commands**:

   - `/barter <offering_file> <requesting_file>` - Propose a file exchange
   - `/accept` - Accept the latest barter proposal
   - `/decline` - Decline the latest barter proposal
   - `/peers` - List connected peers
   - `/return` - Return to the main chat room
   - `/help` - Show available commands

   You can also type regular messages to chat with your partner.

### File Bartering Process

The bartering system allows secure file exchanges:

1. **Propose a Barter**:
   - Use `/barter <your_file> <their_file>` to offer your file in exchange for theirs (make sure files do not contain a space in the name)
2. **Accept/Decline**:
   - The recipient can type `/accept` or `/decline`
3. **File Transfer**:
   - Files are automatically encrypted before sending
   - Received files are saved with a "received\_" prefix (e.g., "received_document.txt")
4. **Return to Chat Room**:
   - When finished, both users can type `/return` to go back to the global chat

## Step-by-Step Examples

### Scenario 1: File Swapping Between All Users on the Same Machine

1. **Start the Rendezvous Server**:

   ```
   cargo run -- --run-rendezvous-server
   ```

2. **Start Client A**:

   ```
   cargo run -- --username Alice
   ```

3. **Start Client B** (in another terminal):

   ```
   cargo run -- --username Bob
   ```

4. **Initiate Direct Message** (from Alice):

   ```
   /dm Bob
   ```

5. **Accept DM Request** (Bob):

   ```
   /accept
   ```

   Then Alice should press Enter to engage the session.

6. **Propose File Barter** (Alice):

   ```
   /barter 1.txt 2.txt
   ```

   This means Alice offers "1.txt" and wants "2.txt" from Bob.

7. **Accept Barter** (Bob):

   ```
   /accept
   ```

8. **Return to Chat Room** (both users):

   ```
   /return
   ```

9. **Exit Application** (both users):
   ```
   /quit
   ```

### Scenario 2: File Swapping Between Computers on the Same Network

1. **Start the Server** (on Computer A):

   ```
   cargo run -- --run-rendezvous-server
   ```

   The server will display its multiaddress and write it to the `.env` file.

2. **Update .env file** (on Computer B):
   Copy the contents of the `.env` file from Computer A to Computer B, or note the multiaddress displayed in the console.

3. **Start Client** (on Computer A):

   ```
   cargo run -- --username ComputerA
   ```

4. **Start Client** (on Computer B):

   ```
   cargo run -- --username ComputerB
   ```

   Or with explicit server address:

   ```
   cargo run -- --username ComputerB --rendezvous-server <multiaddress-from-computer-A>
   ```

5. **Follow steps 4-9 from Scenario 1**.

## Troubleshooting

- **Peer Discovery Issues**: If you can't see other peers, use the `/discover` command to manually trigger peer discovery.

- **File Access Errors**: If you encounter `Access denied (os error 5)`, stop all running instances of the application and run `cargo clean`.

- **Direct Messaging Failures**: Both peers must be online and connected to the rendezvous server for direct messaging to work.
