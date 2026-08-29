mod bencode;
mod download;
mod network;
mod orchestrate;
mod peer;
mod seed;
mod torrent;

use clap::{Parser, Subcommand};
use std::net::SocketAddr;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "bittorrent-client", about = "A from-scratch BitTorrent client")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Generate a .torrent file for a local file.
    MakeTorrent {
        /// Path to the file to torrent.
        #[arg(long)]
        file: PathBuf,
        /// Announce URL to embed (unused for manual-peer demos, but
        /// required by the .torrent format).
        #[arg(long, default_value = "http://example.invalid/announce")]
        announce: String,
        /// Bytes per piece. 262144 (256 KiB) is a reasonable default.
        #[arg(long, default_value_t = 262_144)]
        piece_length: i64,
        /// Where to write the .torrent file. Defaults to `<file>.torrent`.
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Seed a file: listen for peers and serve it.
    Seed {
        /// Path to the .torrent describing what to seed.
        #[arg(long)]
        torrent: PathBuf,
        /// Path to the actual file content on disk.
        #[arg(long)]
        file: PathBuf,
        /// TCP port to listen on.
        #[arg(long, default_value_t = 6881)]
        port: u16,
    },
    /// Leech: download a torrent's pieces from a manually-specified peer list.
    Leech {
        /// Path to the .torrent describing what to download.
        #[arg(long)]
        torrent: PathBuf,
        /// Where to write the downloaded file.
        #[arg(long)]
        output: PathBuf,
        /// Comma-separated peer addresses, e.g. 192.168.1.10:6881,192.168.1.11:6881
        #[arg(long, value_delimiter = ',')]
        peers: Vec<String>,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Command::MakeTorrent { file, announce, piece_length, out } => {
            orchestrate::make_torrent(&file, &announce, piece_length, out)?;
        }
        Command::Seed { torrent, file, port } => {
            let bytes = std::fs::read(&torrent)?;
            let parsed = torrent::parse(&bytes)?;
            orchestrate::run_seed(&parsed, file, port).await?;
        }
        Command::Leech { torrent, output, peers } => {
            let bytes = std::fs::read(&torrent)?;
            let parsed = torrent::parse(&bytes)?;
            let peer_addrs: Vec<SocketAddr> =
                peers.iter().map(|s| s.parse()).collect::<Result<_, _>>()?;
            orchestrate::run_leech(&parsed, output, peer_addrs).await?;
        }
    }

    Ok(())
}