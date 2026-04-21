//! Binary entrypoint for the Legato server daemon.

use legato_server::{Server, ServerConfig};

fn main() {
    let _server = Server::new(ServerConfig::default());
    println!("legato-server bootstrap ready");
}
