//! UDP multicast socket setup.
//!
//! `std::net::UdpSocket` does not expose multicast join options, so we build
//! the socket with `socket2` and convert it into a Tokio async socket.

use std::net::{Ipv4Addr, SocketAddr};

use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::UdpSocket;

use crate::error::{AppError, Result};

/// Create a Tokio UDP socket joined to the given multicast group.
///
/// Binds to `0.0.0.0:<port>` with `SO_REUSEADDR`, joins the group on the
/// loopback interface so same-machine testing works out of the box.
pub async fn join_multicast(multicast_addr: Ipv4Addr, port: u16) -> Result<UdpSocket> {
    let socket =
        Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP)).map_err(AppError::Io)?;

    // Allow multiple processes to bind the same port (useful during dev).
    socket.set_reuse_address(true).map_err(AppError::Io)?;

    let bind_addr = SocketAddr::from(([0, 0, 0, 0], port));
    socket.bind(&bind_addr.into()).map_err(AppError::Io)?;

    // Join the multicast group; loopback interface works for local simulator.
    socket
        .join_multicast_v4(&multicast_addr, &Ipv4Addr::LOCALHOST)
        .map_err(AppError::Io)?;

    // Required before handing off to Tokio's async runtime.
    socket.set_nonblocking(true).map_err(AppError::Io)?;

    let std_socket: std::net::UdpSocket = socket.into();
    UdpSocket::from_std(std_socket).map_err(AppError::Io)
}
