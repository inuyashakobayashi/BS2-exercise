//! Data plane: length-prefixed `postcard` framing for chunk payloads.
//!
//! The data plane is a raw TCP channel, distinct from the control plane
//! (tarpc over JSON). Every message on the wire is framed as
//! `[u32 LE length][postcard bytes]`. Length-prefixing bounds the
//! per-frame allocation up front and keeps message boundaries
//! unambiguous across a streaming connection — postcard itself is
//! non-self-delimiting, so without a framing layer a reader would have
//! no way to know where one message ends and the next begins.
//!
//! `tarpc`'s JSON codec is not well suited to multi-MB payloads; keeping
//! bulk data off the control plane mirrors the real GFS architecture and
//! lets the master stay small and responsive.

use std::io;
use std::net::SocketAddr;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::ChunkId;

/// Maximum size of an inbound frame, in bytes. Guards against a peer
/// announcing a bogus length prefix that would otherwise let the reader
/// allocate unboundedly. 64 MB is an order of magnitude above the
/// default 4 MB chunk, leaving headroom for unusual writes without
/// inviting denial-of-service.
const MAX_FRAME_LEN: usize = 64 * 1024 * 1024;

// --------------------------------------------------------------------------
// Message types
// --------------------------------------------------------------------------

/// A single request on the data plane.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DataRequest {
	/// Store `payload` as chunk `chunk_id`.
	///
	/// If `forward_to` is `Some(addr)`, the receiver is the primary in a
	/// chain-replicated write and must forward the same payload (with
	/// `forward_to: None`) to `addr` before acknowledging.
	Write {
		chunk_id: ChunkId,
		forward_to: Option<SocketAddr>,
		payload: Vec<u8>,
	},

	/// Read the full contents of chunk `chunk_id`.
	Read { chunk_id: ChunkId },

	/// Read only the byte range `[offset, offset + len)` of chunk
	/// `chunk_id` (bonus extension of [`Read`](Self::Read)). Lets a reader
	/// fetch part of a chunk without transferring the whole payload. The
	/// reply is [`DataResponse::Data`] holding just the requested slice,
	/// clamped to the chunk's actual length (so an out-of-range request
	/// yields fewer bytes, or an empty slice, rather than an error), or
	/// [`DataResponse::NotFound`] if the chunk is absent.
	ReadRange {
		chunk_id: ChunkId,
		offset: u64,
		len: u64,
	},
}

/// A single reply on the data plane.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DataResponse {
	/// A write (and any forward) completed successfully.
	Written,
	/// A read returned the full chunk contents.
	Data(Vec<u8>),
	/// The requested chunk is not present on this server.
	NotFound,
	/// Generic failure with a human-readable message, e.g., a failed
	/// forward to the secondary, or a WAL write error.
	Error(String),
}

// --------------------------------------------------------------------------
// Framing
// --------------------------------------------------------------------------

/// Serialise `msg` with postcard and write it as one length-prefixed
/// frame. Flushes the underlying writer before returning so the peer
/// sees the full frame.
pub async fn write_frame<W, T>(w: &mut W, msg: &T) -> io::Result<()>
where
	W: AsyncWrite + Unpin,
	T: Serialize,
{
	let body =
		postcard::to_allocvec(msg).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
	let len = u32::try_from(body.len())
		.map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "frame too large"))?;
	w.write_all(&len.to_le_bytes()).await?;
	w.write_all(&body).await?;
	w.flush().await?;
	Ok(())
}

/// Read one length-prefixed frame from `r` and deserialise it as `T`.
/// Returns an [`io::ErrorKind::UnexpectedEof`] if the connection closed
/// between frames.
pub async fn read_frame<R, T>(r: &mut R) -> io::Result<T>
where
	R: AsyncRead + Unpin,
	T: for<'de> Deserialize<'de>,
{
	let mut len_buf = [0u8; 4];
	r.read_exact(&mut len_buf).await?;
	let len = u32::from_le_bytes(len_buf) as usize;
	if len > MAX_FRAME_LEN {
		return Err(io::Error::new(
			io::ErrorKind::InvalidData,
			format!("frame length {len} exceeds cap {MAX_FRAME_LEN}"),
		));
	}
	let mut body = vec![0u8; len];
	r.read_exact(&mut body).await?;
	postcard::from_bytes(&body).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}
