//! Caliban integration: wire types, NDJSON framing, the control client, and
//! the stream-json normalizer. The wire format is the only coupling to caliban.

pub mod client;
pub mod sources;
pub mod stream;
pub mod transport;
pub mod wire;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

use crate::error::{CoreError, Result};

/// Write one JSON value as an NDJSON frame (compact JSON + `\n`).
pub(crate) async fn write_frame<W, T>(w: &mut W, value: &T) -> Result<()>
where
    W: AsyncWriteExt + Unpin,
    T: serde::Serialize,
{
    let mut line = serde_json::to_vec(value)?;
    line.push(b'\n');
    w.write_all(&line).await?;
    w.flush().await?;
    Ok(())
}

/// Read exactly one NDJSON frame and deserialize it. Returns a protocol error
/// if the stream ends before a full line is read.
pub(crate) async fn read_frame<R, T>(r: &mut R) -> Result<T>
where
    R: AsyncBufReadExt + Unpin,
    T: serde::de::DeserializeOwned,
{
    let mut line = String::new();
    let n = r.read_line(&mut line).await?;
    if n == 0 {
        return Err(CoreError::Protocol(
            "connection closed before a reply frame was read".into(),
        ));
    }
    let value = serde_json::from_str(line.trim_end())?;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::caliband::wire::CtlRequest;
    use tokio::io::BufReader;

    #[tokio::test]
    async fn frame_round_trips_through_a_pipe() {
        // Write a frame into an in-memory buffer, then read it back.
        let mut buf: Vec<u8> = Vec::new();
        write_frame(&mut buf, &CtlRequest::List).await.unwrap();
        assert_eq!(buf, b"{\"kind\":\"list\"}\n");

        let mut reader = BufReader::new(&buf[..]);
        let req: CtlRequest = read_frame(&mut reader).await.unwrap();
        assert_eq!(req, CtlRequest::List);
    }

    #[tokio::test]
    async fn read_frame_errors_on_empty_stream() {
        let empty: &[u8] = b"";
        let mut reader = BufReader::new(empty);
        let r: Result<CtlRequest> = read_frame(&mut reader).await;
        assert!(matches!(r, Err(CoreError::Protocol(_))));
    }
}
