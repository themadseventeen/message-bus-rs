use bytes::{Buf, BytesMut};
use tokio_util::codec::Decoder;

use std::sync::Arc;

use crate::client::client::ClientError;

pub fn body_to_framed_stream(
    resp: reqwest::Response,
) -> impl futures::Stream<Item = Result<serde_json::Value, ClientError>> {
    let mut buf = BytesMut::new();
    let mut codec = MessageBusCodec;

    use futures::StreamExt;
    resp.bytes_stream().map(move |item| {
        match item {
            Ok(chunk) => {
                buf.extend_from_slice(&chunk);

                match codec.decode(&mut buf) {
                    Ok(Some(value)) => Ok(value),
                    Ok(None) => Err(ClientError::Incomplete),
                    Err(e) => Err(ClientError::IoError(e)),
                }
            }
            Err(e) => Err(ClientError::RequestError(Arc::new(e))),
        }
    })
}

pub struct MessageBusCodec;

impl Decoder for MessageBusCodec {
    type Item = serde_json::Value;
    type Error = std::io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if let Some(pos) = buf.iter().position(|b| *b == b'|') {
            let frame = buf.split_to(pos);
            buf.advance(1); // yeet the pipe

            let value = serde_json::from_slice(&frame)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            return Ok(Some(value));
        }

        Ok(None)
    }
    fn decode_eof(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        buf.clear();
        Ok(None)
    }
}
