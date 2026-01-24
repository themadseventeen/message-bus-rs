use bytes::{Buf, BytesMut};
use tokio_util::codec::Decoder;

use futures::TryStreamExt;
use std::io;
use tokio_util::codec::FramedRead;
use tokio_util::io::StreamReader;

pub fn body_to_framed_stream(
    resp: reqwest::Response,
) -> impl futures::Stream<Item = Result<serde_json::Value, io::Error>> {
    let byte_stream = resp
        .bytes_stream()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e));

    let reader = StreamReader::new(byte_stream);
    FramedRead::new(reader, MessageBusCodec)
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
