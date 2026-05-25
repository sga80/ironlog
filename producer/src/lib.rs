use compio::io::AsyncWriteExt;
use compio::net::TcpStream;
use compio::BufResult;
use ironlog_core::{Handshake, PayloadType, ProducerFrame, ProducerResult, RequestType};
use std::io::Error;

pub struct Producer {
    host: String,
    port: u16,
    tcp_stream: TcpStream,
    channel_name: String,
}

impl Producer {
    pub async fn new(host: String, port: u16, channel_name: String) -> Result<Self, Error> {
        let mut tcp_stream = TcpStream::connect(format!("{}:{}", host, port)).await?;
        // first connect and write, channel length, channel name and request type
        let producer_handshake = Handshake::new(channel_name.clone(), RequestType::Produce);
        tcp_stream.write_all(producer_handshake.to_bytes()).await.expect("cannot send handshake to server");
        Ok(Producer { host, port, tcp_stream, channel_name })
    }

    pub async fn send(&mut self, payload_type: PayloadType, payload: &[u8]) -> Result<ProducerResult, Error> {
        let request_frame = ProducerFrame::new(payload_type, payload.to_vec());
        let BufResult(res, _) = self.tcp_stream.write_all(request_frame.to_bytes()).await;
        if res.is_err() {
            return Err(res.err().unwrap());
        }
        let result = ProducerResult::from_bytes(&mut self.tcp_stream).await?;
        Ok(result)
    }
}