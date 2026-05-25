use compio::io::AsyncWriteExt;
use compio::net::TcpStream;
use compio::BufResult;
use ironlog_core::{ConsumerFrame, ConsumerResult, Handshake, RequestType};
use std::io::{Error, ErrorKind};

pub struct Consumer {
    tcp_stream: TcpStream,
}

impl Consumer {
    pub async fn new(host: String, port: u16, channel_name: String) -> Result<Self, Error> {
        let mut tcp_stream = TcpStream::connect(format!("{}:{}", host, port)).await?;
        let consumer_handshake = Handshake::new(channel_name, RequestType::Fetch);
        tcp_stream.write_all(consumer_handshake.to_bytes()).await.expect("cannot send handshake to server ");
        Ok(Consumer {
            tcp_stream,
        })
    }

    pub async fn fetch(&mut self, offset: Option<u64>) -> Result<Vec<ConsumerResult>, Error> {
        let mut results: Vec<ConsumerResult> = Vec::new();
        let consumer_frame = ConsumerFrame::new(offset);
        let BufResult(res, _) = self.tcp_stream.write_all(consumer_frame.to_bytes()).await;
        if res.is_err() {
            return Err(res.err().unwrap());
        }
        loop {
            let result = ConsumerResult::from_bytes(&mut self.tcp_stream).await;
            match result {
                Ok(cr) => {
                    results.push(cr)
                }
                Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                    break; // reached eof
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
        Ok(results)
    }
}