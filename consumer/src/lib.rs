use compio::io::AsyncWriteExt;
use compio::net::TcpStream;
use compio::BufResult;
use ironlog_core::{consumer, ConsumerFrame, ConsumerMetadata, Handshake, PayloadType, RequestType};
use std::io::Error;

#[derive(Debug)]
pub struct ConsumerResult {
    offset: u64,
    timestamp: u64,
    payload_type: PayloadType,
    payload: Vec<u8>,
    is_end_of_file: bool,
}
impl ConsumerResult {
    pub fn new(offset: u64, timestamp: u64, payload_type: PayloadType, payload: Vec<u8>, is_eof: bool) -> Self {
        ConsumerResult {
            offset,
            timestamp,
            payload_type,
            payload,
            is_end_of_file: is_eof,
        }
    }
    pub fn offset(&self) -> u64 {
        self.offset
    }
    pub fn payload(&self) -> Vec<u8> {
        self.payload.clone()
    }
    pub fn is_eof(&self) -> bool {
        self.is_end_of_file
    }
}
pub struct Consumer {
    tcp_stream: TcpStream,
}

impl Consumer {
    pub async fn new(host: String, port: u16, channel_name: String) -> Result<Self, Error> {
        let mut tcp_stream = TcpStream::connect(format!("{}:{}", host, port)).await?;
        tcp_stream.set_nodelay(true).expect("node delay to set to true"); // disables nagle algorithm so that we don't buffer . this is because we are sending small consumer frame
        let consumer_handshake = Handshake::new(channel_name, RequestType::Fetch);
        tcp_stream.write_all(consumer_handshake.to_bytes()).await.expect("cannot send handshake to server ");
        Ok(Consumer {
            tcp_stream,
        })
    }


    pub async fn fetch(&mut self, offset: Option<u64>) -> Result<ConsumerResult, Error> {
        let consumer_frame = ConsumerFrame::new(offset);
        let BufResult(res, _) = self.tcp_stream.write_all(consumer_frame.to_bytes()).await;
        if res.is_err() {
            return Err(res.err().unwrap());
        }

        let result = ConsumerMetadata::from_bytes(&mut self.tcp_stream).await;
        match result {
            Ok(cr) => {
                // now read the payload also
                let payload: Vec<u8> = Vec::with_capacity(cr.payload_length() as usize);
                let payload = consumer::payload_from_bytes(&mut self.tcp_stream, payload).await?;
                Ok(ConsumerResult {
                    offset: cr.offset(),
                    timestamp: cr.timestamp(),
                    payload_type: cr.payload_type(),
                    payload,
                    is_end_of_file: cr.is_end_of_file(),
                })
            }
            Err(e) => {
                return Err(e);
            }
        }
    }
}