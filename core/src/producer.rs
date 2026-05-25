use crate::common::{PayloadType, RequestType, WriteStatus};
use compio::io::AsyncReadExt;
use compio::net::TcpStream;
use compio::BufResult;

#[derive(Clone, Debug)]
pub struct ProducerFrame {
    pub payload_type: PayloadType,
    payload: Vec<u8>,
}


impl ProducerFrame {
    pub fn new(payload_type: PayloadType, payload: Vec<u8>) -> Self {
        ProducerFrame {
            payload_type,
            payload,
        }
    }
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes: Vec<u8> = Vec::new();

        bytes.extend_from_slice(&(self.payload.len() as u32).to_be_bytes()); // 4 bytes
        bytes.extend_from_slice(&(self.payload_type as u16).to_be_bytes());  // 2 bytes
        bytes.extend_from_slice(&self.payload);

        bytes
    }

    pub async fn from_bytes(tcp_stream: &mut TcpStream) -> std::io::Result<Self> {
        //| 4 bytes: length | 2 bytes: payload type | N bytes: payload |
        // 1) read the first bytes to get the length
        let payload_length = tcp_stream.read_u32().await?; // we are using big endian here to represent u32 over the wire. we could also use little endian but BE is easier to read left to write
        let payload_type = tcp_stream.read_u16().await?; // read the next 2 bytes as payload type
        let payload_type = PayloadType::try_from(payload_type)?;
        let payload = Vec::with_capacity(payload_length as usize);
        let BufResult(res, payload) = tcp_stream.read_exact(payload).await;
        res?; //propagate the error
        Ok(Self {
            payload_type,
            payload,
        })
    }
}

#[derive(Clone, Debug)]
pub struct ProducerResult {
    pub offset: u64,
    pub broker_timestamp: u64,
    pub status: WriteStatus,
    pub request_type: RequestType,
}

impl ProducerResult {
    pub fn to_bytes(&self) -> Vec<u8> {
        // | 2 bytes: request type | 1 byte: status | 8 bytes: offset | 8 bytes: timestamp |
        let mut bytes: Vec<u8> = Vec::new();
        bytes.extend_from_slice(&(self.request_type as u16).to_be_bytes()); // 2 bytes
        bytes.extend_from_slice(&(self.status as u8).to_be_bytes());  // 1 bytes
        bytes.extend_from_slice(&self.offset.to_be_bytes());  // 8 bytes
        bytes.extend_from_slice(&self.broker_timestamp.to_be_bytes());

        bytes
    }

    pub async fn from_bytes(tcp_stream: &mut TcpStream) -> std::io::Result<Self> {
        // | 2 bytes: request type | 1 byte: status | 8 bytes: offset | 8 bytes: timestamp |
        let request_type = tcp_stream.read_u16().await?;
        let request_type = RequestType::try_from(request_type)?;
        let write_status = tcp_stream.read_u8().await?;
        let write_status = WriteStatus::try_from(write_status)?;
        let offset = tcp_stream.read_u64().await?;
        let timestamp = tcp_stream.read_u64().await?;

        Ok(Self {
            offset,
            broker_timestamp: timestamp,
            status: write_status,
            request_type,
        })
    }
}

