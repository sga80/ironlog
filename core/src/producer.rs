use crate::common::{PayloadType, RequestType, WriteStatus};
use std::io::Read;
use std::net::TcpStream;

#[derive(Clone, Debug)]
pub struct ProducerFrame {
    pub payload_type: PayloadType,
    channel_name: String,
    payload: Vec<u8>,
}


impl ProducerFrame {
    pub fn new(payload_type: PayloadType, channel_name: String, payload: Vec<u8>) -> Self {
        ProducerFrame {
            payload_type,
            channel_name,
            payload,
        }
    }

    pub fn channel_name(&self) -> &str {
        &self.channel_name
    }
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes: Vec<u8> = Vec::new();

        bytes.extend_from_slice(&(self.payload.len() as u32).to_be_bytes()); // 4 bytes
        bytes.extend_from_slice(&(self.payload_type as u16).to_be_bytes());  // 2 bytes
        bytes.push(self.channel_name.len() as u8); // 1 byte
        bytes.extend_from_slice(self.channel_name.as_bytes());
        bytes.extend_from_slice(&self.payload);

        bytes
    }

    pub fn from_bytes(tcp_stream: &mut TcpStream) -> std::io::Result<Self> {
        // 2 bytes: request type | | 4 bytes: payload_length |2 bytes: payload type | 1 byte: channel name length | M bytes: channel name | N bytes: payload |
        // 1) read the first bytes to get the length
        let mut buf = [0u8; 4]; // this creates a slice with 4 elements all initialized to 0 of type unsigned int 8 bits as  u8 which is a byte
        tcp_stream.read_exact(&mut buf)?;
        let payload_length = u32::from_be_bytes(buf); // we are using big endian here to represent u32 over the wire. we could also use little endian but BE is easier to read left to write

        let mut buf = [0; 2]; // now initialize a slice with 2 bytes
        tcp_stream.read_exact(&mut buf)?;
        let payload_type = u16::from_be_bytes(buf); // read the next 2 bytes as payload type
        let payload_type = PayloadType::try_from(payload_type)?;

        let mut buf = [0; 1]; // now initialize a slice with 1 byte
        tcp_stream.read_exact(&mut buf)?;
        let channel_name_length = buf[0];

        let mut buf = vec![0; channel_name_length as usize];
        tcp_stream.read_exact(&mut buf)?;
        let channel_name = str::from_utf8(&buf).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        let mut payload = vec![0u8; payload_length as usize];
        tcp_stream.read_exact(&mut payload)?;

        Ok(Self {
            payload_type,
            channel_name: channel_name.to_string(),
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

    pub fn from_bytes(tcp_stream: &mut TcpStream) -> std::io::Result<Self> {
        // | 2 bytes: request type | 1 byte: status | 8 bytes: offset | 8 bytes: timestamp |
        let mut buf = [0u8; 2]; // create a slice of u8 with 2 elements which is 2 bytes
        tcp_stream.read_exact(&mut buf)?;
        let request_type = u16::from_be_bytes(buf);
        let request_type = RequestType::try_from(request_type)?;

        let mut buf = [0u8; 1]; // create a slice of one byte
        tcp_stream.read_exact(&mut buf)?;
        let write_status = buf[0];
        let write_status = WriteStatus::try_from(write_status)?;

        let mut buf = [0u8; 8];
        tcp_stream.read_exact(&mut buf)?;
        let offset = u64::from_be_bytes(buf);

        let mut buf = [0u8; 8];
        tcp_stream.read_exact(&mut buf)?;
        let timestamp = u64::from_be_bytes(buf);

        Ok(Self {
            offset,
            broker_timestamp: timestamp,
            status: write_status,
            request_type,
        })
    }
}