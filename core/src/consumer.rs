use crate::PayloadType;
use std::fs::File;
use std::io::Read;
use std::net::TcpStream;

#[derive(Clone, Debug)]
pub struct ConsumerFrame {
    channel_name: String,
    offset: u64,
}

impl ConsumerFrame {
    pub fn new(channel_name: String, offset: Option<u64>) -> Self {
        let offset_value = offset.unwrap_or_default();
        ConsumerFrame {
            channel_name,
            offset: offset_value,
        }
    }

    pub fn channel_name(&self) -> &str {
        &self.channel_name
    }
    pub fn offset(&self) -> u64 {
        self.offset
    }
    pub fn to_bytes(&self) -> Vec<u8> {
        // 1 byte: channel name length | M bytes: channel name  | 8 bytes: offset |
        let mut bytes: Vec<u8> = Vec::new();
        bytes.push(self.channel_name.len() as u8); // 1 byte
        bytes.extend_from_slice(self.channel_name.as_bytes());
        bytes.extend_from_slice(&self.offset.to_be_bytes());
        bytes
    }

    pub fn from_bytes(tcp_stream: &mut TcpStream) -> std::io::Result<Self> {
        //1 byte: channel name length | M bytes: channel name  | 8 bytes: offset |
        // request type is handled in the main
        let mut buf = [0; 1]; // now initialize a slice with 1 byte
        tcp_stream.read_exact(&mut buf)?;
        let channel_name_length = buf[0];

        let mut buf = vec![0; channel_name_length as usize];
        tcp_stream.read_exact(&mut buf)?;
        let channel_name = str::from_utf8(&buf).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        let mut buf = [0u8; 8];
        tcp_stream.read_exact(&mut buf)?;
        let offset = u64::from_be_bytes(buf);

        Ok(Self {
            channel_name: channel_name.to_string(),
            offset,
        })
    }
}

#[derive(Clone, Debug)]
pub struct ConsumerResult {
    offset: u64,
    timestamp: u64,
    payload_type: PayloadType,
    payload: Vec<u8>,
}

impl Default for ConsumerResult {
    fn default() -> Self {
        ConsumerResult {
            offset: 0,
            timestamp: 0,
            payload_type: PayloadType::Text,
            payload: vec![],
        }
    }
}

impl ConsumerResult {
    pub fn new(offset: u64, timestamp: u64, payload_type: PayloadType, payload: Vec<u8>) -> Self {
        ConsumerResult {
            offset,
            timestamp,
            payload_type,
            payload,
        }
    }


    pub fn to_bytes(&self) -> Vec<u8> {
        //| 8 bytes: offset | 8 bytes: timestamp | 2 bytes: payload type | 4 bytes: payload length | N bytes: payload |
        let mut bytes: Vec<u8> = Vec::new();
        bytes.extend_from_slice(&self.offset.to_be_bytes());
        bytes.extend_from_slice(&self.timestamp.to_be_bytes());
        bytes.extend_from_slice(&(self.payload_type as u16).to_be_bytes());
        bytes.extend_from_slice(&(self.payload.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&self.payload);
        bytes
    }
    pub fn from_bytes(tcp_stream: &mut TcpStream) -> std::io::Result<Self> {
        //| 8 bytes: offset | 8 bytes: timestamp | 2 bytes: payload type | 4 bytes: payload length | N bytes: payload |
        let mut buf = [0u8; 8]; // slice of 8 bytes
        tcp_stream.read_exact(&mut buf)?;
        let offset = u64::from_be_bytes(buf);

        let mut buf = [0u8; 8]; // slice of 8 bytes
        tcp_stream.read_exact(&mut buf)?;
        let timestamp = u64::from_be_bytes(buf);

        let mut buf = [0u8; 2]; // this creates a slice of 2 bytes initialized to 0 bytes
        tcp_stream.read_exact(&mut buf)?; // read the stream into the buf which is a u8 with 2 elements. at the end we will have a slice with 2 elements in it which represent PayloadType
        let payload_type = u16::from_be_bytes(buf); // this takes the u8 bytes and converts into a be u16. it is now a u16 value
        let payload_type = PayloadType::try_from(payload_type)?; // try from knows how to convert u16 to PayloadType

        let mut buf = [0u8; 4]; // this creates a slice with 4 elements all initialized to 0 of type unsigned int 8 bits as  u8 which is a byte
        tcp_stream.read_exact(&mut buf)?;
        let payload_length = u32::from_be_bytes(buf); // we are using big endian here to represent u32 over the wire. we could also use little endian but BE is easier to read left to write

        let mut payload = vec![0u8; payload_length as usize];
        tcp_stream.read_exact(&mut payload)?;

        Ok(Self {
            offset,
            timestamp,
            payload_type,
            payload,
        })
    }

    pub fn from_file(file: &mut File) -> std::io::Result<Self> {
        //| 8 bytes: offset | 8 bytes: timestamp | 2 bytes: payload type | 4 bytes: payload length | N bytes: payload |
        let mut buf = [0u8; 8]; // slice of 8 bytes
        file.read_exact(&mut buf)?;
        let offset = u64::from_be_bytes(buf);

        let mut buf = [0u8; 8]; // slice of 8 bytes
        file.read_exact(&mut buf)?;
        let timestamp = u64::from_be_bytes(buf);

        let mut buf = [0u8; 2]; // this creates a slice of 2 bytes initialized to 0 bytes
        file.read_exact(&mut buf)?; // read the stream into the buf which is a u8 with 2 elements. at the end we will have a slice with 2 elements in it which represent PayloadType
        let payload_type = u16::from_be_bytes(buf); // this takes the u8 bytes and converts into a be u16. it is now a u16 value
        let payload_type = PayloadType::try_from(payload_type)?; // try from knows how to convert u16 to PayloadType

        let mut buf = [0u8; 4]; // this creates a slice with 4 elements all initialized to 0 of type unsigned int 8 bits as  u8 which is a byte
        file.read_exact(&mut buf)?;
        let payload_length = u32::from_be_bytes(buf); // we are using big endian here to represent u32 over the wire. we could also use little endian but BE is easier to read left to write

        let mut payload = vec![0u8; payload_length as usize];
        file.read_exact(&mut payload)?;

        Ok(Self {
            offset,
            timestamp,
            payload_type,
            payload,
        })
    }

    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

    pub fn payload_type(&self) -> PayloadType {
        self.payload_type
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
    pub fn offset(&self) -> u64 {
        self.offset
    }
}