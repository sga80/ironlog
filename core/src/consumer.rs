use crate::PayloadType;
use compio::buf::IoBufMut;
use compio::fs::File;
use compio::io::{AsyncReadAtExt, AsyncReadExt};
use compio::net::TcpStream;
use compio::BufResult;
use std::io::Error;

#[derive(Clone, Debug)]
pub struct ConsumerFrame {
    message_offset: u64,
}

impl ConsumerFrame {
    pub fn new(offset: Option<u64>) -> Self {
        let offset_value = offset.unwrap_or_default();
        ConsumerFrame {
            message_offset: offset_value,
        }
    }
    pub fn offset(&self) -> u64 {
        self.message_offset
    }
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes: Vec<u8> = Vec::new();
        bytes.extend_from_slice(&self.message_offset.to_be_bytes());
        bytes
    }

    pub async fn from_bytes(tcp_stream: &mut TcpStream) -> std::io::Result<Self> {
        let buf = [0u8; 8]; // slice of 8 bytes
        let BufResult(res, buf) = tcp_stream.read_exact(buf).await;
        if res.is_err() {
            return Err(res.unwrap_err());
        }
        let offset = u64::from_be_bytes(buf);
        Ok(Self {
            message_offset: offset,
        })
    }
}

#[derive(Clone, Debug, Default)]
pub struct ConsumerMetadata {
    offset: u64,
    timestamp: u64,
    payload_type: PayloadType,
    payload_byte_offset: u64,
    payload_length: u32,
    is_end_of_file: bool,
}
impl ConsumerMetadata {
    pub fn new_with_eof() -> Self {
        let mut cm = ConsumerMetadata::default();
        cm.is_end_of_file = true;
        cm
    }
    pub fn to_bytes(&self) -> Vec<u8> {
        // | 8 bytes: offset | 8 bytes: timestamp | 2 bytes: payload type | 4 bytes: payload length | 1 byte: end_of_file
        // we are not going to send the payload offset here as it doesnt make sense
        let mut bytes: Vec<u8> = Vec::new();
        bytes.extend_from_slice(&self.offset.to_be_bytes());
        bytes.extend_from_slice(&self.timestamp.to_be_bytes());
        bytes.extend_from_slice(&(self.payload_type as u16).to_be_bytes());
        bytes.extend_from_slice(&self.payload_length.to_be_bytes());
        bytes.extend_from_slice(&(self.is_end_of_file as u8).to_be_bytes());

        bytes
    }

    pub async fn from_bytes(tcp_stream: &mut TcpStream) -> std::io::Result<Self> {
        // | 8 bytes: offset | 8 bytes: timestamp | 2 bytes: payload type | 4 bytes: payload length | 1 byte: end_of_file
        let offset = tcp_stream.read_u64().await?;
        let timestamp = tcp_stream.read_u64().await?;
        let payload_type = tcp_stream.read_u16().await?; // this takes the u8 bytes and converts into a be u16. it is now a u16 value
        let payload_type = PayloadType::try_from(payload_type)?; // try from knows how to convert u16 to PayloadType
        let payload_length = tcp_stream.read_u32().await?; // we are using big endian here to represent u32 over the wire. we could also use little endian but BE is easier to read left to write
        let is_end_of_file = tcp_stream.read_u8().await?;
        let is_end_of_file: bool = is_end_of_file == 1; // true
        Ok(Self {
            offset,
            timestamp,
            payload_type,
            payload_byte_offset: 0,
            payload_length,
            is_end_of_file,
        })
    }


    pub async fn from_file(file: &mut File, mut byte_offset: u64) -> Result<(ConsumerMetadata, u64), Error> {
        // | 8 bytes: offset | 8 bytes: timestamp | 2 bytes: payload type | 4 bytes: payload length | 8 bytes: byte_offset_in_payload_file
        let buf = [0u8; 8]; // slice of 8 bytes
        let BufResult(res, buf) = file.read_exact_at(buf, byte_offset).await;
        if res.is_err() {
            return Err(res.unwrap_err());
        }
        let payload_offset = u64::from_be_bytes(buf);
        byte_offset += buf.len() as u64;

        let buf = [0u8; 8]; // slice of 8 bytes
        let BufResult(res, buf) = file.read_exact_at(buf, byte_offset).await;
        if res.is_err() {
            return Err(res.unwrap_err());
        }
        let timestamp = u64::from_be_bytes(buf);
        byte_offset += buf.len() as u64;


        let buf = [0u8; 2]; // this creates a slice of 2 bytes initialized to 0 bytes
        let BufResult(res, buf) = file.read_exact_at(buf, byte_offset).await; // read the stream into the buf which is a u8 with 2 elements. at the end we will have a slice with 2 elements in it which represent PayloadType
        if res.is_err() {
            return Err(res.unwrap_err());
        }
        let payload_type = u16::from_be_bytes(buf); // this takes the u8 bytes and converts into a be u16. it is now a u16 value
        let payload_type = PayloadType::try_from(payload_type)?; // try from knows how to convert u16 to PayloadType
        byte_offset += buf.len() as u64;


        let buf = [0u8; 4]; // this creates a slice with 4 elements all initialized to 0 of type unsigned int 8 bits as  u8 which is a byte
        let BufResult(res, buf) = file.read_exact_at(buf, byte_offset).await;
        if res.is_err() {
            return Err(res.unwrap_err());
        }
        let payload_length = u32::from_be_bytes(buf); // we are using big endian here to represent u32 over the wire. we could also use little endian but BE is easier to read left to write
        byte_offset += buf.len() as u64;

        let buf = [0u8; 8]; // slice of 8 bytes
        let BufResult(res, buf) = file.read_exact_at(buf, byte_offset).await;
        if res.is_err() {
            return Err(res.unwrap_err());
        }
        let payload_byte_offset = u64::from_be_bytes(buf);
        byte_offset += buf.len() as u64;

        Ok(((Self {
            offset: payload_offset,
            timestamp,
            payload_type,
            payload_byte_offset,
            payload_length,
            is_end_of_file: false,
        }), byte_offset))
    }

    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

    pub fn payload_type(&self) -> PayloadType {
        self.payload_type
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn payload_byte_offset(&self) -> u64 {
        self.payload_byte_offset
    }
    pub fn payload_length(&self) -> u32 {
        self.payload_length
    }

    pub fn is_end_of_file(&self) -> bool {
        self.is_end_of_file
    }
}

pub async fn payload_from_bytes(tcp_stream: &mut TcpStream, payload: Vec<u8>) -> std::io::Result<Vec<u8>> {
    let BufResult(res, payload) = tcp_stream.read_exact(payload).await;
    if res.is_err() {
        return Err(res.unwrap_err());
    }
    Ok(payload)
}
