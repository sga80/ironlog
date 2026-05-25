use crate::RequestType;
use compio::buf::IoBufMut;
use compio::io::AsyncReadExt;
use compio::net::TcpStream;
use compio::BufResult;
/*
This struct is the initial handshake which producers and consumers send and server receives before we do anything
 */
#[derive(Debug)]
pub struct Handshake {
    channel_name: String,
    request_type: RequestType,
}

impl Handshake {
    pub fn new(channel_name: String, request_type: RequestType) -> Self {
        Handshake {
            channel_name,
            request_type,
        }
    }
    pub fn request_type(&self) -> RequestType {
        self.request_type
    }
    pub fn channel_name(&self) -> &str {
        &self.channel_name
    }
    pub fn to_bytes(&self) -> Vec<u8> {
        let request_type = self.request_type;
        let mut bytes: Vec<u8> = Vec::new();
        bytes.push(self.channel_name.len() as u8); // 1 byte
        bytes.extend_from_slice(self.channel_name.as_bytes());
        bytes.extend_from_slice(&(request_type as u16).to_be_bytes());
        bytes
    }
}
pub async fn handshake_from_bytes(tcp_stream: &mut TcpStream) -> std::io::Result<Handshake> {
    let channel_name_length = tcp_stream.read_u8().await?; // one byte for channel length
    let buf = Vec::with_capacity(channel_name_length as usize);
    let BufResult(res, buf) = tcp_stream.read_exact(buf).await;
    res?; // propagate any error
    let channel_name_str = str::from_utf8(&buf).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let channel_name = channel_name_str.to_string();
    let request_type = tcp_stream.read_u16().await?;
    let request_type = RequestType::try_from(request_type)?;
    Ok(Handshake {
        channel_name,
        request_type,
    })
}