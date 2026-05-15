use ironlog_core::{PayloadType, ProducerFrame, ProducerResult, RequestType};
use std::io::{Error, Write};
use std::net::TcpStream;

pub struct Producer {
    host: String,
    port: u16,
    tcp_stream: TcpStream,
}

impl Producer {
    pub fn new(host: String, port: u16) -> Result<Self, Error> {
        let mut tcp_stream = TcpStream::connect(format!("{}:{}", host, port))?;
        // first connect and write the request type
        let request_type = (RequestType::Produce as u16).to_be_bytes();
        tcp_stream.write_all(&request_type)?;
        Ok(Producer { host, port, tcp_stream })
    }

    pub fn send(&mut self, channel: String, payload_type: PayloadType, payload: &[u8]) -> Result<ProducerResult, Error> {
        let request_frame = ProducerFrame::new(payload_type, channel, payload.to_vec());
        let result = self.tcp_stream.write_all(&request_frame.to_bytes());
        match result {
            Ok(_) => { ProducerResult::from_bytes(&mut self.tcp_stream) }
            Err(_) => {
                self.connect()?;
                self.tcp_stream.write_all(&request_frame.to_bytes())?;
                ProducerResult::from_bytes(&mut self.tcp_stream)
            }
        }
    }


    fn connect(&mut self) -> Result<(), Error> {
        let mut tcp_stream = TcpStream::connect(format!("{}:{}", self.host, self.port))?;
        // first connect and write the request type
        let request_type = (RequestType::Produce as u16).to_be_bytes();
        tcp_stream.write_all(&request_type)?;
        self.tcp_stream = tcp_stream;
        Ok(())
    }
}