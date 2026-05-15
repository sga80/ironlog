use ironlog_core::{ConsumerFrame, ConsumerResult, RequestType};
use std::io::{Error, ErrorKind, Write};
use std::net::TcpStream;
pub struct Consumer {
    tcp_stream: TcpStream,
}

impl Consumer {
    pub fn new(host: String, port: u16) -> Result<Self, Error> {
        let mut tcp_stream = TcpStream::connect(format!("{}:{}", host, port)).expect("cannot connect to server");
        let request_type = (RequestType::Fetch as u16).to_be_bytes();
        tcp_stream.write_all(&request_type)?;
        Ok(Consumer {
            tcp_stream,
        })
    }

    pub fn fetch(&mut self, channel_name: String, offset: Option<u64>) -> Result<Vec<ConsumerResult>, Error> {
        let mut results: Vec<ConsumerResult> = Vec::new();
        let consumer_frame = ConsumerFrame::new(channel_name, offset);
        let result = self.tcp_stream.write_all(&consumer_frame.to_bytes());
        match result {
            Ok(_) => {
                loop {
                    let consumer_result = ConsumerResult::from_bytes(&mut self.tcp_stream);
                    match consumer_result {
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
            }
            Err(e) => { return Err(e) }
        }

        Ok(results)
    }
}