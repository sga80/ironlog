pub mod commit_logger;

use crate::commit_logger::CommitLogger;
use ironlog_core::consumer::ConsumerFrame;
use ironlog_core::{ProducerFrame, ProducerResult, RequestType, WriteStatus};
use std::io::{Error, ErrorKind, Read, Write};
use std::net::{TcpListener, TcpStream};

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

fn main() -> std::io::Result<()> {
    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();
    
    println!("server");
    let mut commit_logger = CommitLogger::new(String::from("/Users/shyamgavulla/ironlog/data")).unwrap();
    let listener = TcpListener::bind("127.0.0.1:4000")?;
    for stream in listener.incoming() {
        let mut stream = stream?;
        let mut buf = [0u8; 2]; // create a slice of u8 with 2 elements which is 2 bytes
        stream.read_exact(&mut buf)?;
        let request_type = u16::from_be_bytes(buf);
        let request_type = RequestType::try_from(request_type)?;
        match request_type {
            RequestType::Produce => { handle_producer(&mut commit_logger, stream)? }
            RequestType::Fetch => { handle_consumer(&mut commit_logger, stream)? }

            _ => { println!("ack not handled on the server side") } // TODO add custom error message here
        }
    }

    Ok(())
}
fn handle_consumer(commit_logger: &mut CommitLogger, mut tcp_stream: TcpStream) -> std::io::Result<()> {

    // 1) get the consumer frame from the tcp stream
    match ConsumerFrame::from_bytes(&mut tcp_stream) {
        Ok(consumer_frame) => {
            println!("consumer frame is {:?}", consumer_frame);
            // read the consumer result from the commit logger from the frame
            let consumer_result = commit_logger.read_from_commit_log(consumer_frame);
            match consumer_result {
                Ok(result) => {
                    for consumer_result in result {
                        tcp_stream.write_all(&consumer_result.to_bytes())?;
                    }
                    Ok(())
                }
                Err(e) => {
                    println!("not handling the error {} now ", e);
                    Err(e)
                }
            }
        }
        Err(e) if e.kind() == ErrorKind::UnexpectedEof => { // eof is returned if the client closes the connection
            println!("consumer client closed connection, returning with noop");
            Ok(())
        }
        Err(e) => {
            println!("failed to read from consumer, failed with error {}", e);
            Err(e)
        }
    }
}

fn handle_producer(commit_logger: &mut CommitLogger, mut tcp_stream: TcpStream) -> std::io::Result<()> {
    loop {
        // 1) get requestframe from the tcpstream
        // 2) call the commitlogger to write to a file for the channel
        match ProducerFrame::from_bytes(&mut tcp_stream) {
            Ok(request_frame) => {
                let write_result = commit_logger.write_to_commit_log(request_frame);
                match write_result {
                    Ok(result) => { tcp_stream.write_all(&result.to_bytes())?; }
                    Err(e) => {
                        write_error(&mut tcp_stream, e)?; // not handing error as the write itself failed to the client.
                    }
                }
            }
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => { // eof is returned if the client closes the connection
                println!("client closed connection, returning with noop");
                break;
            }
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

fn write_error(tcp_stream: &mut TcpStream, e: Error) -> Result<(), Error> {
    println!("failed with error {:?}", e);
    let result = ProducerResult {
        offset: 0,
        broker_timestamp: 0,
        status: WriteStatus::Failure,
        request_type: RequestType::Ack,
    };
    tcp_stream.write_all(&result.to_bytes())?;
    Ok(())
}