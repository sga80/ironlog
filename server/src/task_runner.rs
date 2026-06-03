use crate::commit_log::{CommitLogger, CommitLoggerImpl};
use compio::io::AsyncWriteExt;
use compio::net::TcpStream;
use compio::BufResult;
use flume::Receiver;
use ironlog_core::producer::ServerProducerFrame;
use ironlog_core::{ConsumerFrame, ProducerResult, RequestType, WriteStatus};
use std::io::{Error, ErrorKind};
use std::os::fd::{FromRawFd, RawFd};

pub struct TaskRunner {
    channel_name: String,
    commit_logger: CommitLoggerImpl,
    receiver: Receiver<(RawFd, RequestType)>,
}

impl TaskRunner {
    pub async fn new(channel_name: String, log_dir: String, receiver: Receiver<(RawFd, RequestType)>) -> Self {
        let commit_logger = CommitLoggerImpl::new(channel_name.clone(), log_dir).await.expect("commit logger to be created");
        TaskRunner {
            channel_name,
            commit_logger,
            receiver,
        }
    }
    pub async fn run(&mut self) {
        loop {
            let result = self.receiver.recv_async().await;
            match result {
                Ok((raw_fd, request_type)) => {
                    let mut tcp_stream = unsafe { TcpStream::from_raw_fd(raw_fd) };
                    if matches!(request_type,RequestType::Produce) {
                        self.handle_producer(&mut tcp_stream).await;
                    } else {
                        self.handle_consumer(&mut tcp_stream).await;
                    }
                }
                Err(e) => {
                    println!("failed to receive from the flume channel, failed with error {}", e);
                    break;
                }
            }
        }
    }

    async fn handle_producer(&mut self, mut tcp_stream: &mut TcpStream) {
        loop { // this loop blocks this thread until the producer finishes it. Handling this as a limitation for now.
            match ServerProducerFrame::from_bytes(&mut tcp_stream).await {
                Ok(request_frame) => {
                    let write_result = self.commit_logger.write_to_commit_log(request_frame, tcp_stream).await;
                    match write_result {
                        Ok(result) => {
                            let BufResult(res, _) = tcp_stream.write_all(result.to_bytes()).await;
                            if res.is_err() {
                                Self::write_error(&mut tcp_stream, res.unwrap_err()).await;
                            }
                        }
                        Err(e) => {
                            Self::write_error(&mut tcp_stream, e).await; // not handing error as the write itself failed to the client.
                        }
                    }
                }
                Err(e) if e.kind() == ErrorKind::UnexpectedEof => { // eof is returned if the client closes the connection
                    println!("client closed connection, returning with noop");
                    break;
                }
                Err(e) => Self::write_error(&mut tcp_stream, e).await,
            }
        }
    }

    async fn write_error(tcp_stream: &mut TcpStream, e: Error) {
        println!("failed with error {:?}", e);
        let result = ProducerResult {
            offset: 0,
            broker_timestamp: 0,
            status: WriteStatus::Failure,
            request_type: RequestType::Ack,
        };
        let BufResult(res, _) = tcp_stream.write_all(result.to_bytes()).await;
        if res.is_err() {
            println!("cannot write error back to the client, logging and failing . the error is {}", res.unwrap_err());
        }
    }
    async fn handle_consumer(&mut self, mut tcp_stream: &mut TcpStream) {
        // we need to loop here otherwise the tcpstream will close afte this function exits
        loop {
            // 1) get the consumer frame from the tcp stream
            match ConsumerFrame::from_bytes(&mut tcp_stream).await {
                Ok(consumer_frame) => {
                    // read the consumer result from the commit logger from the frame
                    let consumer_result = self.commit_logger.write_to_consumer(consumer_frame, tcp_stream).await;
                    if consumer_result.is_err() {
                        println!("error sending data to consumer , failed with error {}", consumer_result.unwrap_err());
                        break;
                    }
                }
                Err(e) => {
                    println!("error getting consumer frame , failed with error {}", e);
                    break;
                }
            }
        }
    }
}