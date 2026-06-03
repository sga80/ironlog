use compio::buf::IoBufMut;

use compio::fs::{File, OpenOptions};
use compio::io::{AsyncReadAtExt, AsyncReadExt, AsyncWriteAt, AsyncWriteExt};
use compio::net::TcpStream;
use compio::BufResult;
use ironlog_core::producer::ServerProducerFrame;
use ironlog_core::{ConsumerFrame, ConsumerMetadata, ProducerResult, RequestType, WriteStatus};
use std::fs;
use std::io::Error;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

const PAYLOAD_FILE_NAME: &str = "1.payload";
const METADATA_FILE_NAME: &str = "1.metadata";

const METADATA_HEADER_BYTE_LENGTH: u64 = 30;

pub trait CommitLogger {
    async fn new(channel_name: String, commit_log_dir: String) -> Result<Self, Error>
    where
        Self: Sized;

    async fn write_to_commit_log(&mut self, producer_frame: ServerProducerFrame, tcp_stream: &mut TcpStream) -> Result<(ProducerResult), Error>;
    async fn write_payload(&mut self, tcp_stream: &mut TcpStream, payload_length: usize) -> std::io::Result<()>;
    async fn write_to_consumer(&mut self, consumer_frame: ConsumerFrame, tcp_stream: &mut TcpStream) -> Result<(), Error>;

    async fn write_payload_to_stream(&mut self, tcp_stream: &mut TcpStream, payload_length: usize, payload_offset: u64) -> std::io::Result<()>;
}
pub struct CommitLoggerImpl {
    channel_name: String,
    channel_state: ChannelState,
}

#[derive(Debug)]
struct ChannelState {
    payload_file_descriptor: File,
    metadata_file_descriptor: File,
    message_offset: u64,
    payload_byte_offset: u64,
    metadata_byte_offset: u64,
    #[cfg(all(target_os = "linux", feature = "splice"))]
    write_pipe: (compio::fs::pipe::Receiver, compio::fs::pipe::Sender),
    #[cfg(all(target_os = "linux", feature = "splice"))]
    read_pipe: (compio::fs::pipe::Receiver, compio::fs::pipe::Sender),
}

impl CommitLogger for CommitLoggerImpl {
    async fn new(channel_name: String, log_dir: String) -> Result<Self, Error> {
        // remove any trailing /'s from commit_log_dir
        let mut commit_log_dir: String = log_dir.clone();
        if log_dir.ends_with("/") {
            commit_log_dir = log_dir.get(0..log_dir.len() - 1).map(str::to_string).unwrap();
        }
        let channel_path = format!("{}/{}", commit_log_dir, channel_name);
        let payload_full_path = format!("{}/{}/{}", commit_log_dir, channel_name, PAYLOAD_FILE_NAME);
        let metadata_full_path = format!("{}/{}/{}", commit_log_dir, channel_name, METADATA_FILE_NAME);
        let payload_path = PathBuf::from(payload_full_path.clone());
        let metadata_path = PathBuf::from(metadata_full_path.clone());
        //these 2 panics are OK here. the system cannot recover from this error
        if payload_path.exists() && !metadata_path.exists() {
            panic!("metadata file missing for channel {}", channel_name);
        }
        if metadata_path.exists() && !payload_path.exists() {
            panic!("payload file missing for channel {}", channel_name);
        }
        if payload_path.exists() && metadata_path.exists() {
            let mut metadata_file = File::open(metadata_path.clone()).await?;
            //the logic to get the current offset is simple. find the total file size and divide it by 30
            let metadata_file_size = metadata_file.metadata().await?.len();
            let message_offset = metadata_file_size / METADATA_HEADER_BYTE_LENGTH; // this gives next offset
            // now read the last message offset to get the payload byte offset
            let mut payload_byte_offset = 0;
            // the logic is that we get the total bytes, say it is 150. we know each header is 30 bytes, so subtract 30 which is 120. this is the offset from where we need to reac to get the
            // last medata header so that we can get the byte offser
            let consumer_result = ConsumerMetadata::from_file(&mut metadata_file, metadata_file_size - METADATA_HEADER_BYTE_LENGTH).await;
            match consumer_result {
                Ok(cr) => {
                    let result = cr.0;
                    // result.payload_byte_offset returns the current payload offset , we need to add the payload length so that we can start writing from the last one
                    payload_byte_offset = result.payload_byte_offset() + result.payload_length() as u64;
                }
                Err(e) => {
                    return Err(e);
                }
            }

            let file_descriptor = OpenOptions::new().create(true).write(true).read(true).open(payload_full_path).await?;
            let metadata_file_descriptor = OpenOptions::new().create(true).write(true).read(true).open(metadata_full_path).await?;

            let channel_state = ChannelState {
                payload_file_descriptor: file_descriptor,
                metadata_file_descriptor: metadata_file_descriptor,
                message_offset,
                payload_byte_offset,
                metadata_byte_offset: metadata_file_size,
                #[cfg(all(target_os = "linux", feature = "splice"))]
                write_pipe: compio::fs::pipe::anonymous().await?,
                #[cfg(all(target_os = "linux", feature = "splice"))]
                read_pipe: compio::fs::pipe::anonymous().await?,
            };
            println!("channel state is {:?}", channel_state);
            Ok(CommitLoggerImpl {
                channel_name,
                channel_state,
            })
        } else {
            // path doesnt exist, create the dir
            // TODO: this will panic, but there is no proper solution as we will fight the borrow checker. the proper solution is or_try_insert_with which is not stable
            fs::create_dir_all(channel_path).expect("failed to create channel directory");
            let payload_file_result = OpenOptions::new().create(true).write(true).read(true).open(payload_full_path).await;
            if payload_file_result.is_err() {
                let err = payload_file_result.unwrap_err();
                println!("error in opening file {}", err);
                return Err(err);
            }
            let metadata_file_result = OpenOptions::new().create(true).write(true).read(true).open(metadata_full_path).await;
            if metadata_file_result.is_err() {
                let err = metadata_file_result.unwrap_err();
                println!("error in opening file {}", err);
                return Err(err);
            }
            let channel_state = ChannelState {
                payload_file_descriptor: payload_file_result?,
                message_offset: 0,
                payload_byte_offset: 0,
                metadata_file_descriptor: metadata_file_result?,
                metadata_byte_offset: 0,
                #[cfg(all(target_os = "linux", feature = "splice"))]
                write_pipe: compio::fs::pipe::anonymous().await?,
                #[cfg(all(target_os = "linux", feature = "splice"))]
                read_pipe: compio::fs::pipe::anonymous().await?,
            };
            Ok(CommitLoggerImpl {
                channel_name,
                channel_state,
            })
        }
    }

    async fn write_to_commit_log(&mut self, server_producer_frame: ServerProducerFrame, tcp_stream: &mut TcpStream) -> Result<(ProducerResult), Error> {
        let current_payload_offset = self.channel_state.payload_byte_offset;
        // first write to the payload file
        self.write_payload(tcp_stream, server_producer_frame.payload_length() as usize).await?;
        // increment the payload byte offset
        self.channel_state.payload_byte_offset += server_producer_frame.payload_length() as u64;


        let mut bytes: Vec<u8> = Vec::new();
        let broker_ts = SystemTime::now();
        let current_epoch = broker_ts.duration_since(UNIX_EPOCH).expect("time should go forward");
        let current_offset = self.channel_state.message_offset;

        // commit log format
        // | 8 bytes: offset | 8 bytes: timestamp | 2 bytes: payload type | 4 bytes: payload length | 8 bytes: byte_offset_in_payload_file
        bytes.extend_from_slice(&self.channel_state.message_offset.to_be_bytes()); // 8 bytes
        bytes.extend_from_slice(&(current_epoch.as_millis() as u64).to_be_bytes()); // 8 bytes for timestamp
        bytes.extend_from_slice(&(server_producer_frame.payload_type() as u16).to_be_bytes());  // 2 bytes
        bytes.extend_from_slice(&(server_producer_frame.payload_length() as u32).to_be_bytes());  // 4 bytes
        bytes.extend_from_slice(&current_payload_offset.to_be_bytes()); // append the payload offset which we were we start writing the payload.

        let BufResult(res, bytes) = self.channel_state.metadata_file_descriptor.write_at(bytes, self.channel_state.metadata_byte_offset).await;
        if res.is_err() {
            return Err(res.unwrap_err());
        }

        //increment the offsets.
        self.channel_state.message_offset += 1;
        self.channel_state.metadata_byte_offset += bytes.len() as u64;

        Ok(ProducerResult {
            offset: current_offset,
            broker_timestamp: (current_epoch.as_millis() as u64),
            status: WriteStatus::Success,
            request_type: RequestType::Ack,
        })
    }

    #[cfg(all(target_os = "linux", feature = "splice"))]
    async fn write_payload(&mut self, tcp_stream: &mut TcpStream, payload_length: usize) -> std::io::Result<()> {
        use compio::fs::pipe::splice;
        use std::cmp::min;

        let payload_file = &self.channel_state.payload_file_descriptor;

        //splice signature is
        //pub fn splice(fd_in, fd_out, len: usize) -> Splice
        // the first iteration we first read the entire payload from the stream and wrote it to the file. However Claude told me about the limitation of 64KB on linux, so we will need to read and write in
        // a single loop than 2 loops
        let mut remaining = payload_length;
        let mut current_payload_byte_offset = self.channel_state.payload_byte_offset as i64;
        while remaining > 0 {
            let chunk = min(remaining, 65_536); // min of remaining and 64KB
            let bytes_transferred = splice(tcp_stream, &self.channel_state.write_pipe.1, chunk).await?; // write from tcp stream to sender
            let bytes_written = splice(&self.channel_state.write_pipe.0, payload_file, bytes_transferred).offset_out(current_payload_byte_offset).await?; // write from receiver side of the pipe to file
            remaining -= bytes_transferred; // reduce the remaining with the amount of bytes transferred
            current_payload_byte_offset += bytes_written as i64; // move the offset by the bytes written value
        }


        Ok(())
    }
    #[cfg(not(all(target_os = "linux", feature = "splice")))]
    async fn write_payload(&mut self, tcp_stream: &mut TcpStream, payload_length: usize) -> std::io::Result<()> {
        println!("using memory to write the payload");
        // read the payload  into a vec and write it to the payload file
        let payload = Vec::with_capacity(payload_length);
        let BufResult(res, payload) = tcp_stream.read_exact(payload).await;
        res?; //propagate the error
        let BufResult(res, bytes) = self.channel_state.payload_file_descriptor.write_at(payload, self.channel_state.payload_byte_offset).await;
        if res.is_err() {
            return Err(res.unwrap_err());
        }
        Ok(())
    }

    async fn write_to_consumer(&mut self, consumer_frame: ConsumerFrame, tcp_stream: &mut TcpStream) -> Result<(), Error> {
        let metadata_byte_offset = METADATA_HEADER_BYTE_LENGTH * consumer_frame.offset();
        //the logic to get the current offset is simple. find the total file size and divide it by 30
        let metadata_file_size = &self.channel_state.metadata_file_descriptor.metadata().await?.len();
        let total_message_offsets = metadata_file_size / METADATA_HEADER_BYTE_LENGTH; // this gives next offset
        if consumer_frame.offset() >= total_message_offsets {
            let eof = ConsumerMetadata::new_with_eof();
            let BufResult(res, _) = tcp_stream.write_all(eof.to_bytes()).await;
            if res.is_err() {
                return Err(res.unwrap_err());
            }
            return Ok(());
        }

        let consumer_result = ConsumerMetadata::from_file(&mut self.channel_state.metadata_file_descriptor, metadata_byte_offset).await;
        match consumer_result {
            Ok(cr) => {
                let result = cr.0;
                //write the metadata header to the stream
                let BufResult(res, _) = tcp_stream.write_all(result.to_bytes()).await;
                if res.is_err() {
                    return Err(res.unwrap_err());
                }
                // now write the payload to the tcp stream
                self.write_payload_to_stream(tcp_stream, result.payload_length() as usize, result.payload_byte_offset()).await?;
            }
            Err(e) => {
                return Err(e);
            }
        }

        Ok(())
    }
    #[cfg(all(target_os = "linux", feature = "splice"))]
    async fn write_payload_to_stream(&mut self, tcp_stream: &mut TcpStream, payload_length: usize, payload_offset: u64) -> std::io::Result<()> {
        use compio::fs::pipe::splice;
        use std::cmp::min;
        println!("using splice");
        let payload_file = &self.channel_state.payload_file_descriptor;

        //splice signature is
        //pub fn splice(fd_in, fd_out, len: usize) -> Splice
        // the first iteration we first read the entire payload from the stream and wrote it to the file. However Claude told me about the limitation of 64KB on linux, so we will need to read and write in
        // a single loop than 2 loops
        let mut remaining = payload_length;
        let mut current_payload_byte_offset = payload_offset as i64;
        while remaining > 0 {
            let chunk = min(remaining, 65_536); // min of remaining and 64KB
            let bytes_transferred = splice(payload_file, &self.channel_state.write_pipe.1, chunk).offset_in(current_payload_byte_offset).await?; // write from file to sender
            let bytes_written = splice(&self.channel_state.write_pipe.0, tcp_stream, bytes_transferred).await?; // write from receiver side to tcp stream
            remaining -= bytes_transferred; // reduce the remaining with the amount of bytes transferred
            current_payload_byte_offset += bytes_written as i64; // move the offset by the bytes written value
        }


        Ok(())
    }
    #[cfg(not(all(target_os = "linux", feature = "splice")))]
    async fn write_payload_to_stream(&mut self, tcp_stream: &mut TcpStream, payload_length: usize, payload_offset: u64) -> std::io::Result<()> {
        // read the payload  into a vec and write it to the payload file
        let payload = Vec::with_capacity(payload_length);
        let BufResult(res, payload) = self.channel_state.payload_file_descriptor.read_exact_at(payload, payload_offset).await;
        if res.is_err() {
            return Err(res.unwrap_err());
        }

        let BufResult(res, bytes) = tcp_stream.write_all(payload).await;
        if res.is_err() {
            return Err(res.unwrap_err());
        }
        Ok(())
    }
}

