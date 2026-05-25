use ironlog_core::{ConsumerFrame, ConsumerResult, ProducerFrame, ProducerResult, RequestType, WriteStatus};
use std::fs;

use compio::fs::{File, OpenOptions};
use compio::io::AsyncWriteAt;
use compio::BufResult;
use std::io::{Error, ErrorKind};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

const FILE_NAME: &str = "1.log";

pub trait CommitLogger {
    async fn new(channel_name: String, commit_log_dir: String) -> Result<Self, Error>
    where
        Self: Sized;

    async fn write_to_commit_log(&mut self, producer_frame: ProducerFrame) -> Result<(ProducerResult), Error>;
    async fn read_from_commit_log(&mut self, consumer_frame: ConsumerFrame) -> Result<(Vec<ConsumerResult>), Error>;
}
pub struct CommitLoggerImpl {
    channel_name: String,
    channel_state: ChannelState,
}

struct ChannelState {
    file_descriptor: File,
    message_offset: u64,
    byte_offset: u64,
}

impl CommitLogger for CommitLoggerImpl {
    async fn new(channel_name: String, log_dir: String) -> Result<Self, Error> {
        // remove any trailing /'s from commit_log_dir
        let mut commit_log_dir: String = log_dir.clone();
        if log_dir.ends_with("/") {
            commit_log_dir = log_dir.get(0..log_dir.len() - 1).map(str::to_string).unwrap();
        }
        let channel_path = format!("{}/{}", commit_log_dir, channel_name);
        let full_path = format!("{}/{}/{}", commit_log_dir, channel_name, FILE_NAME);
        let path = PathBuf::from(full_path.clone());
        if path.exists() {
            let mut file = File::open(full_path.clone()).await?;
            let mut result = None;
            let mut byte_offset: u64 = 0; // this byte_offset is passed to the ConsumerResult so that it keeps incremented to the offset after reading the frame
            loop {
                let consumer_result = ConsumerResult::from_file(&mut file, byte_offset).await;
                match consumer_result {
                    Ok(cr) => {
                        result = Option::Some(cr.0);
                        byte_offset = cr.1;
                    }
                    Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                        break; // reached eof
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
            }


            let offset = result.unwrap_or_default().offset() + 1; // increment by 1 so that the next write happens after this
            let file_descriptor = OpenOptions::new().create(true).write(true).read(true).open(full_path).await?;
            let channel_state = ChannelState {
                file_descriptor,
                message_offset: offset,
                byte_offset,
            };
            Ok(CommitLoggerImpl {
                channel_name,
                channel_state,
            })
        } else {
            println!("path doesnt' exist");
            // path doesnt exist, create the dir
            // TODO: this will panic, but there is no proper solution as we will fight the borrow checker. the proper solution is or_try_insert_with which is not stable
            fs::create_dir_all(channel_path).expect("failed to create channel directory");
            let file_result = OpenOptions::new().create(true).write(true).read(true).open(full_path).await;
            if file_result.is_err() {
                let err = file_result.unwrap_err();
                println!("error in opening file {}", err);
                return Err(err);
            }
            let channel_state = ChannelState { file_descriptor: file_result?, message_offset: 0, byte_offset: 0 };
            Ok(CommitLoggerImpl {
                channel_name,
                channel_state,
            })
        }
    }

    async fn write_to_commit_log(&mut self, producer_frame: ProducerFrame) -> Result<(ProducerResult), Error> {
        let mut bytes: Vec<u8> = Vec::new();
        let broker_ts = SystemTime::now();
        let current_epoch = broker_ts.duration_since(UNIX_EPOCH).expect("time should go forward");
        let current_offset = self.channel_state.message_offset;

        // commit log format
        // | 8 bytes: offset | 8 bytes: timestamp | 2 bytes: payload type | 4 bytes: payload length | N bytes: payload |
        bytes.extend_from_slice(&(self.channel_state.message_offset).to_be_bytes()); // 8 bytes
        bytes.extend_from_slice(&(current_epoch.as_millis() as u64).to_be_bytes()); // 8 bytes for timestamp
        bytes.extend_from_slice(&(producer_frame.payload_type as u16).to_be_bytes());  // 2 bytes
        bytes.extend_from_slice(&(producer_frame.payload().len() as u32).to_be_bytes());  // 4 bytes
        bytes.extend_from_slice(producer_frame.payload());


        let BufResult(res, bytes) = self.channel_state.file_descriptor.write_at(bytes, self.channel_state.byte_offset).await;
        if res.is_err() {
            return Err(res.unwrap_err());
        }

        //increment the offsets.
        self.channel_state.message_offset += 1;
        self.channel_state.byte_offset += bytes.len() as u64;
        println!(" message after write is  {} and byte offset after write is {}", self.channel_state.message_offset, self.channel_state.byte_offset);

        Ok(ProducerResult {
            offset: current_offset,
            broker_timestamp: (current_epoch.as_millis() as u64),
            status: WriteStatus::Success,
            request_type: RequestType::Ack,
        })
    }

    async fn read_from_commit_log(&mut self, consumer_frame: ConsumerFrame) -> Result<(Vec<ConsumerResult>), Error> {
        let mut byte_offset: u64 = 0;
        let mut results = Vec::new();
        loop {
            //let consumer_result = self.read_from_file(byte_offset).await;
            let consumer_result = ConsumerResult::from_file(&mut self.channel_state.file_descriptor, byte_offset).await;
            match consumer_result {
                Ok(cr) => {
                    let result = cr.0;
                    byte_offset = cr.1;
                    if result.offset() >= consumer_frame.offset() {
                        results.push(result);
                    }
                }
                Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                    break; // reached eof
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
        Ok(results)
    }
}

