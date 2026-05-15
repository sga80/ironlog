use ironlog_core::consumer::{ConsumerFrame, ConsumerResult};
use ironlog_core::{ProducerFrame, ProducerResult, RequestType, WriteStatus};
use std::collections::HashMap;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::io::{Error, ErrorKind};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use walkdir::WalkDir;

const FILE_NAME: &str = "1.dat";
struct ChannelState {
    file_descriptor: File,
    offset: u64,
}
pub struct CommitLogger {
    segments: HashMap<String, ChannelState>,
    log_dir: String,
}


impl CommitLogger {
    pub fn new(log_dir: String) -> Result<Self, Error> {
        // read the log for existing commit logs and set the current offset
        println!("walking the log dir {}", log_dir);
        let mut segments = HashMap::new();
        for entry in WalkDir::new(&log_dir).into_iter().filter_map(|e| e.ok()) {
            if entry.file_type().is_file() && entry.file_name().to_str().unwrap().ends_with(".dat") {
                // file is a commit log
                println!("file name is {}", entry.file_name().to_str().unwrap());
                let file_path = entry.into_path();
                let file_path_str = file_path.to_str().unwrap();
                let channel_name = file_path_str.replace(&log_dir, "");
                let channel_name = channel_name.replace(FILE_NAME, "");
                let channel_name = channel_name.replace("/", "");
                println!("channel_name is {}", channel_name);
                let mut file = File::open(&file_path)?;
                let mut result = None;
                loop {
                    let consumer_result = ConsumerResult::from_file(&mut file);
                    match consumer_result {
                        Ok(cr) => {
                            result = Option::Some(cr);
                        }
                        Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                            break; // reached eof
                        }
                        Err(e) => {
                            return Err(e);
                        }
                    }
                }


                let offset = result.unwrap_or_default().offset();
                println!("offset is {}", offset);
                let mut file_descriptor = OpenOptions::new().append(true).create(true).open(&file_path).expect("failed to create channel commit log");
                let state = ChannelState {
                    file_descriptor,
                    offset,
                };
                segments.insert(channel_name, state);
            }
        }
        segments.iter().for_each(|entry| {
            println!("channel is {}, channel offset is {}", entry.0, entry.1.offset);
        });
        Ok(CommitLogger {
            segments,
            log_dir,
        })
    }
    pub fn write_to_commit_log(&mut self, producer_frame: ProducerFrame) -> Result<(ProducerResult), Error> {
        // get the current file for the channel. this is maintained in a hashmap which allows to segment the files
        // the hashmap has channel name as the key and the open file
        // if the hashmap doesn't have it, this function creates a file in append only , writes to the file and adds it to the hashmap
        // by having the file stored in hashmap, we  are not opening and closing the file for every write
        // this is build for a single write , concurrency will be added later
        let channel_name = producer_frame.channel_name().to_string();

        // there is a bug with offset on restarts. When server restarts, it needs to read the file to get the current offset instead starting at 0 which will overwrite values
        let channel_state = self.segments.entry(channel_name).or_insert_with(|| {
            let channel_path = format!("{}/{}", self.log_dir, producer_frame.channel_name());
            let full_path = format!("{}/{}/{}", self.log_dir, producer_frame.channel_name(), FILE_NAME);
            // TODO: this will panic, but there is no proper solution as we will fight the borrow checker. the proper solution is or_try_insert_with which is not stable
            fs::create_dir_all(channel_path).expect("failed to create channel directory");
            let file_descriptor = OpenOptions::new().append(true).create(true).open(full_path).expect("failed to create channel commit log");
            ChannelState { file_descriptor, offset: 0 }
        });

        let mut bytes: Vec<u8> = Vec::new();
        let broker_ts = SystemTime::now();
        let current_epoch = broker_ts.duration_since(UNIX_EPOCH).expect("time should go forward");
        let current_offset = channel_state.offset;

        // commit log format
        // | 8 bytes: offset | 8 bytes: timestamp | 2 bytes: payload type | 4 bytes: payload length | N bytes: payload |
        bytes.extend_from_slice(&(channel_state.offset).to_be_bytes()); // 8 bytes
        bytes.extend_from_slice(&(current_epoch.as_millis() as u64).to_be_bytes()); // 8 bytes for timestamp
        bytes.extend_from_slice(&(producer_frame.payload_type as u16).to_be_bytes());  // 2 bytes
        bytes.extend_from_slice(&(producer_frame.payload().len() as u32).to_be_bytes());  // 4 bytes
        bytes.extend_from_slice(producer_frame.payload());

        channel_state.file_descriptor.write_all(&bytes)?;

        //increment the offset.
        channel_state.offset += 1;
        println!(" offset after write is is {}", channel_state.offset);

        Ok(ProducerResult {
            offset: current_offset,
            broker_timestamp: (current_epoch.as_millis() as u64),
            status: WriteStatus::Success,
            request_type: RequestType::Ack,
        })
    }

    pub fn read_from_commit_log(&mut self, consumer_frame: ConsumerFrame) -> Result<Vec<ConsumerResult>, Error> {
        // this is slightly easier as we are only doing reads and not writes. We shouldn't be creating file descriptors here
        // get the file_descriptor from the hashmap
        let mut result: Vec<ConsumerResult> = Vec::new();

        let full_path = format!("{}/{}/{}", self.log_dir, consumer_frame.channel_name(), FILE_NAME);
        if Path::new(&full_path).exists() {
            let mut file = File::open(full_path)?;

            loop {
                let consumer_result = ConsumerResult::from_file(&mut file);
                match consumer_result {
                    Ok(cr) => {
                        if cr.offset() >= consumer_frame.offset() {
                            result.push(cr);
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
        }
        Ok(result)
    }
}