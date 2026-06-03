use crate::task_runner::TaskRunner;
use compio::runtime::spawn;
use flume::Receiver;
use ironlog_core::{Handshake, RequestType};
use std::collections::HashMap;
use std::os::fd::RawFd;

pub struct Worker {
    thread_name: String,
    receiver: Receiver<(RawFd, Handshake)>,
    commit_log_dir: String,
}

impl Worker {
    pub async fn new(thread_name: String, commit_log_dir: String, receiver: Receiver<(RawFd, Handshake)>) -> Self {
        Worker {
            thread_name,
            receiver,
            commit_log_dir,
        }
    }

    pub async fn start_worker(mut self) {
        let mut runners: HashMap<String, flume::Sender<(RawFd, RequestType)>> = HashMap::new();
        // loop and receive on the flume channel
        loop {
            let result = self.receiver.recv_async().await;
            match result {
                Ok((raw_fd, handshake)) => {
                    let channel_name = handshake.channel_name();
                    if runners.contains_key(channel_name) {
                        let sender = runners.get(channel_name).unwrap();
                        let send_result = sender.send((raw_fd, handshake.request_type()));
                        if send_result.is_err() {
                            println!("send error is {}", send_result.unwrap_err());
                            panic!("cannot send result ")
                        }
                    } else {
                        let (sender, receiver): (flume::Sender<(RawFd, RequestType)>, flume::Receiver<(RawFd, RequestType)>) = flume::unbounded();
                        sender.send((raw_fd, handshake.request_type())).expect("send to task runner failed");
                        runners.insert(channel_name.to_string(), sender);
                        let mut task_runner = TaskRunner::new(channel_name.to_string(), self.commit_log_dir.clone(), receiver).await;
                        let _ = spawn(async move { task_runner.run().await }).detach();
                    }
                }
                Err(e) => {
                    println!("failed to receive from the flume channel, failed with error {}", e);
                    // continue here after logging the error as we dont want to block the thread
                }
            }
        }
    }
}