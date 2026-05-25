use crate::worker::Worker;
use compio::runtime::Runtime;
use ironlog_core::Handshake;
use rustc_hash::FxHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::io::{Error, ErrorKind};
use std::os::fd::RawFd;
use std::thread::JoinHandle;

pub const FILE_NAME: &str = "1.log";

pub struct IronLogRuntime {
    thread_handles: Vec<JoinHandle<()>>,
    thread_to_senders: HashMap<String, flume::Sender<(RawFd, Handshake)>>,
    num_cores: usize,
}

impl IronLogRuntime {
    pub async fn new(commit_log_dir: String) -> Result<Self, Error> {
        let num_cores = std::thread::available_parallelism()?.get();
        let mut thread_to_senders: HashMap<String, flume::Sender<(RawFd, Handshake)>> = HashMap::new();
        // now create n threads based on num cores on the machine

        let mut thread_handles = vec![];
        for i in 0..num_cores {
            let thread_name = format!("Thread-{}", i);
            let (sender, receiver): (flume::Sender<(RawFd, Handshake)>, flume::Receiver<(RawFd, Handshake)>) = flume::unbounded();
            thread_to_senders.insert(thread_name.clone(), sender);
            let commit_log_dir = commit_log_dir.clone();
            // spawn a thread and assign a compio runtime
            let handle = std::thread::spawn(|| {
                let compio_runtime = Runtime::new().expect("compio runtime failed to create ");
                compio_runtime.block_on(async move {
                    let worker = Worker::new(thread_name, commit_log_dir, receiver).await;
                    worker.start_worker().await;
                })
            });
            thread_handles.push(handle);
        }
        Ok(IronLogRuntime {
            thread_handles,
            thread_to_senders,
            num_cores,
        })
    }
    pub fn send_stream_to_worker(&self, raw_fd: RawFd, handshake: Handshake) -> Result<(), Error> {
        let mut hasher = FxHasher::default();
        handshake.channel_name().hash(&mut hasher);
        let hash_value = hasher.finish() as usize;
        let thread_name = format!("Thread-{}", hash_value % self.num_cores);
        println!("sending stream to thread {}", thread_name);
        let sender = self.thread_to_senders.get(&thread_name).expect("thread should have a sender setup"); // this is fine here to panic as it is unexpected here
        sender.send((raw_fd, handshake)).map_err(|e| std::io::Error::new(ErrorKind::Other, e))?;
        Ok(())
    }
    pub fn join(self) {
        for handle in self.thread_handles {
            handle.join().expect("worker thread panicked! ")
        }
    }
}