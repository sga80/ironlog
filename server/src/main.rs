mod commit_log;
mod ironlog_runtime;
mod worker;
mod task_runner;

use crate::ironlog_runtime::IronLogRuntime;
use compio::driver::AsRawFd;
use compio::net::TcpListener;
use libc;
use std::io::{Read, Write};
use std::os::unix::io::RawFd;

#[compio::main]
async fn main() -> std::io::Result<()> {
    let driver = compio::runtime::Runtime::with_current(|r| r.driver_type());
    println!("compio driver: {:?}", driver);
    let commit_log_dir = std::env::args().nth(1).unwrap_or_else(|| { std::env::temp_dir().into_os_string().into_string().unwrap() });
    println!("commit log dir is {}", commit_log_dir);
    println!("starting serer with new async everywhere");
    let ironlog_runtime = IronLogRuntime::new(commit_log_dir).await.expect("IronLogRuntime failed to bootstrap");

    let listener = TcpListener::bind("0.0.0.0:4000").await?;
    loop { // this loop is running on the main thread. Until dispatch is called , the main thread will be waiting on the tcp_stream await
        let (mut tcp_stream, _) = listener.accept().await?; // compio uses io_uring in linux for this.
        tcp_stream.set_nodelay(true).expect("except not delay to set");
        let handshake = ironlog_core::handshake_from_bytes(&mut tcp_stream).await?;
        // the below code is out of my depth.
        // the tcp_stream in compio uses Rc which is not Send. std::net::TCPStream is Send and it can be passed safely across threads, but not compio's TCpStream as it enforces share nothing
        // the tcp stream is nothing but a file descriptor.
        // the blow code is using unsafe code to duplicate the file descriptor and drop the tcp stream, but the socket is alive because we created a reference to the same underlying kernel socket.
        // both file descriptors before drop point to the same socker.
        let raw_fd = tcp_stream.as_raw_fd();
        let duped = dup_fd(raw_fd)?;
        drop(tcp_stream); // close original — socket stays alive via the dup
        let send_result = ironlog_runtime.send_stream_to_worker(duped, handshake);
        if send_result.is_err() {
            let error = send_result.unwrap_err();
            println!("failed to send message to threadn. failed with error {}", error);
        }
    }

    // this is dead code ,not called now. But we will call it when we implement shutdown
    // the threads are already started
    //ironlog_runtime.join();
}

// provided by claude as I was out of depth here .
// duplicates the file descriptor so that we can reconstruct the stream in the dispatch closure
fn dup_fd(raw_fd: RawFd) -> std::io::Result<RawFd> {
    let duped = unsafe { libc::fcntl(raw_fd, libc::F_DUPFD_CLOEXEC, 0) };
    if duped == -1 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(duped)
}
