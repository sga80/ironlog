use ironlog_core::PayloadType;
use ironlog_producer::Producer;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::sync::mpsc::channel;

#[compio::main]
async fn main() {
    let host = std::env::var("IRONLOG_HOST").unwrap_or_else(|_| String::from("127.0.0.1"));
    let channel_name = std::env::var("IRONLOG_CHANNEL").unwrap_or_else(|_| String::from("test"));
    println!("writing to channel {}", channel_name);
    let mut producer = Producer::new(host, 4000, String::from(channel_name)).await.expect("producer not created");
    let log_file = std::env::var("LOG_FILE").expect("log file should be set in the env");
    let file = File::open(log_file).expect("file missing");
    let reader = BufReader::new(file);
    let channel_name = String::from("test");
    let mut index = 0;
    let mut last_offset = 0;
    for line in reader.lines() {
        let line = line;
        match line {
            Ok(line) => {
                let result = producer.send(PayloadType::Text, line.as_bytes()).await.expect("failed to send data to broker");
                last_offset = result.offset;
                index += 1;
            }
            Err(_) => {}
        }
    }
    println!("last offset is {} and total lines written is {}", last_offset, index)
    // let host = std::env::var("IRONLOG_HOST").unwrap_or_else(|_| String::from("127.0.0.1"));
    // println!("host is {}", host);
    // let mut producer = Producer::new(host, 4000, String::from("test")).await.expect("producer not created");
    // producer.send(PayloadType::Text, "this is a test ".as_bytes()).await.expect("send to work");
}