use ironlog_consumer::Consumer;
use std::mem::offset_of;

#[compio::main]
async fn main() {
    let host = std::env::var("IRONLOG_HOST").unwrap_or_else(|_| String::from("127.0.0.1"));
    let port = std::env::var("IRONLOG_PORT")
        .ok()
        .and_then(|v| v.parse::<u16>().ok())
        .unwrap_or(4000);
    let channel = std::env::var("IRONLOG_CHANNEL").unwrap_or_else(|_| String::from("test"));
    let offset = std::env::var("IRONLOG_OFFSET")
        .ok()
        .and_then(|v| v.parse::<u64>().ok());
    let mut consumer = Consumer::new(host, port, channel).await.expect("connection failed to server");
    let results = consumer.fetch(offset).await.expect("cannot fetch results from server");
    println!("results len is {}", results.len());

    for result in results {
        println!("consumer result is {:?}", result)
    }
}
