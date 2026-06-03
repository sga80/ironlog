use ironlog_consumer::Consumer;

#[compio::main]
async fn main() {
    let host = std::env::var("IRONLOG_HOST").unwrap_or_else(|_| String::from("127.0.0.1"));
    let port = std::env::var("IRONLOG_PORT")
        .ok()
        .and_then(|v| v.parse::<u16>().ok())
        .unwrap_or(4000);
    let channel = std::env::var("IRONLOG_CHANNEL").unwrap_or_else(|_| String::from("test"));
    let mut offset = std::env::var("IRONLOG_OFFSET")
        .ok()
        .and_then(|v| v.parse::<u64>().ok());
    let mut consumer = Consumer::new(host, port, channel).await.expect("connection failed to server");
    loop {
        let consumer_result = consumer.fetch(offset).await.expect("cant fetch result");
        println!("offset is {} and payload is {}", consumer_result.offset(), String::from_utf8(consumer_result.payload()).unwrap_or(String::from("cannot convert payload to string")));
        offset = Some(consumer_result.offset() + 1);
        if consumer_result.is_eof() {
            break;
        }
    }
}
