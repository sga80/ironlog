use ironlog_core::PayloadType;
use ironlog_producer::Producer;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;

#[compio::main]
async fn main() {
    let host = std::env::var("IRONLOG_HOST").unwrap_or_else(|_| String::from("127.0.0.1"));
    let channel_name = std::env::var("IRONLOG_CHANNEL").unwrap_or_else(|_| String::from("test"));
    println!("writing to channel {}", channel_name);
    let mut producer = Producer::new(host, 4000, channel_name).await.expect("producer not created");
    let log_file = std::env::var("LOG_FILE").expect("log file should be set in the env");
    let file = File::open(log_file).expect("file missing");
    let reader = BufReader::new(file);
    let mut index = 0;
    let mut last_offset = 0;
    for line in reader.lines() {
        match line {
            Ok(line) => {
                let result = producer.send(PayloadType::Text, line.as_bytes()).await.expect("failed to send data to broker");
                last_offset = result.offset;
                index += 1;
            }
            Err(_) => {}
        }
    }
    println!("last offset is {} and total lines written is {}", last_offset, index);
    //producer.send(PayloadType::Text, String::from("this is a test 1").as_bytes()).await.expect("failed to send data to broker");
}

// async fn send_binary_files(producer: &mut Producer) {
//     let mut files: Vec<_> = fs::read_dir("/data")
//         .expect("failed to read /data directory")
//         .filter_map(|e| e.ok())
//         .map(|e| e.path())
//         .filter(|p| p.extension().map(|e| e == "bin").unwrap_or(false))
//         .collect();
//
//     assert!(!files.is_empty(), "no .bin files found in /data");
//     println!("found {} binary files", files.len());
//
//     let mut index: u64 = 0;
//     let mut last_offset: u64 = 0;
//
//     for iteration in 0..2880 {
//         for path in &files {
//             let bytes = fs::read(path).expect("failed to read binary file");
//             let result = producer.send(PayloadType::Binary, &bytes).await.expect("failed to send");
//             last_offset = result.offset;
//             index += 1;
//         }
//         println!("iteration {} done, sent {} messages, last offset {}", iteration + 1, index, last_offset);
//     }
//     println!("done. total messages: {}, last offset: {}", index, last_offset);
// }