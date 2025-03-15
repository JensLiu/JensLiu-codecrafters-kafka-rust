use std::io::{Read, Write};
use std::net::TcpListener;

struct Response<H> {
    message_size: i32,
    header: H,
    // body:
}
struct ResponseHeaderV0 {
    correlation_id: i32,
}

type ResponseV0 = Response<ResponseHeaderV0>;

fn main() -> anyhow::Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage

    let listener = TcpListener::bind("127.0.0.1:9092")?;

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                // let response = ResponseV0 {
                //     message_size: 0,
                //     header: ResponseHeaderV0 {
                //         correlation_id: 7   // hard coded for now
                //     }
                // };
                let buf = [0, 0, 0, 0, 0, 0, 0, 7]; // hard-coded resposne
                stream.write(&buf)?;
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }

    Ok(())
}
