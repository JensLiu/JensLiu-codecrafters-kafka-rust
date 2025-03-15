use std::io::{Cursor, Read, Write, Seek};
use std::net::TcpListener;
use bytes::{Buf, BufMut};
use crate::ErrorCode::UnsupportedVersion;

struct Response<H> {
    message_size: i32,
    header: H,
    // body:
}
struct ResponseHeaderV0 {
    correlation_id: i32,
}

struct RequestHeaderV2 {
    request_api_key: i16,
    request_api_version: i16,
    correlation_id: i32,
    client_id: String,
}

enum RequestApiKey {
    Produce = 0,
    Fetch = 1,
}

enum ErrorCode {
    UnsupportedVersion = 35
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
                let mut request = [0u8; 4];
                stream.read_exact(&mut request)?;
                let message_size = i32::from_be_bytes(request);
                let mut request = vec![0u8; message_size as usize];
                stream.read_exact(&mut *request)?;
                let mut cur = request.as_slice();
                let request_api_key = cur.get_i16();
                let request_api_version = cur.get_i16();
                let correlation_id = cur.get_i32();
                let client_id_len = cur.get_i32();
                println!("request_api_key={}", request_api_key);
                println!("request_api_version={}", request_api_version);
                println!("correlation_id={}", correlation_id);
                println!("client_id_len={}", client_id_len);
                let mut response = Vec::<u8>::with_capacity(8);
                response.put_i32(0);
                response.put_i32(correlation_id);
                if request_api_version < 0 || request_api_version > 4 {
                    println!("Invalid request_api_version={}", request_api_version);
                    response.put_i16(UnsupportedVersion as i16)
                }
                stream.write_all(&*response)?;
                println!("written");
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }

    Ok(())
}
