use std::io::{Cursor, Read, Write, Seek};
use std::net::TcpListener;
use anyhow::bail;
use bytes::{Buf, BufMut};
use crate::ErrorCode::UnsupportedVersion;
use crate::RequestApiKey::{ApiVersions, Produce};

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

#[derive(Debug, Eq, PartialEq)]
enum RequestApiKey {
    Produce = 0,
    Fetch = 1,
    ApiVersions = 18
}

impl TryFrom<i32> for RequestApiKey {
    type Error = anyhow::Error;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            18 => Ok(Produce),
            _ => Err(bail!("Invalid RequestApiKey value: {}", value))
        }
    }
}

enum ErrorCode {
    NoError = 0,
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
                let mut header = [0u8; 4];
                stream.read_exact(&mut header)?;
                let message_size = i32::from_be_bytes(header);
                let mut request = vec![0u8; message_size as usize];
                stream.read_exact(&mut *request).unwrap();
                {
                    let req = header.iter().chain(request.as_slice().iter()).map(|&x| format!("{:x}", x)).collect::<Vec<String>>().join("");
                    println!("Received request {:?}", req);
                }
                let mut cur = request.as_slice();
                // request header
                let request_api_key = cur.get_i16();
                let request_api_version = cur.get_i16();
                let correlation_id = cur.get_i32();
                let client_id_len = cur.get_i16();
                let client_id = if client_id_len != -1 {
                    let mut client_id = vec![0u8; client_id_len as usize];
                    cur.read_exact(&mut client_id).unwrap();
                    String::from_utf8(client_id)?
                } else {
                    "".into()
                };
                println!("request_api_key={}", request_api_key);
                println!("request_api_version={}", request_api_version);
                println!("correlation_id={}", correlation_id);
                println!("client_id_len={}", client_id_len);
                println!("client_id={}", client_id);
                // request
                let request_key = RequestApiKey::try_from(request_api_key as i32).unwrap();
                if request_key == RequestApiKey::ApiVersions {
                    // let client_software_name =
                }
                // response
                let mut data = Vec::new();
                data.put_i32(correlation_id);       // correlation_id

                if request_key == ApiVersions {
                    // error code
                    if request_api_version < 0 || request_api_version > 4 {
                        println!("Invalid request_api_version={}", request_api_version);
                        data.put_i16(UnsupportedVersion as i16)
                    } else {
                        data.put_i16(ErrorCode::NoError as i16);
                    }
                    data.put_i8(2); // array end index
                    // API_VERSION API
                    data.put_i16(ApiVersions as i16);   // api key
                    data.put_i16(0);    // min version
                    data.put_i16(4);    // max version
                    data.put_i8(0);     // TAG_BUFFER
                    data.put_i32(0);    // throttle time in ms?
                    data.put_i8(0);     // TAG_BUFFER
                    // possible other APIs
                }

                println!("Response data = {:?}", data);

                let mut response = Vec::new();
                response.put_i32(data.len() as i32);    // data length
                response.put(&data[..]);            // data
                stream.write_all(&*response).unwrap();
                println!("written {:?}", response);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }

    Ok(())
}
