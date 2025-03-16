mod network_req_resp;

use std::io::{Cursor, Read, Write, Seek};
use std::net::TcpListener;
use bytes::{Buf, BufMut};
use crate::network_req_resp::*;



// type ResponseV0 = Response<ResponseHeaderV0>;

fn main() -> anyhow::Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage

    let listener = TcpListener::bind("127.0.0.1:9092")?;

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                let mut request_buf= Vec::new();
                stream.read_to_end(&mut request_buf)?;
                let mut cur = Cursor::new(&*request_buf);
                let request = Request::parse(&mut cur).unwrap();
                println!("{:?}", request);
                let response_header = ResponseHeaderV0 {
                    correlation_id: request.header.correlation_id,
                };
                let response_body = match request.header.request_api_key {
                    RequestApiKey::Produce => ResponseBody::Empty,
                    RequestApiKey::Fetch => ResponseBody::Empty,
                    RequestApiKey::ApiVersions => {
                        let error_code = if request.header.request_api_version < 0 || request.header.request_api_version > 4 {
                            ErrorCode::UnsupportedVersion
                        } else {
                            ErrorCode::NoError
                        };
                        let api_versions = APIKey {
                            api_key: RequestApiKey::ApiVersions as i16,
                            min_version: 0,
                            max_version: 4,
                        };
                        let api_keys = vec![api_versions];
                        ResponseBody::APIVersionsResponseBodyV4(
                            APIVersionsResponseBodyV4 {
                                error_code,
                                api_keys,
                                throttle_time_ms: 420
                            }
                        )
                    }
                };

                let response = Response {
                    header: response_header,
                    body: response_body
                };

                println!("{:?}", response);

                let mut response_buf = Vec::new();
                response.write(&mut response_buf);
                stream.write_all(&*response_buf).expect("TODO: panic message");
            }


        Err(e) => {
            println!("error: {}", e);
        }
    }
}

Ok(())
}
