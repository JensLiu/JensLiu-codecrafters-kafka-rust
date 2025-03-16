use std::hash::Hasher;
use std::io::{Cursor, Read, Write};
use anyhow::{anyhow, bail};
use bytes::{Buf, BufMut};
use byteorder::WriteBytesExt;

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub(crate) enum RequestApiKey {
    Produce = 0,
    Fetch = 1,
    ApiVersions = 18
}

impl TryFrom<i16> for RequestApiKey {
    type Error = anyhow::Error;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            18 => Ok(RequestApiKey::ApiVersions),
            _ => Err(bail!("Invalid RequestApiKey value: {}", value))
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub(crate) enum ErrorCode {
    NoError = 0,
    UnsupportedVersion = 35,
    WTF = -1,
}

#[derive(Debug)]
enum RequestBody {
    Empty,
}

#[derive(Debug)]
pub(crate) struct Request {
    pub(crate) message_size: i32,
    pub(crate) header: RequestHeaderV2,
    pub(crate) body: RequestBody,
}

#[derive(Debug)]
pub(crate) struct RequestHeaderV2 {
    pub(crate) request_api_key: RequestApiKey,
    pub(crate) request_api_version: i16,
    pub(crate) correlation_id: i32,
    pub(crate) client_id: String,
}

impl RequestHeaderV2 {
    fn parse(cur: &mut Cursor<&[u8]>) -> anyhow::Result<Self> {
        let request_api_key = cur.get_i16();
        let request_api_version = cur.get_i16();
        let correlation_id = cur.get_i32();
        let client_id = parse_nullable_string(cur)?;
        Ok(Self {
            request_api_key: RequestApiKey::try_from(request_api_key)?,
            request_api_version,
            correlation_id,
            client_id
        })
    }
}

impl RequestBody {
    fn parse(cur: &mut Cursor<&[u8]>, request_api_version: i16, request_api_key: RequestApiKey) -> anyhow::Result<Self, ErrorCode> {
        match request_api_key {
            RequestApiKey::ApiVersions => {
                // if request_api_version < 0 || request_api_version > 4 {
                //     Err(ErrorCode::UnsupportedVersion)?;
                // }
                Ok(RequestBody::Empty)
            },
            _ => Err(ErrorCode::WTF)
        }
    }
}

impl Request {
    pub fn parse(cur: &mut Cursor<&[u8]>) -> Result<Self, ErrorCode> {
        let message_size = cur.get_i32();
        let mut buf = vec![0u8; message_size as usize];
        cur.read_exact(&mut *buf).expect("TODO: panic message");
        let mut cur_body = Cursor::new(&*buf);
        let Ok(header) = RequestHeaderV2::parse(&mut cur_body) else {
            Err(ErrorCode::WTF)?
        };
        let body = RequestBody::parse(&mut cur_body, header.request_api_version, header.request_api_key)?;
        Ok(Self {
            message_size, header, body
        })
    }
}
enum ResponseHeader {
    ResponseHeaderV0(ResponseHeaderV0)
}

#[derive(Debug)]
pub(crate) struct ResponseHeaderV0 {
    pub(crate) correlation_id: i32,
}

#[derive(Debug)]
pub(crate) enum ResponseBody {
    Empty,
    APIVersionsResponseBodyV4(APIVersionsResponseBodyV4)
}

#[derive(Debug)]
pub(crate) struct Response {
    pub(crate) header: ResponseHeaderV0,
    pub(crate) body: ResponseBody,
}

#[derive(Debug)]
pub(crate) struct APIKey {
    pub(crate) api_key: i16,
    pub(crate) min_version: i16,
    pub(crate) max_version: i16,
}
#[derive(Debug)]
pub(crate) struct APIVersionsResponseBodyV4 {
    pub(crate) error_code: ErrorCode,
    pub(crate) api_keys: Vec<APIKey>,
    pub(crate) throttle_time_ms: i32
}

impl ResponseHeaderV0 {
    fn write(&self, buf: &mut Vec<u8>) {
        buf.put_i32(self.correlation_id);
    }
}

impl APIVersionsResponseBodyV4 {
    fn write(&self, buf: &mut Vec<u8>) {
        buf.put_i16(self.error_code as i16);
        // println!("current buffer: {}", hex::encode(buf.clone()));
        // NOTE: Implemented as a compact array
        if self.api_keys.len() == 0 {
            buf.put_i8(0);
        } else {
            buf.put_i8(self.api_keys.len() as i8 + 1);  // N + 1 for compact array
            // println!("current buffer: {}", hex::encode(buf.clone()));
            for api_key in &self.api_keys {
                buf.put_i16(api_key.api_key);
                // println!("current buffer: {}", hex::encode(buf.clone()));
                buf.put_i16(api_key.min_version);
                // println!("current buffer: {}", hex::encode(buf.clone()));
                buf.put_i16(api_key.max_version);
                // println!("current buffer: {}", hex::encode(buf.clone()));
                put_tag_buffer(buf);
                // println!("current buffer: {}", hex::encode(buf.clone()));
            }
            buf.put_i32(self.throttle_time_ms);
            // println!("current buffer: {}", hex::encode(buf.clone()));
            put_tag_buffer(buf);
            // println!("current buffer: {}", hex::encode(buf.clone()));
        }
    }
}

impl ResponseBody {
    fn write(&self, cur: &mut Vec<u8>) {
        // println!("current buffer: {}", hex::encode(cur.clone()));
        match self {
            ResponseBody::APIVersionsResponseBodyV4(body) => {
                // println!("current buffer: {}", hex::encode(cur.clone()));
                body.write(cur)
            }
            _ => {}
        }
    }
}

impl Response {
    pub fn write(&self, buf: &mut Vec<u8>) {
        let mut header_buf = Vec::new();
        self.header.write(&mut header_buf);
        let mut body_buf = Vec::new();
        // println!("current buffer: {}", hex::encode(body_buf.clone()));
        self.body.write(&mut body_buf);
        // println!("body: {:?}", hex::encode(body_buf.clone()));
        let message_size = header_buf.len() + body_buf.len();
        buf.put_i32(message_size as i32);
        buf.put(header_buf.as_slice());
        buf.put(body_buf.as_slice());
    }
}



fn parse_nullable_string(cur: &mut Cursor<&[u8]>) -> anyhow::Result<String> {
    let size = cur.get_i16();
    let str = if size == -1 { "".into() } else {
        let mut s = vec![0u8; size as usize];
        cur.read_exact(&mut *s)?;
        String::from_utf8(s.to_vec())?
    };
    Ok(str)
}

fn put_tag_buffer(buf: &mut Vec<u8>) {
    buf.put_i8(0);
}