use bytes::Bytes;
use memchr::memchr;
use std::{
    cmp,
    io::{self, Read},
};

/// Buffer wrapper type from postgres-protocol
// https://docs.rs/postgres-protocol/0.6.2/src/postgres_protocol/message/backend.rs.html#281
pub struct Buffer {
    pub bytes: Bytes,
    pub idx: usize,
}

impl Buffer {
    #[inline]
    pub fn slice(&self) -> &[u8] {
        &self.bytes[self.idx..]
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.slice().is_empty()
    }

    #[inline]
    pub fn read_cstr(&mut self) -> io::Result<Bytes> {
        match memchr(0, self.slice()) {
            Some(pos) => {
                let start = self.idx;
                let end = start + pos;
                let cstr = self.bytes.slice(start..end);
                self.idx = end + 1;
                Ok(cstr)
            }
            None => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "unexpected EOF",
            )),
        }
    }
}

impl Read for Buffer {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = {
            let slice = self.slice();
            let len = cmp::min(slice.len(), buf.len());
            buf[..len].copy_from_slice(&slice[..len]);
            len
        };
        self.idx += len;
        Ok(len)
    }
}
