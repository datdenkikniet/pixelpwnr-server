use std::num::NonZeroUsize;

use bytes::BytesMut;
use zstd::{
    stream::raw::{Decoder, Operation},
    zstd_safe::{InBuffer, OutBuffer},
};

use super::BUF_SIZE;

pub trait Decompressor {
    type Error: std::fmt::Debug;

    /// Decompress (part of) the given input
    fn decompress_stream(
        &mut self,
        input_data: &[u8],
        output: &mut BytesMut,
    ) -> Result<usize, Self::Error>;

    /// Give a hint for the expected next size
    fn size_hint(&self) -> Option<NonZeroUsize>;
}

pub struct ZstdDecompressor<'a> {
    decoder: Decoder<'a>,
    last_size_hint: usize,
}

impl<'a> ZstdDecompressor<'a> {
    pub fn new() -> Self {
        Self {
            decoder: Decoder::new().unwrap(),
            last_size_hint: 0,
        }
    }
}

impl<'a> Decompressor for ZstdDecompressor<'a> {
    type Error = std::io::Error;

    fn decompress_stream(
        &mut self,
        input_data: &[u8],
        output: &mut BytesMut,
    ) -> Result<usize, Self::Error> {
        let mut out_buf = Vec::with_capacity(BUF_SIZE * 32);

        let mut read = &input_data[..];
        let mut decompressed_bytes = 0;
        while read.len() > 0 {
            let read_bytes = if self.last_size_hint == 0 || self.last_size_hint > read.len() {
                read.len()
            } else {
                self.last_size_hint
            };

            let in_buffer = &mut InBuffer::around(&read[..read_bytes]);
            let out_buffer = &mut OutBuffer::around(&mut out_buf);
            let op = self.decoder.run(in_buffer, out_buffer)?;

            output.extend_from_slice(out_buffer.as_slice());
            decompressed_bytes += out_buffer.as_slice().len();

            read = &read[in_buffer.pos()..];

            if op == 0 {
                self.decoder.reinit()?;
            }
            self.last_size_hint = 0;
        }
        Ok(decompressed_bytes)
    }

    fn size_hint(&self) -> Option<NonZeroUsize> {
        NonZeroUsize::new(self.last_size_hint)
    }
}
