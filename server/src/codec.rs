use std::sync::Arc;
use std::task::Poll;
use std::{mem::MaybeUninit, pin::Pin};

use bytes::BytesMut;
use futures::Stream;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::stats::Stats;

/// The capacity of the read and write buffer in bytes.
const BUF_SIZE: usize = 64_000;

/// The threshold length on which to fill the buffer again in bytes.
///
/// When this threshold is reached, new memory may be allocated in the buffer
/// to satisfy the preferred buffer capacity.
/// This theshold must be larger than the longest frame we might receive over
/// the network, or else the frame might be incomplete when read.
///
/// Should be less than `BUF_SIZE` to prevent constant socket reads.
const BUF_THRESHOLD: usize = 16_000;

/// The maximum length of a line in bytes.
/// If a received line is longer than than the specified amount of bytes,
/// the search for a newline character (marking the end of a line) will be stalled,
/// and the line stream will end.
/// This is to prevent the stream from blocking, when no complete line could be read from a
/// full buffer.
///
/// This value must be smaller than `BUF_THRESHOLD` to prevent the server from getting
/// stuck as it can't find the end of a line within a full buffer.
const LINE_MAX_LENGTH: usize = 1024;

/// The prefix used for the Pixel Binary command
pub const PXB_PREFIX: [u8; 2] = ['P' as u8, 'B' as u8];

/// The size of a single Pixel Binary command.
///
///`                            Prefix             x   y   r   g   b   a
pub const PXB_CMD_SIZE: usize = PXB_PREFIX.len() + 2 + 2 + 1 + 1 + 1 + 1;

/// Line based codec.
///
/// This decorates a socket and presents a line based read / write interface.
///
/// As a user of `Lines`, we can focus on working at the line level. So, we
/// send and receive values that represent entire lines. The `Lines` codec will
/// handle the encoding and decoding as well as reading from and writing to the
/// socket.
#[derive(Debug)]
pub struct Lines<'a, T>
where
    T: AsyncRead + AsyncWrite,
{
    /// The TCP socket.
    socket: Pin<&'a mut T>,

    /// Buffer used when reading from the socket. Data is not returned from
    /// this buffer until an entire line has been read.
    rd: BytesMut,

    /// Buffer used to stage data before writing it to the socket.
    wr: BytesMut,

    /// Server stats.
    stats: Arc<Stats>,
}

impl<'a, T> Lines<'a, T>
where
    T: AsyncRead + AsyncWrite,
{
    /// Create a new `Lines` codec backed by the socket
    pub fn new(socket: Pin<&'a mut T>, stats: Arc<Stats>) -> Self {
        Lines {
            socket,
            rd: BytesMut::with_capacity(BUF_SIZE),
            wr: BytesMut::with_capacity(BUF_SIZE),
            stats,
        }
    }

    /// Buffer a line.
    ///
    /// This writes the line to an internal buffer. Calls to `poll_flush` will
    /// attempt to flush this buffer to the socket.
    pub fn buffer(&mut self, line: &[u8]) {
        // Push the line onto the end of the write buffer.
        //
        // The `put` function is from the `BufMut` trait.
        self.wr.extend_from_slice(line);
    }

    /// Flush the write buffer to the socket
    pub fn poll_flush(self: &mut Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<()> {
        // As long as there is buffered data to write, try to write it.
        while !self.wr.is_empty() {
            // `try_nb` is kind of like `try_ready`, but for operations that
            // return `io::Result` instead of `Async`.
            //
            // In the case of `io::Result`, an error of `WouldBlock` is
            // equivalent to `Async::NotReady.

            let len = self.wr.len();
            let bytes = self.wr.split_to(len);

            let socket = self.socket.as_mut();

            if let Poll::Ready(Ok(bytes)) = socket.poll_write(cx, &bytes) {
                bytes
            } else {
                return Poll::Pending;
            };
        }

        Poll::Ready(())
    }

    /// Read data from the socket if the buffer isn't full enough,
    /// and it's length reached the lower size threshold.
    ///
    /// If the size threshold is reached, and there isn't enough data
    /// in the buffer, memory is allocated to give the buffer enough capacity.
    /// The buffer is then filled with data from the socket with all the data
    /// that is available.
    ///
    /// Bool indicates whether or not an error occured
    fn fill_read_buf(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), ()>> {
        // Get the length of buffer contents
        let len = self.rd.len();

        // We've enough data to continue
        if len > BUF_THRESHOLD {
            return Poll::Ready(Ok(()));
        }

        // Read data and try to fill the buffer, update the statistics
        let mut local_buf: [MaybeUninit<u8>; BUF_SIZE] = [MaybeUninit::uninit(); BUF_SIZE];
        let mut read_buf = tokio::io::ReadBuf::uninit(&mut local_buf[..BUF_SIZE - len]);

        let amount = match self.socket.as_mut().poll_read(cx, &mut read_buf) {
            Poll::Ready(Ok(_)) => read_buf.filled().len(),
            Poll::Ready(Err(_)) => return Poll::Ready(Err(())),
            _ => return Poll::Pending,
        };

        // poll_read returns Ok(0) if the other end has hung up/EOF has been reached
        if amount == 0 {
            return Poll::Ready(Err(()));
        }

        self.rd.extend_from_slice(read_buf.filled());

        self.stats.inc_bytes_read(amount);

        // We're done reading
        return Poll::Ready(Ok(()));
    }
}

impl<'a, T> Stream for Lines<'a, T>
where
    T: AsyncRead + AsyncWrite,
{
    type Item = Vec<BytesMut>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut lines = Vec::with_capacity(self.rd.len() / 8);

        // Try to read any new data into the read buffer
        let fill_read_buf = self.as_mut().fill_read_buf(cx);
        let new_rd_len = self.rd.len();

        match fill_read_buf {
            // An error occured (most likely disconnection)
            Poll::Ready(Err(_)) => return Poll::Ready(None),
            Poll::Ready(Ok(_)) => {
                if new_rd_len < 2 {
                    // If the buffer cannot possibly contain a command, it makes sense
                    // to return `Poll::Pending`. However, this also means that we've now
                    // created our own pending condition that does not have a waker set by
                    // an underlying implementation. To avoid having to set that up, we simply
                    // defer our waking to `fill_read_buf` (which in turn defers it to some tokio::io
                    // impl) by waking our task immediately.
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
            }
            Poll::Pending => return Poll::Pending,
        }

        loop {
            let rd_len = self.rd.len();

            let is_binary_command = cfg!(feature = "binary-pixel-cmd")
                && rd_len >= PXB_PREFIX.len()
                && &self.rd[..PXB_PREFIX.len()] == PXB_PREFIX;

            // See if it's the specialized binary command
            if is_binary_command && rd_len >= PXB_CMD_SIZE {
                let line = self.rd.split_to(PXB_CMD_SIZE);
                lines.push(line);
            } else if !is_binary_command {
                // Find the new line character
                let pos = self
                    .rd
                    .iter()
                    .take(LINE_MAX_LENGTH)
                    .position(|b| *b == b'\n' || *b == b'\r');

                // Get the line, return it
                if let Some(pos) = pos {
                    // Find how many line ending chars this line ends with
                    let mut newlines = 1;
                    match self.rd.get(pos + 1) {
                        Some(b) => match *b {
                            b'\n' | b'\r' => newlines = 2,
                            _ => {}
                        },
                        _ => {}
                    }

                    // Pull the line of the read buffer
                    let mut line = self.rd.split_to(pos + newlines);

                    // Drop trailing new line characters
                    line.truncate(pos);

                    // Return the line
                    lines.push(line)
                } else if rd_len > LINE_MAX_LENGTH {
                    // If no line ending was found, and the buffer is larger than the
                    // maximum command length, disconnect

                    // TODO: report this error to the client
                    println!(
                        "Client sent a line longer than {} characters, disconnecting",
                        LINE_MAX_LENGTH,
                    );

                    // Break the connection, by ending the lines stream
                    return Poll::Ready(None);
                } else {
                    // Didn't find any more data to process
                    break;
                }
            } else {
                break;
            }
        }

        Poll::Ready(Some(lines))
    }
}