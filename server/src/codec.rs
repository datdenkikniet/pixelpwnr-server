use std::sync::Arc;
use std::task::Poll;
use std::{mem::MaybeUninit, pin::Pin};

use bytes::BytesMut;
use futures::Future;
use pixelpwnr_render::{Color, Pixmap};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::cmd::{Cmd, CmdResult};
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
pub struct Lines<'sock, T>
where
    T: AsyncRead + AsyncWrite,
{
    /// The TCP socket.
    socket: Pin<&'sock mut T>,

    /// Buffer used when reading from the socket. Data is not returned from
    /// this buffer until an entire line has been read.
    rd: BytesMut,

    /// Buffer used to stage data before writing it to the socket.
    wr: BytesMut,

    /// Server stats.
    stats: Arc<Stats>,

    /// A pixel map.
    pixmap: Arc<Pixmap>,

    /// This is `Some(Reason)` if this Lines is disconnecting
    disconnecting: Option<String>,
}

impl<'sock, T> Lines<'sock, T>
where
    T: AsyncRead + AsyncWrite,
{
    /// Create a new `Lines` codec backed by the socket
    pub fn new(socket: Pin<&'sock mut T>, stats: Arc<Stats>, pixmap: Arc<Pixmap>) -> Self {
        Lines {
            socket,
            rd: BytesMut::with_capacity(BUF_SIZE),
            wr: BytesMut::with_capacity(BUF_SIZE),
            stats,
            pixmap,
            disconnecting: None,
        }
    }

    /// Buffer a line.
    ///
    /// This writes the line to an internal buffer. Calls to `poll_flush` will
    /// attempt to flush this buffer to the socket.
    pub fn buffer(&mut self, line: &[u8], cx: &mut std::task::Context<'_>) {
        // Push the line onto the end of the write buffer.
        //
        // The `put` function is from the `BufMut` trait.
        self.wr.extend_from_slice(line);

        // Wake the context so we can be polled again immediately
        cx.waker().wake_by_ref();
    }

    /// Flush the write buffer to the socket
    pub fn poll_write(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Result<(), &str>> {
        let Self { socket, wr, .. } = self;

        match socket.as_mut().poll_write(cx, wr) {
            Poll::Ready(Ok(0)) => Poll::Ready(Err("Client disconnected")),
            Poll::Ready(Ok(size)) => {
                let _ = wr.split_to(size);
                if self.wr.is_empty() {
                    Poll::Ready(Ok(()))
                } else {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
            Poll::Ready(Err(_)) => Poll::Ready(Err("Socket error")),
            Poll::Pending => Poll::Pending,
        }
    }

    /// Read data from the socket if the buffer isn't full enough,
    /// and it's length reached the lower size threshold.
    ///
    /// If the size threshold is reached, and there isn't enough data
    /// in the buffer, memory is allocated to give the buffer enough capacity.
    /// The buffer is then filled with data from the socket with all the data
    /// that is available.
    ///
    /// If the return value is Poll::Ready, the result contains either `Ok(new_rd_size)` or
    /// `Err(disconnect reason message)`.
    #[inline(always)]
    fn fill_read_buf(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Result<usize, ()>> {
        // Get the length of buffer contents
        let len = self.rd.len();

        // We've enough data to continue
        if len > BUF_THRESHOLD {
            return Poll::Ready(Ok(len));
        }

        // Read data and try to fill the buffer, update the statistics
        let mut local_buf: [MaybeUninit<u8>; BUF_SIZE] = [MaybeUninit::uninit(); BUF_SIZE];
        let mut read_buf = tokio::io::ReadBuf::uninit(&mut local_buf[..BUF_SIZE - len]);

        let amount = match self.socket.as_mut().poll_read(cx, &mut read_buf) {
            Poll::Ready(Ok(_)) => read_buf.filled().len(),
            Poll::Ready(Err(_)) => return Poll::Ready(Err(())),
            Poll::Pending => return Poll::Pending,
        };

        // poll_read returns Ok(0) if the other end has hung up/EOF has been reached
        if amount == 0 {
            return Poll::Ready(Err(()));
        }

        self.rd.extend_from_slice(read_buf.filled());

        self.stats.inc_bytes_read(amount);

        // We're done reading
        return Poll::Ready(Ok(self.rd.len()));
    }

    #[inline(always)]
    fn process_rx_buffer(&mut self, cx: &mut std::task::Context<'_>) -> Result<(), String> {
        let mut pixels = 0;
        let error_message = loop {
            let rd_len = self.rd.len();

            let is_binary_command = cfg!(feature = "binary-pixel-cmd")
                && rd_len >= PXB_PREFIX.len()
                && &self.rd[..PXB_PREFIX.len()] == PXB_PREFIX;

            // See if it's the specialized binary command
            let command = if is_binary_command && rd_len >= PXB_CMD_SIZE {
                let input_bytes = self.rd.split_to(PXB_CMD_SIZE);
                const OFF: usize = PXB_PREFIX.len();
                let x = u16::from_le_bytes(input_bytes[OFF..OFF + 2].try_into().expect("Huh"));
                let y = u16::from_le_bytes(input_bytes[OFF + 2..OFF + 4].try_into().expect("Huh"));

                let r = input_bytes[OFF + 4];
                let g = input_bytes[OFF + 5];
                let b = input_bytes[OFF + 6];
                let a = input_bytes[OFF + 7];

                Cmd::SetPixel(x as usize, y as usize, Color::from_rgba(r, g, b, a))
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
                    match Cmd::decode_line(line.freeze()) {
                        Ok(cmd) => cmd,
                        Err(e) => {
                            // Report the error to the client
                            self.buffer(&format!("ERR {}\r\n", e).as_bytes(), cx);
                            break Some("Command decoding failed".to_string());
                        }
                    }
                } else if rd_len > LINE_MAX_LENGTH {
                    // If no line ending was found, and the buffer is larger than the
                    // maximum command length, disconnect

                    self.buffer(b"ERR Line length >1024\r\n", cx);

                    // Break the connection, by ending the lines stream
                    break Some("Client line length too long".to_string());
                } else {
                    // Didn't find any more data to process
                    break None;
                }
            } else {
                break None;
            };

            let result = command.invoke(&self.pixmap, &mut pixels);
            // Do something with the result
            match result {
                // Do nothing
                CmdResult::Ok => {}

                // Respond to the client
                CmdResult::Response(msg) => {
                    // Create a bytes buffer with the message
                    self.buffer(msg.as_bytes(), cx);
                    self.buffer(b"\r\n", cx);
                }

                // Report the error to the user
                CmdResult::ClientErr(err) => {
                    // Report the error to the client
                    self.buffer(&format!("ERR {}\r\n", err).as_bytes(), cx);
                    break Some(format!("Client error: {}", err));
                }

                // Quit the connection
                CmdResult::Quit => {
                    break Some("Client sent QUIT".to_string());
                }
            }
        };

        // Increase the amount of set pixels by the amount of pixel set commands
        // that we processed in this batch
        self.stats.inc_pixels_by_n(pixels);

        if let Some(disconnect_message) = error_message {
            Err(disconnect_message)
        } else {
            Ok(())
        }
    }
}

impl<'sock, T> Future for Lines<'sock, T>
where
    T: AsyncRead + AsyncWrite,
{
    type Output = String;

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        // First try to write all we have left to write
        let write_is_pending = if !self.wr.is_empty() {
            match self.poll_write(cx) {
                Poll::Ready(Ok(_)) => {
                    // We've finished writing, do nothing
                    false
                }
                Poll::Ready(Err(e)) => return Poll::Ready(e.to_string()),
                Poll::Pending => true,
            }
        } else {
            false
        };

        if !write_is_pending {
            if let Some(reason) = &self.disconnecting {
                return Poll::Ready(reason.clone());
            }
        }

        // Try to read any new data into the read buffer
        let fill_read_buf = self.fill_read_buf(cx);

        match fill_read_buf {
            // An error occured (most likely disconnection)
            Poll::Ready(Err(_)) => return Poll::Ready("Client disconnected".into()),
            Poll::Ready(Ok(new_rd_len)) => {
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

        let rx_process_result = self.process_rx_buffer(cx);

        if let Err(disconnect_message) = rx_process_result {
            self.disconnecting = Some(disconnect_message);
        }

        if !write_is_pending {
            // We're not blocking on any IO, so we have to re-wake
            // immediately
            cx.waker().wake_by_ref();
        }

        Poll::Pending
    }
}

#[tokio::test]
async fn response_newline() {
    let stats = Arc::new(Stats::new());
    let pixmap = Arc::new(Pixmap::new(400, 800));

    let mut test = tokio_test::io::Builder::new()
        // Test all commands that require a response
        .read(b"PX 16 16\r\n")
        .write(b"PX 16 16 000000\r\n")
        .read(b"SIZE\r\n")
        .write(b"SIZE 400 800\r\n")
        .read(b"HELP\r\n")
        .write(format!("{}\r\n", Cmd::help_list()).as_bytes())
        // Test different variations of newlines
        .read(b"PX 16 16\n")
        .write(b"PX 16 16 000000\r\n")
        // Verify that adding a few whitespaces after the command doesn't make a difference
        .read(b"PX 16 16                     \n")
        .write(b"PX 16 16 000000\r\n")
        // Using an out of bounds index should return an error
        .read(b"PX 1000 0\r\n")
        .write(b"ERR x coordinate out of bound\r\n")
        .build();

    let test = Pin::new(&mut test);

    let lines = Lines::new(test, stats.clone(), pixmap.clone());

    lines.await;
}