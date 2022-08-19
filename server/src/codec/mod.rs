use std::sync::Arc;
use std::task::Poll;
use std::{mem::MaybeUninit, pin::Pin};

use bytes::BytesMut;
use futures::Future;
use pixelpwnr_render::{Color, Pixmap};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::arg_handler::ServerOptions;
use crate::cmd::{Cmd, CmdResult};
use crate::stats::Stats;

mod decompressor;

use self::decompressor::{Decompressor, ZstdDecompressor};

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

pub const BINARY_PX_SIZE: usize = 8;

/// The prefix used for the Pixel Binary command
pub const PXB_PREFIX: [u8; 2] = ['P' as u8, 'B' as u8];

/// The size of a single Pixel Binary command.
pub const PXB_CMD_SIZE: usize = PXB_PREFIX.len() + BINARY_PX_SIZE;

/// The prefix for the repeated Pixel Binary command
pub const PNB_PREFIX: [u8; 2] = ['P' as u8, 'N' as u8];

/// The size of a PNB command
pub const PNB_CMD_SIZE: usize = PNB_PREFIX.len() + 4;

/// An error that can occur while attempting to read from a socket
#[derive(Debug)]
pub enum ReadError {
    EOF,
    DecompressorError(std::io::Error),
    SocketError(std::io::Error),
}

/// Line based codec.
///
/// This decorates a socket and presents a line based read / write interface.
///
/// As a user of `Lines`, we can focus on working at the line level. So, we
/// send and receive values that represent entire lines. The `Lines` codec will
/// handle the encoding and decoding as well as reading from and writing to the
/// socket.
pub struct Lines<'sock, 'decomp, T>
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

    /// A possible decompressor
    decoder: Option<ZstdDecompressor<'decomp>>,

    /// The amount of repeated binary commands to process
    repeated_binary_commands: u32,

    /// Server options
    opts: ServerOptions,
}

impl<'sock, 'decomp, T> Lines<'sock, 'decomp, T>
where
    T: AsyncRead + AsyncWrite,
{
    /// Create a new `Lines` codec backed by the socket
    pub fn new(
        socket: Pin<&'sock mut T>,
        stats: Arc<Stats>,
        pixmap: Arc<Pixmap>,
        opts: ServerOptions,
    ) -> Self {
        Lines {
            socket,
            rd: BytesMut::with_capacity(BUF_SIZE),
            wr: BytesMut::with_capacity(BUF_SIZE),
            stats,
            pixmap,
            decoder: None,
            repeated_binary_commands: 0,
            opts,
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
    ) -> Poll<Result<(), ReadError>> {
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
            Poll::Ready(Err(e)) => return Poll::Ready(Err(ReadError::SocketError(e))),
            _ => return Poll::Pending,
        };

        // poll_read returns Ok(0) if the other end has hung up/EOF has been reached
        if amount == 0 {
            return Poll::Ready(Err(ReadError::EOF));
        }

        let mut output_bytes = BytesMut::with_capacity(BUF_SIZE * 8);

        let extend_slice = if let Some(decompressor) = &mut self.decoder {
            match decompressor.decompress_stream(read_buf.filled(), &mut output_bytes) {
                Ok(_decompressed_bytes_read) => {}
                Err(e) => return Poll::Ready(Err(ReadError::DecompressorError(e))),
            }
            &output_bytes
        } else {
            read_buf.filled()
        };
        self.rd.extend_from_slice(extend_slice);

        self.stats.inc_bytes_read(amount);

        // We're done reading
        return Poll::Ready(Ok(()));
    }

    /// `input_bytes` must be at least [`BINARY_PX_SIZE`] bytes
    #[inline(always)]
    fn handle_pixel_bytes(input_bytes: &[u8]) -> (u16, u16, Color) {
        let x = u16::from_le_bytes(input_bytes[..2].try_into().expect("Huh"));
        let y = u16::from_le_bytes(input_bytes[2..4].try_into().expect("Huh"));

        let r = input_bytes[4];
        let g = input_bytes[5];
        let b = input_bytes[6];
        let a = input_bytes[7];
        (x, y, Color::from_rgba(r, g, b, a))
    }

    #[inline(always)]
    fn handle_cmd_result(self: &mut Pin<&mut Self>, result: CmdResult) -> Result<(), String> {
        // Do something with the result
        match result {
            // Do nothing
            CmdResult::Ok => Ok(()),

            // Respond to the client
            CmdResult::Response(msg) => {
                // Create a bytes buffer with the message
                self.buffer(format!("{}\r\n", msg).as_bytes());
                Ok(())
            }

            // Report the error to the user
            CmdResult::ClientErr(err) => {
                // Report the error to the client
                self.buffer(&format!("ERR {}\r\n", err).as_bytes());
                Err(format!("Client error: {}", err))
            }

            // Report the error to the server
            CmdResult::ServerErr(err) => {
                // Show an error message in the console
                println!("Client error \"{}\" occurred, disconnecting...", err);

                // Disconnect the client
                Err(format!("Client error occured. {}", err))
            }

            // Quit the connection
            CmdResult::Quit => Err("Client quit".to_string()),
        }
    }

    #[inline(always)]
    fn process_lines(
        self: &mut Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Result<(), String> {
        let mut pixels_set = 0;
        let result = loop {
            let rd_len = self.rd.len();

            // See if it's the specialized binary command
            let command = if self.opts.binary_command_support && self.rd.starts_with(&PXB_PREFIX) {
                if self.rd.len() >= PXB_CMD_SIZE {
                    let input_bytes = self.rd.split_to(PXB_CMD_SIZE);
                    let (x, y, color) = Self::handle_pixel_bytes(&input_bytes[PXB_PREFIX.len()..]);

                    // Set the pixel
                    if let Err(e) = self.pixmap.set_pixel(x as usize, y as usize, color) {
                        let line = format!("ERR {}\r\n", e);
                        let err_line = e.to_string();
                        self.buffer(line.as_bytes());
                        let _ = self.poll_flush(cx);
                        break Err(err_line);
                    }
                    pixels_set += 1;
                    continue;
                } else {
                    break Ok(pixels_set);
                }
            } else if self.opts.binary_command_support && self.rd.starts_with(&PNB_PREFIX) {
                if self.rd.len() >= PNB_CMD_SIZE {
                    let input_bytes = self.rd.split_to(PNB_CMD_SIZE);
                    const OFF: usize = PNB_PREFIX.len();
                    let pixels =
                        u32::from_le_bytes(input_bytes[OFF..OFF + 4].try_into().expect("hun"));
                    self.repeated_binary_commands = pixels;
                }
                break Ok(pixels_set);
            } else {
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

                    if self.opts.compression_support && &line[..] == b"COMPRESS" {
                        self.decoder = Some(ZstdDecompressor::new());
                        self.buffer(b"COMPRESS\r\n");

                        if let Poll::Pending = self.poll_flush(cx) {
                            break Err(
                                "Sending compressor confirmation message failed.".to_string()
                            );
                        }
                        line.truncate(0);
                        break Ok(pixels_set);
                    }

                    // Return the line
                    match Cmd::decode_line(line.freeze()) {
                        Ok(cmd) => cmd,
                        Err(e) => {
                            // Report the error to the client
                            self.buffer(&format!("ERR {}\r\n", e).as_bytes());
                            break Err("Command decoding failed.".to_string());
                        }
                    }
                } else if rd_len > LINE_MAX_LENGTH {
                    // If no line ending was found, and the buffer is larger than the
                    // maximum command length, disconnect

                    // TODO: report this error to the client
                    println!(
                        "Client sent a line longer than {} characters, disconnecting",
                        LINE_MAX_LENGTH,
                    );

                    // Break the connection, by ending the lines stream
                    break Err("Line sent by client was too long".to_string());
                } else {
                    // Didn't find any more data to process
                    break Ok(pixels_set);
                }
            };

            let result = command.invoke(&self.pixmap, &mut pixels_set, &self.opts);

            self.handle_cmd_result(result)?;
        };

        // Increase the amount of set pixels by the amount of pixel set commands
        // that we processed in this batch
        self.stats.inc_pixels_by_n(pixels_set);
        result.map(|pixels_set| {
            self.stats.inc_pixels_by_n(pixels_set);
            ()
        })
    }
}

impl<'sock, 'decomp, T> Future for Lines<'sock, 'decomp, T>
where
    T: AsyncRead + AsyncWrite,
{
    type Output = String;

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        // Try to read any new data into the read buffer
        let fill_read_buf = self.as_mut().fill_read_buf(cx);
        let new_rd_len = self.rd.len();

        match fill_read_buf {
            // An error occured (most likely disconnection)
            Poll::Ready(Err(e)) => {
                let message = match e {
                    ReadError::EOF => "EOF reached".to_string(),
                    ReadError::DecompressorError(e) => {
                        format!("Decompressor error: {:?}", e)
                    }
                    ReadError::SocketError(e) => format!("Socket error: {:?}", e),
                };

                self.buffer(&format!("ERR {}\r\n", message).as_bytes());
                let _ignored = self.poll_flush(cx);

                return Poll::Ready(format!("Client error occured. {}", message));
            }
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

        let command_result = if self.repeated_binary_commands == 0 {
            self.process_lines(cx)
        } else {
            let pixel_split_bytes = self
                .rd
                .len()
                .min(BINARY_PX_SIZE * self.repeated_binary_commands as usize);
            // Ensure that we split off a multiple of `BINARY_PX_SIZE` bytes for parsing by dividing by 9 to get
            // the floored value, and multiplying by `BINARY_PX_SIZE` again.
            let pixel_split_bytes = (pixel_split_bytes / BINARY_PX_SIZE) * BINARY_PX_SIZE;

            let mut split = &self.rd.split_to(pixel_split_bytes)[..];

            while split.len() > 0 {
                let (x, y, color) = Self::handle_pixel_bytes(&split[..BINARY_PX_SIZE]);

                if let Err(e) = self.pixmap.set_pixel(x as usize, y as usize, color) {
                    let e_str = e.to_string();
                    self.buffer("Error: coordinate out of bounds\r\n".to_string().as_bytes());
                    let _ = self.poll_flush(cx);
                    return Poll::Ready(format!("Client error: {:?}", e_str));
                }

                split = &split[BINARY_PX_SIZE..];
            }

            self.repeated_binary_commands -= (pixel_split_bytes / BINARY_PX_SIZE) as u32;
            self.stats
                .inc_pixels_by_n(pixel_split_bytes / BINARY_PX_SIZE);
            Ok(())
        };

        // This isn't used correctly: we probably need a (small) state machine
        // to determine that we should keep transmitting until we have emptied
        // our write buffer
        let _ = self.poll_flush(cx);

        match command_result {
            Ok(_) => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(m) => Poll::Ready(m),
        }
    }
}

#[tokio::test]
async fn response_newline() {
    let server_opts = ServerOptions {
        binary_command_support: true,
        compression_support: true,
    };

    let stats = Arc::new(Stats::new());
    let pixmap = Arc::new(Pixmap::new(400, 800));

    let mut test = tokio_test::io::Builder::new()
        // Test all commands that require a response
        .read(b"PX 16 16\r\n")
        .write(b"PX 16 16 000000\r\n")
        .read(b"SIZE\r\n")
        .write(b"SIZE 400 800\r\n")
        .read(b"HELP\r\n")
        .write(format!("{}\r\n", Cmd::help_list(&server_opts)).as_bytes())
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

    let lines = Lines::new(test, stats.clone(), pixmap.clone(), server_opts);

    lines.await;
}

#[tokio::test]
async fn compression() {
    let server_opts = ServerOptions {
        binary_command_support: true,
        compression_support: true,
    };

    let stats = Arc::new(Stats::new());
    let pixmap = Arc::new(Pixmap::new(400, 800));

    let compressed_data = zstd::encode_all(&b"PX 16 16 AABBCC\r\nPX 16 16\r\n"[..], 0).unwrap();

    let mut test = tokio_test::io::Builder::new()
        .read(b"COMPRESS\r\n")
        .write(b"COMPRESS\r\n")
        .read(&compressed_data)
        .write(b"PX 16 16 000000\r\n")
        .build();

    let test = Pin::new(&mut test);

    let lines = Lines::new(test, stats.clone(), pixmap.clone(), server_opts);

    lines.await;
}
