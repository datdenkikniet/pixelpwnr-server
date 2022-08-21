use tokio_test::io::Builder;

use super::*;

use std::{pin::Pin, sync::Arc};

const SERVER_OPTS: ServerOptions = ServerOptions {
    binary_command_support: true,
    compression_support: true,
};

fn setup() -> (Arc<Stats>, Arc<Pixmap>) {
    let stats = Arc::new(Stats::new());
    let pixmap = Arc::new(Pixmap::new(400, 800));
    (stats, pixmap)
}

#[tokio::test]
async fn response_newline() {
    let (stats, pixmap) = setup();

    let mut test = Builder::new()
        // Test all commands that require a response
        .read(b"PX 16 16\r\n")
        .write(b"PX 16 16 000000\r\n")
        .read(b"SIZE\r\n")
        .write(b"SIZE 400 800\r\n")
        .read(b"HELP\r\n")
        .write(format!("{}\r\n", Cmd::help_list(&SERVER_OPTS)).as_bytes())
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

    let lines = Lines::new(test, stats.clone(), pixmap.clone(), SERVER_OPTS);

    lines.await;
}

#[tokio::test]
async fn binary_command() {
    let (stats, pixmap) = setup();

    let mut test = Builder::new()
        // Verify the size
        .read(&[b'P', b'B', 5, 0, 5, 0, 0xAB, 0xCD, 0xEF, 0xFF])
        .read(b"PX 5 5\n")
        .write(b"PX 5 5 ABCDEF\r\n")
        .build();

    let test = Pin::new(&mut test);

    let lines = Lines::new(test, stats.clone(), pixmap.clone(), SERVER_OPTS);

    lines.await;
}

#[tokio::test]
async fn compression() {
    let (stats, pixmap) = setup();

    let compressed_data = zstd::encode_all(&b"PX 16 16 AABBCC\r\nPX 16 16\r\n"[..], 0).unwrap();

    let mut test = tokio_test::io::Builder::new()
        .read(b"COMPRESS\r\n")
        .write(b"COMPRESS\r\n")
        .read(&compressed_data)
        .write(b"PX 16 16 AABBCC\r\n")
        .build();

    let test = Pin::new(&mut test);

    let lines = Lines::new(test, stats.clone(), pixmap.clone(), SERVER_OPTS);

    lines.await;
}
