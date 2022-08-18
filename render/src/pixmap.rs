use lazy_static::lazy_static;
use std::ptr;

use crate::color::Color;

lazy_static! {
    /// The default color value for each pixel
    static ref DEFAULT_PIXEL: u32 = Color::black().to_raw();
}

/// A struct representing a pixelmap for pixelflut.
///
/// This struct holds the data for each pixel, and can be concidered a bitmap.
/// For each pixel, a u32 (DWORD) value is used containing 4 bytes that define
/// the value for each of the 4 color channels.
///
/// This data structure is focussed on performance and multithreaded use with
/// multiple readers and writers.  This structure does not use any kind of
/// locks. Instead, it is assumed that the operations done on the internal map
/// are atomic (on a pixel basis).  This is perfectly fine for what this
/// pixelmap is used for.
///
/// Because this structure is aligned to 4 bytes in memory, each raw color
/// value (u32) is also aligned to 4 bytes. This makes direct reads and writes
/// on these values on most CPUs (but not all!). The fact that this may not be
/// atomic in some cases is accepted for this structure. The speed of not using
/// locks is preferred over the minor side effect of seldom rendering artifact
/// on some systems.
///
/// More info: https://stackoverflow.com/a/5002256/1000145
///
/// Important: this data structure is considered unsafe, but is perfectly
/// usable for pixelflut applications.
#[repr(align(4))]
pub struct Pixmap {
    /// A map with a raw color value for each pixel in the map, where each
    /// pixel consists of 4 bytes in a single u32 for each color channel.
    map: Vec<u32>,

    /// Pixelmap dimensions, width and height
    dimensions: (usize, usize),
}

impl Pixmap {
    /// Construct a new
    pub fn new(width: usize, height: usize) -> Self {
        Pixmap {
            // Build a pixel map, with the default value and the proper sizeto
            // fit each pixel
            map: vec![*DEFAULT_PIXEL; width * height],

            // Set the dimensions
            dimensions: (width, height),
        }
    }

    /// Get the width of the pixel map.
    pub fn width(&self) -> usize {
        self.dimensions.0
    }

    /// Get the height of the pixel map.
    pub fn height(&self) -> usize {
        self.dimensions.1
    }

    /// Get the dimensions of the pixel map.
    #[allow(dead_code)]
    pub fn dimensions(&self) -> (usize, usize) {
        self.dimensions
    }

    /// Get the pixel at the given coordinate, as color.
    #[allow(dead_code)]
    pub fn pixel(&self, x: usize, y: usize) -> Result<Color, PixmapErr> {
        Ok(Color::new(self.pixel_raw(x, y)?))
    }

    /// Get the pixel at the given coordinate, as raw color value.
    pub fn pixel_raw(&self, x: usize, y: usize) -> Result<u32, PixmapErr> {
        Ok(self.map[self.pixel_index(x, y)?])
    }

    /// Set the pixel at the given coordinate, to the given color.
    pub fn set_pixel(&self, x: usize, y: usize, color: Color) -> Result<(), PixmapErr> {
        let mut current_color = self.pixel(x, y)?;
        current_color.blend(color);
        self.set_pixel_raw(x, y, current_color.to_raw())
    }

    /// Set the pixel at the given coordinate, to the given raw color value.
    pub fn set_pixel_raw(&self, x: usize, y: usize, raw: u32) -> Result<(), PixmapErr> {
        // Determine the pixel index
        let index = self.pixel_index(x, y)?;

        // Write the pixel data
        unsafe {
            Pixmap::write_pixel_raw(&self.map, index, raw);
        }

        Ok(())
    }

    /// Write raw pixel data to the given pixel `map`.
    ///
    /// Note: this function writes raw pixel data on the pixel map at the
    /// given index, even though the map itself is immutable.
    /// This allows multiple writes from multiple threads at the same time.
    /// This operation is considered safe however, as the writen set of bytes
    /// is aligned.
    /// See the description of this struct for more information.
    ///
    /// Note: this method does not check for pixel index bounds, as it's only
    /// used in this structure internally.
    unsafe fn write_pixel_raw(map: &Vec<u32>, i: usize, raw: u32) {
        // Create a mutable pointer, to the pixel data on the immutable pixel map
        let pixel_ptr: *mut u32 = (&map[i] as *const u32) as *mut u32;

        // Write the new raw value to the pointer
        ptr::write(pixel_ptr, raw);
    }

    /// Get the index a pixel is at, for the given coordinate.
    fn pixel_index(&self, x: usize, y: usize) -> Result<usize, PixmapErr> {
        // Check pixel bounds
        if x >= self.dimensions.0 {
            return Err(PixmapErr::OutOfBound("x coordinate out of bound"));
        } else if y >= self.dimensions.1 {
            return Err(PixmapErr::OutOfBound("y coordinate out of bound"));
        }

        // Determine the index and return
        Ok(y * self.dimensions.0 + x)
    }

    /// Get the pixelmap data, as slice with the raw color value of each
    /// pixel.
    ///
    /// Note: this method returns a single u32 for each pixel, instead of 4
    /// u8 bytes for each pixel as the `as_bytes()` method does.
    pub fn as_slice(&self) -> &[u32] {
        self.map.as_slice()
    }

    /// Get the pixelmap data, as a slice of bytes.
    ///
    /// Each pixel consumes a sequence of 4 bytes, each defining the value of
    /// a different color channel.
    ///
    /// This data may be used to send to the GPU, as raw texture buffer, for
    /// rendering.
    pub fn as_bytes(&self) -> &[u8] {
        // The following code transmutes the raw slice bytes from the
        // `[u32; size]` type into `[u8; size * 4]`. Cloning the data array
        // and casting each raw value to 4 u8 bytes if a very expensive
        // operation to do each frame for such a big array of pixels.
        // Transmuting is considered unsafe, but usually is about a 1000 times
        // faster resulting in insane performance gains. This unsafe bit of
        // code is desirable over safe code that is enormously slower.
        // The implementation below is memory safe.

        let slice_u32 = self.as_slice();
        let len = slice_u32.len() * 4;

        unsafe { core::slice::from_raw_parts(slice_u32.as_ptr() as _, len) }
    }
}

unsafe impl Send for Pixmap {}
unsafe impl Sync for Pixmap {}

/// An error representation for pixel map operations.
#[derive(Debug)]
pub enum PixmapErr<'a> {
    /// The given pixel coordinate or index is out of bound.
    OutOfBound(&'a str),
}
