//! SSTable binary format constants and footer read/write helpers.
//!
//! The footer is always the **last 12 bytes** of an SSTable file:
//!
//! ```text
//! [index_offset: u64 LE][magic: u32 LE = 0x5353_5431]
//! ```

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Read, Result as IoResult, Seek, SeekFrom, Write};

/// Magic number identifying SSTable v1 files (ASCII "SST1").
pub const SSTABLE_MAGIC: u32 = 0x5353_5431;

/// Size of the footer in bytes: 8 (`index_offset`) + 4 (`magic`).
pub const FOOTER_BYTES: u64 = 8 + 4;

/// Returns the byte offset where the footer starts: `filesize - 12`.
///
/// Uses [`u64::saturating_sub`] so files smaller than 12 bytes return 0
/// rather than underflowing.
pub fn footer_pos(filesize: u64) -> u64 {
    filesize.saturating_sub(FOOTER_BYTES)
}

/// Writes the SSTable footer (`index_offset` + `magic`) to `w`.
pub fn write_footer<W: Write>(w: &mut W, index_offset: u64) -> IoResult<()> {
    w.write_u64::<LittleEndian>(index_offset)?;
    w.write_u32::<LittleEndian>(SSTABLE_MAGIC)?;
    Ok(())
}

/// Reads the SSTable footer from `r`, returning `(index_offset, magic)`.
///
/// The reader is seeked to the end to determine file size, then to the
/// footer position. After this call the cursor is at the end of the file.
pub fn read_footer<R: Read + Seek>(r: &mut R) -> IoResult<(u64, u32)> {
    let filesize = r.seek(SeekFrom::End(0))?;
    r.seek(SeekFrom::Start(footer_pos(filesize)))?;
    let index_offset = r.read_u64::<LittleEndian>()?;
    let magic = r.read_u32::<LittleEndian>()?;
    Ok((index_offset, magic))
}
