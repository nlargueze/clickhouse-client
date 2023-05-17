//! Compression

/// Compression method
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[allow(missing_docs)]
pub enum Compression {
    Gzip,
    Br,
    Deflate,
    Xz,
    Zstd,
    Lz4,
    Bz2,
    Snappy,
}

impl std::fmt::Display for Compression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Compression::Gzip => "gzip",
            Compression::Br => "br",
            Compression::Deflate => "deflate",
            Compression::Xz => "xz",
            Compression::Zstd => "zstd",
            Compression::Lz4 => "lz4",
            Compression::Bz2 => "bz2",
            Compression::Snappy => "snappy",
        };
        write!(f, "{s}")
    }
}
