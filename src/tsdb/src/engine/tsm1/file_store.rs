/// The extension used to describe temporary snapshot files.
pub(crate) const TMP_TSMFILE_EXTENSION: &'static str = "tmp";

/// The extension used to describe corrupt snapshot files.
pub(crate) const BAD_TSMFILE_EXTENSION: &'static str = "bad";

/// TSMFile represents an on-disk TSM file.
pub(crate) trait TSMFile {
    /// Path returns the underlying file path for the TSMFile.  If the file
    /// has not be written or loaded from disk, the zero value is returned.
    fn path(&self) -> &str;

    // Read returns all the values in the block where time t resides.
    // fn read(&self, key: &[]byte, t i64) -> anyhow::Result<>;
}
