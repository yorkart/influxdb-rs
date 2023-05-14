pub mod tsm1;

pub const MAX_TSM_FILE_SIZE: u32 = 2048 * 1024 * 1024; // 2GB

/// COMPACTION_TEMP_EXTENSION is the extension used for temporary files created during compaction.
pub(crate) const COMPACTION_TEMP_EXTENSION: &'static str = "tmp";

/// TSMFILE_EXTENSION is the extension used for TSM files.
pub const TSM_FILE_EXTENSION: &'static str = "tsm";

/// The extension used to describe corrupt snapshot files.
pub const BAD_TSM_FILE_EXTENSION: &'static str = "bad";
