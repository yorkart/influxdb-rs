pub mod tsm1;

pub(crate) const MAX_TSMFILE_SIZE: u32 = 2048 * 1024 * 1024; // 2GB

/// COMPACTION_TEMP_EXTENSION is the extension used for temporary files created during compaction.
pub(crate) const COMPACTION_TEMP_EXTENSION: &'static str = "tmp";

/// TSMFILE_EXTENSION is the extension used for TSM files.
pub(crate) const TSM_FILE_EXTENSION: &'static str = "tsm";
