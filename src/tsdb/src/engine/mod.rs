pub mod tsm1;

pub(crate) const maxTSMFileSize: u32 = 2048 * 1024 * 1024; // 2GB

/// CompactionTempExtension is the extension used for temporary files created during compaction.
pub(crate) const CompactionTempExtension: &'static str = "tmp";

/// TSMFileExtension is the extension used for TSM files.
pub(crate) const TSMFileExtension: &'static str = "tsm";
