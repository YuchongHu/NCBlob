mod cache;
mod local_filesystem;
#[cfg(feature = "memmap")]
mod mapped_file;
#[cfg(feature = "sqlite")]
mod sqlite;

pub mod prelude {
    pub use super::cache::*;
    pub use super::local_filesystem::*;
    #[cfg(feature = "memmap")]
    pub use super::mapped_file::*;
    #[cfg(feature = "sqlite")]
    pub use super::sqlite::*;
}

mod helpers {
    /// check if this range contains that range
    #[inline]
    pub(crate) fn range_contains<I: Sized + std::cmp::PartialOrd>(
        this: &std::ops::Range<I>,
        that: &std::ops::Range<I>,
    ) -> bool {
        this.start <= that.start && this.end >= that.end
    }
}
