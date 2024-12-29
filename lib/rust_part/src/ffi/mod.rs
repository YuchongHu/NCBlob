// generate ffi bindings

mod local_file_system;
mod memory_cache;
#[cfg(feature = "memmap")]
mod memmap;
#[cfg(feature = "sqlite")]
mod sqlite;

mod azure_trace;