use std::hash::{DefaultHasher, Hash, Hasher};

use ffi::{BlobAccessTrace, BlobType};

use csv::StringRecord;

#[cxx::bridge(namespace = "azure_trace_rs")]
mod ffi {
    #[repr(u8)]
    #[derive(Debug, Copy, Clone)]
    enum BlobType {
        Application,
        Image,
        Text,
        None,
        Other,
    }

    #[derive(Debug, Copy, Clone)]
    enum TraceError {
        Exhaust,
        BadRecord,
        Io,
        Other,
    }

    #[derive(Debug, Clone)]
    struct BlobAccessTrace {
        pub time_stamp: u64,
        pub region_id: u64,
        pub user_id: u64,
        pub app_id: u64,
        pub func_id: u64,
        pub blob_id: usize,
        pub blob_type: BlobType,
        pub version_tag: u64,
        pub size: usize,
        pub read: bool,
        pub write: bool,
    }

    extern "Rust" {
        #[cxx_name = "reader"]
        type BlobAccessTraceReader;

        fn open_reader(file: &str) -> Result<Box<BlobAccessTraceReader>>;
        fn next_record(&mut self) -> Result<BlobAccessTrace>;
        #[cxx_name = "str_to_err"]
        fn str_to_trace_err(string: &str) -> TraceError;
        #[cxx_name = "err_to_str"]
        fn trace_err_to_str(err: TraceError) -> &'static str;
    }
}

impl std::fmt::Display for ffi::TraceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(trace_err_to_str(*self))
    }
}

fn str_to_trace_err(string: &str) -> ffi::TraceError {
    match string {
        "Exhaust" => ffi::TraceError::Exhaust,
        "BadRecord" => ffi::TraceError::BadRecord,
        "Io" => ffi::TraceError::Io,
        "Other" => ffi::TraceError::Other,
        _ => ffi::TraceError::Other,
    }
}

fn trace_err_to_str(err: ffi::TraceError) -> &'static str {
    match err {
        ffi::TraceError::Exhaust => "Exhaust",
        ffi::TraceError::BadRecord => "BadRecord",
        ffi::TraceError::Io => "Io",
        ffi::TraceError::Other => "Other",
        _ => "Unknown",
    }
}

struct BlobAccessTraceReader {
    records: csv::StringRecordsIntoIter<std::io::BufReader<std::fs::File>>,
}

fn open_reader(file: &str) -> Result<Box<BlobAccessTraceReader>, ffi::TraceError> {
    let file = std::fs::File::open(file).map_err(|_| ffi::TraceError::Io)?;
    let reader = csv::Reader::from_reader(std::io::BufReader::new(file));
    Ok(Box::new(BlobAccessTraceReader {
        records: reader.into_records(),
    }))
}

impl BlobAccessTraceReader {
    fn next_record(&mut self) -> Result<BlobAccessTrace, ffi::TraceError> {
        for record in self.records.by_ref() {
            let rcd = record
                .map_err(|_| ffi::TraceError::BadRecord)
                .map(parse_record);
            if let Ok(rcd) = rcd {
                return Ok(rcd);
            }
        }
        Err(ffi::TraceError::Exhaust)
    }
}

impl std::str::FromStr for BlobType {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(make_blob_type_from_str(s))
    }
}

impl From<&str> for BlobType {
    fn from(s: &str) -> Self {
        make_blob_type_from_str(s)
    }
}

fn make_blob_type_from_str(str: &str) -> ffi::BlobType {
    use ffi::BlobType;
    match str.split('/').nth(1) {
        Some("application") => BlobType::Application,
        Some("image") => BlobType::Image,
        Some("text") => BlobType::Text,
        Some("none") => BlobType::None,
        Some(_) => BlobType::Other,
        None => BlobType::Other,
    }
}

fn parse_record(record: StringRecord) -> BlobAccessTrace {
    // csv HEAD format
    // Timestamp,AnonRegion,AnonUserId,AnonAppName,AnonFunctionInvocationId,AnonBlobName,BlobType,AnonBlobETag,BlobBytes,Read,Write
    let timestamp = record.get(0).unwrap().parse::<u64>().unwrap();
    let region = record.get(1).unwrap().to_string();
    let user_id = record.get(2).unwrap().to_string();
    let app_name = record.get(3).unwrap().to_string();
    let func_id = record.get(4).unwrap().to_string();
    let blob_name = record.get(5).unwrap().to_string();
    let blob_type = record.get(6).unwrap().to_string();
    let blob_tag = record.get(7).unwrap().to_string();
    let blob_bytes = record
        .get(8)
        .unwrap()
        .parse::<f64>()
        .unwrap_or_default()
        .round() as usize;
    let read = record
        .get(9)
        .unwrap()
        .to_lowercase()
        .parse::<bool>()
        .unwrap();
    let write = record
        .get(10)
        .unwrap()
        .to_lowercase()
        .parse::<bool>()
        .unwrap();
    BlobAccessTrace {
        time_stamp: timestamp,
        region_id: {
            // hash region string to u64
            let mut hasher = DefaultHasher::new();
            region.hash(&mut hasher);
            hasher.finish()
        },
        user_id: {
            // hash user_id string to u64
            let mut hasher = DefaultHasher::new();
            user_id.hash(&mut hasher);
            hasher.finish()
        },
        app_id: {
            // hash app_name string to u64
            let mut hasher = DefaultHasher::new();
            app_name.hash(&mut hasher);
            hasher.finish()
        },
        func_id: {
            // hash func_id string to u64
            let mut hasher = DefaultHasher::new();
            func_id.hash(&mut hasher);
            hasher.finish()
        },
        blob_id: {
            // hash blob_name string to u64
            let mut hasher = DefaultHasher::new();
            blob_name.hash(&mut hasher);
            usize::try_from(hasher.finish()).unwrap()
        },
        blob_type: blob_type.as_str().into(),
        version_tag: {
            // hash blob_tag string to u64
            let mut hasher = DefaultHasher::new();
            blob_tag.hash(&mut hasher);
            hasher.finish()
        },
        size: blob_bytes,
        read,
        write,
    }
}
