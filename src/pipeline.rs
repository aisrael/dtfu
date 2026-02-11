//! The `pipeline` module is the core of the dtfu crate.

pub mod avro;
pub mod csv;
pub mod json;
pub mod parquet;
pub mod record_batch_filter;
pub mod xlsx;

use arrow::array::RecordBatchReader;

use crate::Result;

/// Arguments for writing a file (CSV, Avro, Parquet, JSON, XLSX).
pub struct WriteArgs {
    pub path: String,
}

/// Concrete operations that can be executed in a pipeline
pub enum Operation {
    ReadAvro(avro::ReadAvroStep),
    ReadParquet(parquet::ReadParquetStep),
    WriteAvro(WriteArgs),
    WriteParquet(WriteArgs),
    WriteCsv(WriteArgs),
    WriteJson(WriteArgs),
    WriteXlsx(WriteArgs),
}

/// A `Step` defines a step in the pipeline that can be executed
/// and has an input and output type.
pub trait Step {
    type Input;
    type Output;

    /// Execute the step
    fn execute(self) -> Result<Self::Output>;
}

/// A source of `RecordBatchReader`
pub trait RecordBatchReaderSource {
    fn get_record_batch_reader(&mut self) -> Result<Box<dyn RecordBatchReader>>;
}

/// A RecordBatchReader that limits the number of rows read.
pub struct LimitingRecordBatchReader<Inner: RecordBatchReader + 'static> {
    inner: Inner,
    limit: usize,
    records_read: usize,
}
