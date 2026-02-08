use arrow::array::RecordBatchReader;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::Error;
use crate::Result;
use crate::pipeline::RecordBatchReaderSource;

/// Arguments for reading a parquet file
pub struct ReadParquetArgs {
    pub path: String,
    pub limit: Option<usize>,
}

/// A step in a pipeline that reads a parquet file
pub struct ReadParquetStep {
    pub args: ReadParquetArgs,
}

impl RecordBatchReaderSource for ReadParquetStep {
    fn get_record_batch_reader(&mut self) -> Result<Box<dyn RecordBatchReader + 'static>> {
        read_parquet(&self.args)
            .map(|reader| Box::new(reader) as Box<dyn RecordBatchReader + 'static>)
    }
}

/// Read a parquet file and return a RecordBatchReader.
pub fn read_parquet(args: &ReadParquetArgs) -> Result<ParquetRecordBatchReader> {
    let file = std::fs::File::open(&args.path).map_err(Error::IoError)?;

    let mut builder =
        ParquetRecordBatchReaderBuilder::try_new(file).map_err(Error::ParquetError)?;
    if let Some(limit) = args.limit {
        builder = builder.with_limit(limit);
    }
    builder.build().map_err(Error::ParquetError)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_parquet() {
        let args = ReadParquetArgs {
            path: "fixtures/table.parquet".to_string(),
            limit: None,
        };
        let mut reader =
            read_parquet(&args).expect("read_parquet failed to return a ParquetRecordBatchReader");
        let batch = reader
            .next()
            .expect("None")
            .map_err(Error::ArrowError)
            .expect("Unable to read batch");
        assert_eq!(batch.num_rows(), 3, "Expected 3 rows");
    }

    #[test]
    fn test_read_parquet_with_limit() {
        let args = ReadParquetArgs {
            path: "fixtures/table.parquet".to_string(),
            limit: Some(1),
        };
        let mut reader =
            read_parquet(&args).expect("read_parquet failed to return a ParquetRecordBatchReader");
        let batch = reader
            .next()
            .expect("None")
            .map_err(Error::ArrowError)
            .expect("Unable to read batch");
        assert_eq!(batch.num_rows(), 1, "Expected only 1 row");
    }
}
