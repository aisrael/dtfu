//! `dtfu schema` - display the schema of a Parquet or Avro file

use std::fmt::Display;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

use anyhow::Result;
use anyhow::bail;
use arrow_avro::reader::ReaderBuilder;
use dtfu::FileType;
use dtfu::cli::SchemaArgs;
use parquet::basic::ConvertedType;
use parquet::file::metadata::ParquetMetaDataReader;
use parquet::schema::types::ColumnDescriptor;

struct SchemaOutput {
    column_name: String,
    data_type: String,
    converted_type: Option<ConvertedType>,
    nullable: bool,
}

impl Display for SchemaOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let nullable = if self.nullable { ", nullable" } else { "" };
        if let Some(converted_type) = &self.converted_type {
            write!(
                f,
                "{}: {} ({:?}){}",
                self.column_name, self.data_type, converted_type, nullable
            )
        } else {
            write!(f, "{}: {}{}", self.column_name, self.data_type, nullable)
        }
    }
}

/// Map an element of `SchemaDescriptor::columns()` into a `SchemaOutput`
fn column_to_schema_output(column: &Arc<ColumnDescriptor>) -> SchemaOutput {
    let path = column.path();
    let physical_type = column.physical_type();
    let logical_type = column.logical_type_ref();
    let converted_type = column.converted_type();

    let column_name = path.parts().join(".");

    let data_type = if let Some(logical) = logical_type {
        format!("{:?}", logical)
    } else {
        format!("{}", physical_type)
    };

    let converted_type = if matches!(converted_type, ConvertedType::NONE) {
        None
    } else {
        Some(converted_type)
    };

    // A column is nullable if max_def_level > 0
    // max_def_level == 0 means the column is required (not nullable)
    let nullable = column.max_def_level() > 0;

    SchemaOutput {
        column_name,
        data_type,
        converted_type,
        nullable,
    }
}

fn schema_avro(path: &str) -> Result<()> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let arrow_reader = ReaderBuilder::new().build(reader)?;
    let schema = arrow_reader.schema();
    for field in schema.fields() {
        let nullable = if field.is_nullable() {
            ", nullable"
        } else {
            ""
        };
        println!(
            "{name}: {dt:?}{nullable}",
            name = field.name(),
            dt = field.data_type(),
            nullable = nullable
        );
    }
    Ok(())
}

/// The `dtfu schema` command
pub fn schema(args: SchemaArgs) -> Result<()> {
    let file_type: FileType = args.file.as_str().try_into()?;
    match file_type {
        FileType::Parquet => schema_parquet(&args.file),
        FileType::Avro => schema_avro(&args.file),
        _ => bail!("schema is only supported for Parquet and Avro files"),
    }
}

fn schema_parquet(path: &str) -> Result<()> {
    let file = File::open(path)?;
    let metadata = ParquetMetaDataReader::new()
        .parse_and_finish(&file)
        .map_err(anyhow::Error::from)?;

    let file_metadata = metadata.file_metadata();
    let schema_descr = file_metadata.schema_descr();

    let columns: Vec<SchemaOutput> = schema_descr
        .columns()
        .iter()
        .map(column_to_schema_output)
        .collect();

    for column in columns {
        println!("{column}");
    }

    Ok(())
}
