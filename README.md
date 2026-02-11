datu - a data file utility
=======================

`datu` is intended to be a lightweight, fast, and versatile CLI tool for reading, querying, and converting data in various file formats, such as Parquet, .XLSX, CSV, and even f3.

It is used non-interactively: you invoke a subcommand with arguments on the CLI or from scripts for automated pipelines.

Internally, it also uses a pipeline architecture that aids in extensibility and testing, as well as allowing for parallel processing even of large datasets, if the input/output formats support it.

## How it Works Internally

Internally, `datu` constructs a pipeline based on the command and arguments.


For example, the following invocation

```sh
datu convert input.parquet output.csv --select id,name,email
```

constructs a pipeline that reads the input, selects only the specified columns, and writes the output.

## Examples

### `schema`

Display the schema of a Parquet or Avro file (column names, types, and nullability). Useful for inspecting file structure without reading data.

**Supported input formats:** Parquet (`.parquet`, `.parq`), Avro (`.avro`).

**Usage:**

```sh
datu schema <FILE> [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `--output <FORMAT>` | Output format: `csv`, `json`, or `yaml`. Case insensitive. Default: `csv`. |

**Output formats:**

- **csv** (default): One line per column, e.g. `name: String (UTF8), nullable`.
- **json**: Pretty-printed JSON array of objects with `name`, `data_type`, `nullable`, and optionally `converted_type` (Parquet).
- **yaml**: YAML list of mappings with the same fields.

**Examples:**

```sh
# Default CSV-style output
datu schema data.parquet

# JSON output
datu schema data.parquet --output json

# YAML output (e.g. for config or tooling)
datu schema events.avro --output yaml
datu schema events.avro -o YAML
```

---

### `convert`

Convert data between supported formats. Input and output formats are inferred from file extensions.

**Supported input formats:** Parquet (`.parquet`, `.parq`), Avro (`.avro`).

**Supported output formats:** CSV (`.csv`), JSON (`.json`), Parquet (`.parquet`, `.parq`), Avro (`.avro`), XLSX (`.xlsx`).

**Usage:**

```sh
datu convert <INPUT> <OUTPUT> [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `--select <COLUMNS>...` | Columns to include. If not specified, all columns are written. Column names can be given as multiple arguments or as comma-separated values (e.g. `--select id,name,email` or `--select id --select name --select email`). |
| `--limit <N>` | Maximum number of records to read from the input. |

**Examples:**

```sh
# Parquet to CSV (all columns)
datu convert data.parquet data.csv

# Parquet to Avro (first 1000 rows)
datu convert data.parquet data.avro --limit 1000

# Avro to CSV, only specific columns
datu convert events.avro events.csv --select id,timestamp,user_id

# Parquet to Parquet with column subset
datu convert input.parq output.parquet --select one,two,three

# Parquet or Avro to Excel (.xlsx)
datu convert data.parquet report.xlsx
datu convert events.avro report.xlsx --select id,name,value

# Parquet or Avro to JSON
datu convert data.parquet data.json
```

---

### `head`

Print the first N rows of a Parquet or Avro file as CSV to stdout.

**Supported input formats:** Parquet (`.parquet`, `.parq`), Avro (`.avro`).

**Usage:**

```sh
datu head <INPUT> [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `-n`, `--number <N>` | Number of rows to print. Default: 10. |
| `--select <COLUMNS>...` | Columns to include. If not specified, all columns are printed. Same format as `convert --select`. |

**Examples:**

```sh
# First 10 rows (default)
datu head data.parquet

# First 100 rows
datu head data.parquet -n 100
datu head data.avro --number 100

# First 20 rows, specific columns
datu head data.parquet -n 20 --select id,name,email
```

---

### `tail`

Print the last N rows of a Parquet or Avro file as CSV to stdout.

**Supported input formats:** Parquet (`.parquet`, `.parq`), Avro (`.avro`).

**Usage:**

```sh
datu tail <INPUT> [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `-n`, `--number <N>` | Number of rows to print. Default: 10. |
| `--select <COLUMNS>...` | Columns to include. If not specified, all columns are printed. Same format as `convert --select`. |

**Examples:**

```sh
# Last 10 rows (default)
datu tail data.parquet

# Last 50 rows
datu tail data.parquet -n 50
datu tail data.avro --number 50

# Last 20 rows, specific columns
datu tail data.parquet -n 20 --select id,name,email

# Redirect tail output to a file
datu tail data.parquet -n 1000 > last1000.csv
```

---

### Version

Print the installed `datu` version:

```sh
datu version
```
