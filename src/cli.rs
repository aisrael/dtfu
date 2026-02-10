//! Contains shared types for the `dtfu` CLI and implementation.

use clap::Args;
use clap::Subcommand;

/// The `dtfu` CLI top-level command
#[derive(Subcommand)]
pub enum Command {
    Convert(ConvertArgs),
    Head(HeadArgs),
    Tail(TailArgs),
    Version,
}

/// head command arguments
#[derive(Args)]
pub struct HeadArgs {
    pub input: String,
    #[arg(short = 'n', long, default_value_t = 10, help = "Number of lines to print.")]
    pub number: usize,
}

/// tail command arguments
#[derive(Args)]
pub struct TailArgs {
    pub input: String,
    #[arg(short = 'n', long, default_value_t = 10, help = "Number of lines to print.")]
    pub number: usize,
}

/// convert command arguments
#[derive(Args)]
pub struct ConvertArgs {
    pub input: String,
    pub output: String,
    #[arg(
        long,
        help = "Columns to select. If not specified, all columns will be selected."
    )]
    pub select: Option<Vec<String>>,
    #[arg(long, help = "Maximum number of records to read from the input.")]
    pub limit: Option<usize>,
}
