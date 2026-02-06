//! Contains shared types for the `dtfu` CLI and implementation.

use clap::Args;
use clap::Subcommand;

/// The `dtfu` CLI top-level command
#[derive(Subcommand)]
pub enum Command {
    Convert(ConvertArgs),
    Version,
}

/// convert command arguments
#[derive(Args)]
pub struct ConvertArgs {
    pub input: String,
    pub output: String,
}
