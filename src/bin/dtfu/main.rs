//! dtfu - a data multi-tool CLI

use clap::Parser;
use dtfu::cli::Command;

mod commands;

use commands::convert;

/// dtfu - a data multi-tool
#[derive(Parser)]
#[command(name = "dtfu")]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Convert(args) => convert(args),
        Command::Version => {
            println!("dtfu v{}", dtfu::VERSION);
            Ok(())
        }
    }
}
