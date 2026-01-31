//! dtfu - a data multi-tool CLI

use clap::Parser;
use clap::Subcommand;

/// dtfu - a data multi-tool
#[derive(Parser)]
#[command(name = "dtfu")]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Version,
}

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Command::Version => println!("dtfu v{}", dtfu::VERSION),
    }
}
