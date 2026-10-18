use clap::{Parser, Subcommand};
mod index;
mod wet_crawl;
mod crawl;
mod search;
use crate::index::{download_index, prepare_db};
use crate::search::open_search;
use crate::crawl::{start_crawl};
use crate::wet_crawl::{start_wet_crawl};

#[derive(Parser)]
#[command(name = "kamenx", version, about = "search engine tool", long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// List things
    List,
    /// Manage indexes
    Search,
    Download, 
}


#[tokio::main]
async fn main() {
    let args = Args::parse();

    match args.command {
        Command::List => println!("list"),
        Command::Search => {
                open_search().await; 
        },
        Command::Download => {
                start_wet_crawl();
        }
    }
}
