use clap::{Parser, Subcommand};
mod index;
mod wet_crawl;
mod crawl;
use crate::index::{download_index, prepare_db};
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
    Index {
        #[command(subcommand)]
        action: IndexAction,
    },
    Crawl {
        #[command(subcommand)]
        action: CrawlAction
    }
}

#[derive(Subcommand)]
enum CrawlAction{
    /// Start crawling
    Start{
    }
}
#[derive(Subcommand)]
enum IndexAction {
    /// Download entire common crawl index
    Download {
    /// Index size limit in GB default: unlimited
    #[arg(short,long, default_value="0")]
    limit: i8

    },
    /// Merge entire index into one database
    Load{
    #[arg(short,long)]
    limit:u64
    }
}

fn main() {
    let args = Args::parse();

    match args.command {
        Command::List => println!("list"),
        Command::Index { action } => match action {
            IndexAction::Download { limit } => {
                download_index(limit);
            },
            IndexAction::Load {limit} => {
                prepare_db(limit);
            }
        },
        Command::Crawl { action } => match action {
            CrawlAction::Start {} =>{
                start_wet_crawl();
            }
        }
    }
}
