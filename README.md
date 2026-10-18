# Kamenx
kamenx is super simple search engine based on commoncrawl dataset 

## Try it out
**[-> slop.racek.xyz](https://slop.racek.xyz)**
demo holds dataset with 10M pages
## What is this
This is extremely simple search engine built as simple project, it's not intended real use the search quality is bad and its to be expected. 
I'm probably gonna release second version with entire search pipeline reworked.

## Technologia
Download:
1. Start downloading common crawl index 
2. Filter it in realtime 
3. Store everything in duckdb
    - you can stop, resume download at any time without loosing any progress

Search:
1. Download duckdb BM25 plugin
2. index entire database on snippets of page
3. search database with BM25

quickstart:

### Dataset gathering
```bash
cargo run --release -- download
# wait 5-30 minutes or more based on how big dataset you want than Ctrl-C
```
### Search backend
```
cargo run --release -- search
```
### Search frontned
```
cd frontend
pnpm install
pnpm run dev
```
after that open localhost:5173 to try out the searchengine
