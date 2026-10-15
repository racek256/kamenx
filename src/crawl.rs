use duckdb::Connection;
use std::sync::{Arc, Mutex};
use rs_trafilatura::{ExtractResult, extract};
use std::io::Read;
use flate2::bufread::GzDecoder;
use crossbeam_channel::bounded;
use std::thread;


struct Page{
    file:String,
    url:String,
    offset:i32,
    length:i32
}

struct RawPage{
    url:String,
    content:String,
}
struct ParsedPage{
    url:String,
    content:ExtractResult
}

pub fn start_crawl(){
    // Connect to index duckdb
    let index_conn = Connection::open("./index/index.db").unwrap();
    let pages_count:i64 = index_conn.query_row("select count(*) from data",[], |row| row.get(0)).unwrap();
    if pages_count < 1000{
        println!("You have too small index db gather at least 1000 pages into index");
        return;
    }else{
        println!("Downloader is preparing to blast your computer into outer space with {}pages",pages_count)
    }

    // Create connection to Downloader DB 
    let db_path = "./crawl/crawl.db";
    let conn = Connection::open(db_path).unwrap();

    let mut stmt = index_conn.prepare(
        "select warc_filename, warc_record_offset, warc_record_length, url from data limit 5"
    ).unwrap();
    
    let pages: Vec<Page> = stmt
    .query_map([], |row| {
        Ok(Page {
            file: row.get(0)?,
            offset: row.get(1)?,
            length: row.get(2)?,
            url: row.get(3)?,
        })
    }).unwrap()
        .collect::<Result<Vec<_>, _>>().unwrap();




    //downloader(pages);

    let value = Arc::new(Mutex::new(0i32));

    // 2. Stats loop needs its own clone to READ the value
    let stats_value = Arc::clone(&value);



    let downloader_threads = 2; // More than 2 and you are risking ip ban
    let parser_threads = 2;


    // Multiplayer between fetcher -> Downloaders
    let (page_tx, page_rx) = bounded::<Page>(64);
    // Multiplayer between Downloaders -> parsers
    let (raw_tx, raw_rx) = bounded::<RawPage>(64);
    // Multiplayer between parser -> writer
    let (parsed_tx, parsed_rx) = bounded::<ParsedPage>(64);

    let (p_rx, r_rx, w_rx) = (page_rx.clone(), raw_rx.clone(), parsed_rx.clone());
    thread::spawn(move || loop {
        thread::sleep(std::time::Duration::from_secs(1));
        eprintln!(
            "queues  pages {}/{}  raw {}/{}  parsed {}/{}",
            p_rx.len(), p_rx.capacity().unwrap(),
            r_rx.len(), r_rx.capacity().unwrap(),
            w_rx.len(), w_rx.capacity().unwrap(),
        );
        println!("pages in index: {}", stats_value.lock().unwrap());

    });
    
    // Create Downloader Slaves
    let consumers: Vec<_> = (0..downloader_threads)
        .map(|_|{
            let page_rx = page_rx.clone();
            let raw_tx = raw_tx.clone();
            downloader_consumer(page_rx, raw_tx)

        }).collect();
    // Create parser consumers
    let parse_consumers: Vec<_>=(0..parser_threads)
        .map(|_|{
            let raw_rx = raw_rx.clone();
            let parsed_tx = parsed_tx.clone();
            parser_consumer(raw_rx, parsed_tx);
        }).collect();
    let writer_consumer: Vec<_>={0..1}
    .map(|_|{
            let value = Arc::clone(&value);
            let parsed_rx = parsed_rx.clone();
            thread::spawn(move||{ 
                for item in parsed_rx {
                    // Storing everything in database 
                    *value.lock().unwrap() += 1;
                    

                    
                }
            })

        }).collect();
    
    drop(page_rx);
    


    // Create Master
    thread::spawn(move || {
    let mut stmt = index_conn
        .prepare("select warc_filename, warc_record_offset, warc_record_length,url from data")
        .unwrap();

    // MappedRows is an iterator over the statement — no big Vec, no collect()
    let mut rows = stmt.query_map([], |row| {
        Ok(Page {
            file: row.get(0)?,
            offset: row.get(1)?,
            length: row.get(2)?,
            url: row.get(3)?,
        })
    }).unwrap();

    while let Some(page) = rows.next() {
        let page = page.unwrap();
        page_tx.send(page).unwrap(); // <-- backpressure: blocks when buffer full
    }

     drop(page_tx);

});


    // Wait for slaves to finish
    for c in consumers {
        c.join().unwrap();
    }





    
}


fn downloader_consumer(page_rx:crossbeam_channel::Receiver<Page>, raw_tx:crossbeam_channel::Sender<RawPage>)->thread::JoinHandle<()>{
        thread::spawn(move||{
            let client = reqwest::blocking::Client::new();
            
            for item in page_rx {
                // doing work frfr ngl
                let url = format!("https://data.commoncrawl.org/{}", item.file);
                let resp: reqwest::blocking::Response = client.get(url)
                    .header("Range", format!("bytes={}-{}",item.offset,item.offset+item.length-1))
                    .send().unwrap();
                let bytes = resp.bytes().unwrap();
                let body = gunzip_to_text(&bytes).unwrap();                
                let raw_page = RawPage{
                    url:item.url,
                    content:body
                };
                raw_tx.send(raw_page).unwrap()
                
            }
        })
}

fn parser_consumer(raw_rx:crossbeam_channel::Receiver<RawPage>, parsed_tx:crossbeam_channel::Sender<ParsedPage>)->thread::JoinHandle<()>{
    thread::spawn(move||{
        for item in raw_rx{
        let body = item.content;
            
        let body = match body.find("<!DOCTYPE html>") {
            Some(idx) => body[idx + "<!DOCTYPE html>".len()..].to_string(),
            None => body.to_string(), // no doctype → nothing to remove
        };
        let result = extract(&body).unwrap();
        let parsed_page = ParsedPage{
                url:item.url,
                content:result
        };
        parsed_tx.send(parsed_page).unwrap();
            
        }
    })

}



fn downloader(pages:Vec<Page>){

    let client = reqwest::blocking::Client::new();
    for page in pages{
        let url = format!("https://data.commoncrawl.org/{}", page.file);
        let resp: reqwest::blocking::Response = client.get(url)
            .header("Range", format!("bytes={}-{}",page.offset,page.offset+page.length-1))
            .send().unwrap();
        let bytes = resp.bytes().unwrap();
        let body = gunzip_to_text(&bytes).unwrap();
        println!("{}",body);

        
        let body = match body.find("<!DOCTYPE html>") {
            Some(idx) => body[idx + "<!DOCTYPE html>".len()..].to_string(),
            None => body.to_string(), // no doctype → nothing to remove
        };    
        let result = extract(&body).unwrap();
        println!("{}",result.content_text);    
    }
}


fn gunzip_to_text(input: &[u8]) -> std::io::Result<String> {
    let mut decoder = GzDecoder::new(input);   // &[u8] is already a reader
    let mut bytes = Vec::new();
    decoder.read_to_end(&mut bytes)?;
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}
