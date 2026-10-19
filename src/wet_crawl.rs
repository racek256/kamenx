use duckdb::{Connection, params};
use indicatif::{ProgressBar, ProgressStyle};

use flate2::bufread::MultiGzDecoder;
use std::collections::HashSet;
use std::io::{BufRead, BufReader, Write};

use std::fs::{File, OpenOptions, remove_file};
use std::sync::{Arc, Mutex};
use std::thread;
use crossbeam_channel::bounded;

pub struct WetRecord { pub url: String, pub lang: String, pub text: String }


pub fn start_wet_crawl(){
        let crawl:&str = "CC-MAIN-2026-39";
        let table_url:String = format!("https://data.commoncrawl.org/crawl-data/{}/wet.paths.gz", crawl);

        std::fs::create_dir_all("./crawl").unwrap();

        println!("downloading index table");
        let _ = download(&table_url,"./crawl/table.gz");
        let _ = gunzip("./crawl/table.gz","./crawl/table.txt");
        let index_list = load_table("./crawl/table.txt").unwrap();
        let done: HashSet<String> = match File::open("./crawl/done.log") {
                Ok(f) => BufReader::new(f).lines().map(|l| l.unwrap().trim().to_string()).filter(|l| !l.is_empty()).collect(),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => HashSet::new(),
                Err(e) => panic!("failed to read ./crawl/done.log: {e}"),
        };
        let mut skipped = 0usize;
        let download_urls: Vec<String> = index_list.into_iter().filter(|e| {
                let fname = e.split('/').next_back().unwrap_or(e.as_str());
                if done.contains(fname) {
                        skipped += 1;
                        false
                } else {
                        true
                }
        }).map(|e| format!("https://data.commoncrawl.org/{}",e)).collect();
        eprintln!("resume: skipped {skipped} already-done segments ({} remaining)", download_urls.len());
        let (tx, rx) = bounded::<String>(64); 
        let (file_tx, file_rx) = bounded::<String>(64);
        let (parsed_tx, parsed_rx) = bounded::<(String, Vec<WetRecord>)>(64);

        let value = Arc::new(Mutex::new(0i32));
        let stats_value = Arc::clone(&value);


        let (p_rx, r_rx, w_rx) = (rx.clone(), file_rx.clone(), parsed_rx.clone());
        let pb = ProgressBar::new(0);
       pb.set_style(ProgressStyle::default_spinner()          
            .template("{spinner} {msg}")          // returns Result — unwrap here
            .unwrap()
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"));


        thread::spawn(move || loop {
            thread::sleep(std::time::Duration::from_millis(100));
            
            pb.set_message(format!("pages added to dataset: {}", stats_value.lock().unwrap()));
            pb.tick()


        }); 
        

        let _consumers: Vec<_> = (0..2)
        .map(|_|{
            let page_rx = rx.clone();
            let raw_tx = file_tx.clone();
            download_consumer(page_rx, raw_tx)

        }).collect(); 
        let _parse_consumer: Vec<_> = (0..6)
        .map(|_|{
            let parsed_tx = parsed_tx.clone();
            let file_rx = file_rx.clone();
            parser_consumer(file_rx, parsed_tx)
        }).collect();
        let _write_consumer: Vec<_> = (0..1)
         .map(|_|{
            let conn = Connection::open("./crawl/crawl.db").unwrap();
            let parsed_rx = parsed_rx.clone();
            let value = Arc::clone(&value);
            
            writer_consumer(parsed_rx, conn, value);
        }).collect();

        for url in download_urls{
            tx.send(url).unwrap();
        }
}




fn download_consumer(rx:crossbeam_channel::Receiver<String>,tx:crossbeam_channel::Sender<String>)->thread::JoinHandle<()>{
    thread::spawn(move||{
        let client = reqwest::blocking::Client::new();

        for url in rx {
            let filename = &url.split("/").last().unwrap();
            let response = client.get(&url).send().unwrap();
            let bytes = response.bytes().unwrap();
            let mut file = File::create(format!("./crawl/{}",filename)).unwrap();
            let _ = file.write_all(&bytes);
            tx.send(filename.to_string()).unwrap();
        }
    })

}



fn parser_consumer(rx:crossbeam_channel::Receiver<String>,tx:crossbeam_channel::Sender<(String, Vec<WetRecord>)>)->thread::JoinHandle<()>{
    thread::spawn(move||{
        for item in rx{
            let filename = &format!("./crawl/{}",item);

            let file = File::open(filename).unwrap();

            let reader: Box<dyn BufRead> = Box::new(BufReader::new(MultiGzDecoder::new(BufReader::new(file))));
            let parsed = parse_wet(reader).into_iter();

            let filtered = filter_data(parsed);
            tx.send((item, filtered)).unwrap();
            remove_file(filename).unwrap();
            
            
        }
    })
}


use wordnet_lemmatizer::{Lemmatizer, Pos};
static WNL: std::sync::LazyLock<Lemmatizer> =
  std::sync::LazyLock::new(Lemmatizer::embedded);

fn lemma(tok: &str) -> String {
  // lowercase before lookup, else "Dogs" misses
  let lower = tok.to_ascii_lowercase();
  WNL.lemmatize(&lower, Pos::Noun)
}

fn lemmatized_text(text: &str) -> String {
  text.split(|c: char| !c.is_ascii_alphabetic())
    .filter(|t| t.len() >= 2 && t.len() <= 30)
    .map(lemma)
    .collect::<Vec<_>>()
    .join(" ")
}

fn filter_data<I: IntoIterator<Item = WetRecord>>(data: I) -> Vec<WetRecord> {
    data.into_iter()
        .filter(|e| e.lang == "eng" && !e.text.contains("Redirect Notice"))
        .map(|mut e| {
          e.text = lemmatized_text(&e.text);
          e
        })
        .filter(|e| !e.text.is_empty())
        .collect()
}











fn writer_consumer(rx: crossbeam_channel::Receiver<(String, Vec<WetRecord>)>, conn: duckdb::Connection, value: Arc<Mutex<i32>>) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        conn.execute_batch(
            r"CREATE TABLE IF NOT EXISTS wet (url VARCHAR, lang VARCHAR, text VARCHAR);"
        ).unwrap();

        for (filename, batch) in rx {
            let mut app = conn.appender("wet").unwrap();
            for r in &batch {
                app.append_row(params![r.url, r.lang, r.text]).unwrap();
            }
            app.flush().unwrap();
            drop(app);
            // Commit happened via flush + drop; only now mark done.
            {
                let mut log = OpenOptions::new().append(true).create(true).open("./crawl/done.log").unwrap();
                log.write_all(format!("{filename}\n").as_bytes()).unwrap();
                log.sync_all().unwrap();
            }

            *value.lock().unwrap() += batch.len() as i32;
        }
    })
}




// Ai SLOP function i have no idea how to work with large strings like that 
fn parse_wet(mut r: Box<dyn BufRead>) -> Vec<WetRecord> {
    let mut out = Vec::new();
    loop {
        let mut header = String::new();
        // read header block
        let mut headers: Vec<String> = Vec::new();
        loop {
            header.clear();
            if r.read_line(&mut header).unwrap() == 0 { return out; }
            let line = header.trim_end().to_string();
            if line == "WARC/1.0" { headers.clear(); continue; }
            if line.is_empty() { break; }
            headers.push(line);
        }
        let get = |k: &str| headers.iter()
            .find_map(|h| h.strip_prefix(k).map(|v| v.trim().to_string())).unwrap_or_default();
        let typ: String = get("WARC-Type:");
        let len: usize = get("Content-Length:").parse().unwrap_or(0);
        let url = get("WARC-Target-URI:");
        let lang = get("WARC-Identified-Content-Language:");

        let mut body = vec![0u8; len];
        r.read_exact(&mut body).unwrap();
        // consume trailing \r\n\r\n
        let mut trailer = [0u8; 4];
        let _ = r.read(&mut trailer);
        if typ == "conversion" {
            out.push(WetRecord { url, lang, text: (String::from_utf8_lossy(&body).into_owned()).chars().take(500).collect() });
        }
    }
}

fn gunzip(in_path: &str, out_path: &str) -> std::io::Result<()> {
    let file = File::open(in_path)?;
    let mut out = File::create(out_path)?;

    let mut decompressed = MultiGzDecoder::new(BufReader::new(file));
    std::io::copy(&mut decompressed, &mut out)?;
    Ok(())
}


fn download(url: &str, dest: &str) -> Result<(), Box<dyn std::error::Error>>{
    let resp = reqwest::blocking::get(url)?;
    if !resp.status().is_success(){
        return Err(format!("HTTP {}", resp.status()).into())
    }
    let bytes = resp.bytes()?;
    let mut file = File::create(dest)?;
    file.write_all(&bytes)?;
    Ok(())
}   



fn load_table(path:&str)->Result<Vec<String>,std::io::Error>{
    let file = File::open(path)?;
    let reader = BufReader::new(file);

    let lines:Vec<String> = reader.lines().map(|e| e.unwrap()).collect();
    Ok(lines)

}
