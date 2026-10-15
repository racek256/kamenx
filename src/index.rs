use std::fs::File;
use reqwest;
use std::io::{BufReader, Write};
use flate2::bufread::GzDecoder;
use std::io::BufRead; 
use indicatif::ProgressBar;
use duckdb::{Connection, params};


pub fn download_index(limit:i8){
    if limit>0{
        println!(" Starting index download with size limit of {limit}GB")
    }else{
        println!("Starting index download without size limit eta: 200GB")
    }
    // Getting Index table
    let crawl:&str = "CC-MAIN-2026-39";
    let table_url:String = format!("https://data.commoncrawl.org/crawl-data/{}/cc-index-table.paths.gz", crawl);

    println!("downloading index table");
    let _ = download(&table_url,"./index/table.gz");
    let _ = gunzip("./index/table.gz","./index/table.txt");
    let index_list = loadTable("./index/table.txt").unwrap();
    let index_urls:Vec<String> = index_list.into_iter().map(|e| format!("https://data.commoncrawl.org/{}",e)).collect();
    let _ = download_process(&index_urls, "./index/");
}

pub fn prepare_db(limit:u64){
        // Get filenames 
    let index_list = loadTable("./index/table.txt").unwrap();
    let index_urls:Vec<String> = index_list.into_iter().map(|e| format!("https://data.commoncrawl.org/{}",e)).collect();

    let mut filenames: Vec<&str> = index_urls
        .iter()
        .filter(|url| url.contains("subset=warc"))
        .map(|url| url.split("/").last().unwrap())
        .collect();
    filenames.truncate(limit as usize);

    insert_db("./index/", filenames)
}


pub fn insert_db(index_path:&str, filenames:Vec<&str>){
    // Create connection to database
    let conn = Connection::open(format!("{}/index.db",index_path)).unwrap();
    conn.execute("PRAGMA memory_limit='16GB'", []).unwrap();
    conn.execute("PRAGMA threads=10", []).unwrap();
    let mut first:bool = true;    

    let mut position = 0;
    let bar = ProgressBar::new(filenames.len() as u64);
    bar.suspend(|| {
        eprintln!();   // blank line → the bar lifts off the bottom edge
    });
    for file in filenames{
        let full_path = format!("{}{}", index_path, file);
        println!("{}",full_path);
        if first{
            let _ = conn.execute("create table data as select url,url_host_name, fetch_status, warc_filename, warc_record_offset, warc_record_length, content_languages from read_parquet(?)  where content_languages like 'eng'",params![full_path]);
            first = false;
            bar.set_position(position+1);
            position += 1

        }else{
            let _ = conn.execute("insert into data select url,url_host_name, fetch_status, warc_filename, warc_record_offset, warc_record_length, content_languages from read_parquet(?) where content_languages like 'eng'",params![full_path]);
            bar.set_position(position+1);
            position +=1


        }
    }


}






fn download_process(index_list:&Vec<String>, path_prefix:&str)-> Result<(), Box<dyn std::error::Error>>{
    let warc_urls: Vec<&str> = index_list
        .iter()
        .filter(|url| url.contains("subset=warc"))
        .map(|e| e.as_str())
        .collect();
    let mut position:u64 = 0;
    let client = reqwest::blocking::Client::new();
    let bar = ProgressBar::new(warc_urls.len() as u64);
    bar.suspend(|| {
        eprintln!();   // blank line → the bar lifts off the bottom edge
    });
    for url in &warc_urls {
            let out = format!("{}{}",path_prefix, url.split("/").last().unwrap());
            let resp: reqwest::blocking::Response = client.get(*url).send().unwrap();
            std::fs::write(&out, resp.bytes()?)?;
            bar.set_position(position+1);
            position += 1;

    }
    Ok(())
}


fn loadTable(path:&str)->Result<Vec<String>,std::io::Error>{
    let file = File::open(path)?;
    let reader = BufReader::new(file);

    let lines:Vec<String> = reader.lines().map(|e| e.unwrap()).collect();
    Ok(lines)

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



fn gunzip(in_path: &str, out_path: &str) -> std::io::Result<()> {
    let file = File::open(in_path)?;
    let mut out = File::create(out_path)?;

    let mut decompressed = GzDecoder::new(BufReader::new(file));
    std::io::copy(&mut decompressed, &mut out)?;
    Ok(())
}
