use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::{Json, Router, extract::{Query, State}, routing::get};
use duckdb::Connection;
use serde::Serialize;
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;

type SharedConn = Arc<Mutex<Connection>>;

const DB_PATH: &str = "./crawl/crawl.db";

#[derive(Serialize)]
struct SearchResult {
    url: String,
    snippet: String,
    score: f64,
}

fn fts_present(conn: &Connection) -> bool {
    let n: i64 = conn
        .query_row(
            "SELECT count(*) FROM information_schema.schemata WHERE schema_name='fts_main_wet'",
            [],
            |row| row.get(0),
        )
        .unwrap_or(0);
    n > 0
}

fn indexed_count() -> i64 {
    std::fs::read_to_string("./crawl/fts_count.log")
        .unwrap_or_default()
        .trim()
        .parse()
        .unwrap_or(-1)
}

fn store_indexed_count(n: i64) {
    let _ = std::fs::write("./crawl/fts_count.log", n.to_string());
}

fn ensure_index_blocking() -> Result<(), String> {
    let conn = Connection::open(DB_PATH).map_err(|e| e.to_string())?;
    conn.execute_batch("CREATE TABLE IF NOT EXISTS wet(url VARCHAR, lang VARCHAR, text VARCHAR)")
        .map_err(|e| e.to_string())?;
    // FTS extension is statically linked in bundled build; LOAD may be a no-op.
    let _ = conn.execute_batch("LOAD fts");
    let rows: i64 = conn
        .query_row("SELECT count(*) FROM wet", [], |row| row.get(0))
        .map_err(|e| e.to_string())?;
    if fts_present(&conn) && indexed_count() == rows {
        println!("Index already precomputed ({rows} rows), starting search engine");
        return Ok(());
    }
    println!("Building FTS index over {rows} rows (overwrite=true), this can take a while...");
    conn.execute_batch(
        "SET memory_limit='24GB'; PRAGMA create_fts_index('wet', 'url', 'text', overwrite=true);",
    )
    .map_err(|e| e.to_string())?;
    store_indexed_count(rows);
    println!("Index generation finished ({rows} rows), starting search engine");
    Ok(())
}

fn search_on_conn(conn: &Connection, q: String, limit: i64) -> Result<Vec<SearchResult>, String> {
    let lit = q.replace('\'', "''");
    let sql = format!(
        "SELECT * FROM (SELECT url, substr(text, 1, 500) AS snippet, fts_main_wet.match_bm25(url, '{lit}') AS score FROM wet) WHERE score IS NOT NULL ORDER BY score DESC LIMIT {limit}"
    );
    let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map([], |row| {
            let url: String = row.get(0)?;
            let snippet: String = row.get(1)?;
            let score: f64 = row.get(2)?;
            Ok(SearchResult { url, snippet, score })
        })
        .map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    for r in rows {
        out.push(r.map_err(|e| e.to_string())?);
    }
    Ok(out)
}

async fn search_handler(
    State(conn): State<SharedConn>,
    Query(params): Query<HashMap<String, String>>,
) -> Json<Vec<SearchResult>> {
    let q = params.get("q").cloned().unwrap_or_default();
    let limit: i64 = params
        .get("limit")
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
    if q.is_empty() {
        return Json(Vec::new());
    }
    match tokio::task::spawn_blocking(move || {
        let guard = conn.lock().map_err(|e| e.to_string())?;
        search_on_conn(&guard, q, limit)
    })
    .await {
        Ok(Ok(rows)) => Json(rows),
        Ok(Err(e)) => {
            eprintln!("search query failed: {e}");
            Json(Vec::new())
        }
        Err(e) => {
            eprintln!("search task failed: {e}");
            Json(Vec::new())
        }
    }
}

pub async fn open_search() {
    if let Err(e) =
        tokio::task::spawn_blocking(ensure_index_blocking).await.unwrap_or(Err("spawn failed".into()))
    {
        eprintln!("failed to ensure search index: {e}");
    }
    let shared: SharedConn = match Connection::open(DB_PATH) {
        Ok(conn) => {
            let _ = conn.execute_batch("LOAD fts");
            let _ = conn.execute_batch("SET scalar_subquery_error_on_multiple_rows=false");
            let _ = conn.execute_batch("SET memory_limit='24GB'");
            Arc::new(Mutex::new(conn))
        }
        Err(e) => {
            eprintln!("failed to open search database: {e}");
            return;
        }
    };
    let app = Router::new()
        .route("/search", get(search_handler))
        .layer(CorsLayer::permissive())
        .with_state(shared);
    let listener = TcpListener::bind("0.0.0.0:3000").await.unwrap();
    println!("search API listening on 0.0.0.0:3000");
    axum::serve(listener, app).await.unwrap();
}
