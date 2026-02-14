use cursor_db::cursor::CursorDB;
use cursor_db::cursor::DBStats;
use std::fs;
use std::path::Path;

fn main() -> std::io::Result<()> {
    // Define file paths
    let data_path: &str = "../data/data.cdb";
    let index_path: &str = "../data/index.cdbi";

    for path in &[data_path, index_path] {
        if Path::new(path).exists() {
            fs::remove_file(path)?;
            println!("Deleted previous file: {}", path);
        }
    }

    //=========================================================================

    // Create database
    let mut db: CursorDB = CursorDB::open_or_create(data_path, index_path)?;

    let total_records: i64 = 20;

    // Create records
    for i in 0..total_records {
        let timestamp: i64 = 1_000_000_000 + i;
        let payload: Vec<u8> = format!("payload-{}", i).into_bytes();

        match db.exists(timestamp) {
            Ok(true) => {
                println!("🗙 Already exists: {}", timestamp);
            }
            Ok(false) => {
                db.append(timestamp, &payload)?;
                println!("✔ New Record Saved: {}", timestamp);
            }
            Err(e) => {
                eprintln!("🗙 Error checking existence: {}", e);
            }
        }
    }

    //Display stats
    let stats: DBStats = db.stats()?;
    println!("{}", stats);

    //=========================================================================

    match db.current() {
        Ok(Some(rec)) => {
            println!("Current position: {:?}", rec.timestamp);
        }
        Ok(None) => {
            println!("Current position does not exist");
        }
        Err(e) => {
            eprintln!("Current error: {}", e);
        }
    }

    //=========================================================================

    let ts_0: i64 = 3;

    match db.move_cursor_at(ts_0) {
        Ok(Some(r)) => {
            println!("Position found: {}", r.timestamp);
        }

        Ok(None) => {
            println!("Position not found: {}", ts_0);
        }

        Err(e) => {
            eprintln!("Position not found error: {} {}", ts_0, e);
        }
    }

    //=========================================================================

    match db.move_to_first() {
        Ok(Some(record)) => {
            println!("Moved to first: {}", record.timestamp);
        }

        Ok(None) => {
            println!("There is no first position");
        }

        Err(e) => {
            eprintln!("Moved to first error: {}", e);
        }
    }

    //=========================================================================

    match db.back() {
        Ok(Some(record)) => {
            println!("Moved to back: {}", record.timestamp);
        }

        Ok(None) => {
            println!("There is no position back");
        }

        Err(e) => {
            eprintln!("Back error: {}", e);
        }
    }

    //=========================================================================

    match db.next() {
        Ok(Some(_r)) => println!("Moved to next"),
        Ok(None) => println!("No more records following"),

        Err(e) => eprintln!("Moved to next error: {}", e),
    }

    //=========================================================================

    let before: u64 = 0;
    let after: u64 = total_records as u64;

    match db.range_around_cursor(before, after) {
        Ok(records) => {
            if records.is_empty() {
                println!("Empty range");
            } else {
                println!(
                    "Range around cursor: {}..{} {}",
                    before,
                    after,
                    records.len()
                );

                for record in records {
                    println!("  Timestamp: {}", record.timestamp,);
                }
            }
        }
        Err(e) => eprintln!("Range error: {}", e),
    }

    //=========================================================================

    match db.move_to_last() {
        Ok(Some(record)) => {
            println!("Moved to last: {}", record.timestamp);
        }

        Ok(None) => {
            println!("There is no last record");
        }

        Err(e) => {
            eprintln!("Moved to last error: {}", e);
        }
    }

    //=========================================================================
    Ok(())
}
