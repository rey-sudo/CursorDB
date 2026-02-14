# CursorDB

CursorDB is a lightweight, append-only database engine designed for sequential data streams and time-series records. It utilizes a memory-mapped sparse index to provide fast timestamp lookups and row-based navigation without the overhead of loading the entire dataset into RAM.

## Key Features

- Deterministic Writes: Records are appended with CRC32 integrity validation.
- Sparse Indexing: Maintains a navigational map in RAM every 1024 records (default stride) for logarithmic jumps across the data file.
- Bi-directional Cursor: Efficiently move forward (next()) and backward (back()) through variable-length records.
- Integrity Auditing: Automatically detects orphan data or truncation during startup and provides detailed storage statistics.

## Installation

Add this to your Cargo.toml:

[dependencies]
cursordb = "0.1.0"

## Quick Start

```
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

    let total_records: u64 = 20;

    // Create records
    for i in 0..total_records {
        let timestamp: i64 = 1_000_000_000 + i as i64;
        let payload: Vec<u8> = format!("payload-{}", i).into_bytes();

        match db.insert(timestamp, &payload) {
            Ok(_) => {
                println!("✔ Record Saved: TS {}", timestamp);
            }
            Err(e) => {
                eprintln!("🗙 Insert Error: {}", e);
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
    let after: u64 = total_records;

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

```

## Binary Storage Format (.cdb)

To ensure portability and recovery, each record is stored using a strictly defined binary layout:

| Offset | Size    | Description                       |
| :----- | :------ | :-------------------------------- |
| 0      | 8 bytes | Timestamp (i64, Little Endian)    |
| 8      | 4 bytes | Payload Size (u32, Little Endian) |
| 12     | 4 bytes | CRC32 Checksum (u32)              |
| 16     | N bytes | Raw Payload Data                  |

## Performance & Constraints

- Max Payload: 16 MB per record.
- Write Speed: O(1) sequential appends.
- Search Speed: O(log N) jump via sparse index + O(Stride) linear scan.
- Safety: Uses std::fs::File::flush to ensure data persistence before index updates.

## Statistics and Reporting

You can generate a health report of the database state:

let stats = db.stats()?;
println!("{}", stats);

## License

This project is licensed under the GPL v3 License.
