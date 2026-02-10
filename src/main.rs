use cursor_db::cursor::CursorDB;
use cursor_db::record::Record;

fn main() -> std::io::Result<()> {
    let mut db: CursorDB = CursorDB::open_or_create("data/data.bin", "data/index.bin")?;

    println!("Appending records...");
    for i in 0..100 {
        let timestamp = 1_700_000_000 + i;
        let payload = format!("payload-{}", i).into_bytes();
        db.append(timestamp, &payload)?;
    }

    let cur: Record = db.current().unwrap();
    println!("Current={}", cur.timestamp);

    let next: Record = db.next().unwrap();
    println!("Next={}", next.timestamp);

    let current: Record = db.current().unwrap();
    println!("Current={}", current.timestamp);

    let back: Record = db.back().unwrap();
    println!("Back={}", back.timestamp);

    let current: Record = db.current().unwrap();
    println!("Current={}", current.timestamp);

    let moved: Record = db.move_cursor_at(1_700_000_040).unwrap();
    println!("Moved={}", moved.timestamp);

    let current: Record = db.current().unwrap();
    println!("Current={}", current.timestamp);

    Ok(())
}
