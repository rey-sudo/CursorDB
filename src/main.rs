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

    let cur_again: Record = db.current().unwrap();
    println!("Current={}", cur_again.timestamp);

    let back: Record = db.back().unwrap();
    println!("Back={}", back.timestamp);

    let cur_final: Record = db.current().unwrap();
    println!("Current={}", cur_final.timestamp);


    /*
        // 3️⃣ Mover cursor a un timestamp específico
        println!("\nMove cursor at timestamp 1700000040");
        if let Some(record) = db.move_cursor_at(1_700_000_040) {
            println!("Cursor positioned at ts={} payload={:?}",
                record.timestamp,
                String::from_utf8_lossy(&record.payload)
            );
        }




        // 5️⃣ Back
        println!("\nBack()");
        if let Some(record) = db.back() {
            println!("Back ts={} payload={:?}",
                record.timestamp,
                String::from_utf8_lossy(&record.payload)
            );
        }

        // 6️⃣ Range relativo al cursor
        println!("\nRange around cursor (5 before, 3 after)");
        let range = db.range_around_cursor(5, 3);
        for r in range {
            println!(
                "ts={} payload={:?}",
                r.timestamp,
                String::from_utf8_lossy(&r.payload)
            );
        }
    */
    Ok(())
}
