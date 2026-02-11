use std::thread;
use std::time::Duration;
use cursor_db::cursor::CursorDB;
use cursor_db::cursor::DBStats;
use cursor_db::record::Record;
fn main() -> std::io::Result<()> {
    let mut db: CursorDB = CursorDB::open_or_create("data/data.cdb", "data/index.cdbi")?;

    /*
        for i in 0..1_000_000 {
            let timestamp = 1_000_000_000 + i;
            let payload = format!("payload-{}", i).into_bytes();
            db.append(timestamp, &payload)?;
        }
    */

    thread::sleep(Duration::from_millis(500));

    let stats: DBStats = db.stats()?;
    println!("{}", stats);

    match db.move_cursor_at(0) {
        Some(r) => println!("Encontrado: {}", r.timestamp),
        None => println!("Timestamp fuera de rango"),
    }

    match db.back() {
        Some(r) => println!("Anterior: {}", r.timestamp),
        None => println!("Ya estás en el inicio."),
    }

    match db.current() {
        Some(rec) => println!("Registro actual: {:?}", rec.timestamp),
        None => println!("La base de datos está vacía."),
    }

    match db.next() {
        Some(r) => println!("Siguiente: {}", r.timestamp),
        None => println!("Fin del archivo alcanzado."),
    }

    match db.back() {
        Some(r) => println!("Anterior: {}", r.timestamp),
        None => println!("Ya estás en el inicio."),
    }

    match db.move_cursor_at(1000999999) {
        Some(r) => println!("Encontrado: {}", r.timestamp),
        None => println!("Timestamp fuera de rango"),
    }

    match db.current() {
        Some(rec) => println!("Registro actual: {:?}", rec.timestamp),
        None => println!("La base de datos está vacía."),
    }

    let logs: Vec<Record> = db.range_around_cursor(10, 10);
    match logs.len() {
        0 => println!("No se encontraron registros en el rango especificado."),
        n => println!("Ventana de {} registros obtenida.", n),
    }

    match db.move_cursor_at(0) {
        Some(r) => println!("Encontrado: {}", r.timestamp),
        None => println!("Timestamp fuera de rango"),
    }

    let mut count: i32 = 0;
    while count < 5 {
        match db.next() {
            Some(_record) => match db.current() {
                Some(rec) => println!("Registro actual: {:?}", rec.timestamp),
                None => println!("La base de datos está vacía."),
            },
            None => {
                println!("Llegamos al final");
                break;
            }
        }
        count += 1;
    }

    Ok(())
}
