use cursor_db::cursor::CursorDB;
use cursor_db::cursor::DBStats;
use std::fs;
use std::path::Path;
use std::thread;
use std::time::Duration;

fn main() -> std::io::Result<()> {
    // Definimos las rutas
    let data_path: &str = "data/data.cdb";
    let index_path: &str = "data/index.cdbi";

    // 1. BORRAR ARCHIVOS PREVIOS (Clean Start)
    // Usamos un pequeño closure para ignorar el error si el archivo no existe
    for path in &[data_path, index_path] {
        if Path::new(path).exists() {
            fs::remove_file(path)?;
            println!("Deleted old file: {}", path);
        }
    }

    // Asegurarse de que el directorio existe
    if let Some(parent) = Path::new(data_path).parent() {
        fs::create_dir_all(parent)?;
    }

    //==================================================================
    let mut db: CursorDB = CursorDB::open_or_create("data/data.cdb", "data/index.cdbi")?;

    for i in 0..20 {
        let timestamp = 1_000_000_000 + i;
        let payload = format!("payload-{}", i).into_bytes();
        db.append(timestamp, &payload)?;
    }

    thread::sleep(Duration::from_millis(500));

    let stats: DBStats = db.stats()?;
    println!("{}", stats);

    match db.move_cursor_at(0)? {
        // El '?' maneja el error de disco/CRC
        Some(r) => println!("Encontrado: {}", r.timestamp),
        None => {
            println!("Timestamp fuera de rango moviendo a first");
            db.move_to_first()?;
        }
    }

    //=========================================================================

    if let Some(r) = db.back()? {
        println!("Anterior: {}", r.timestamp);
    } else {
        println!("Ya estás en el inicio.");
    }

    if let Some(rec) = db.current()? {
        println!("Registro actual: {:?}", rec.timestamp);
    }

    if let Some(r) = db.next()? {
        println!("Siguiente: {}", r.timestamp);
    }

    if let Some(rec) = db.current()? {
        println!("Registro actual: {:?}", rec.timestamp);
    }

    //=========================================================================
    let nuevo_ts: i64 = 1715600000;
    let payload: &[u8; 13] = b"Informacion A";

    // db.append(nuevo_ts, payload)?;

    if db.exists(nuevo_ts)? {
        println!(
            "⚠️ El registro con TS {} ya está guardado. Saltando escritura.",
            nuevo_ts
        );
    } else {
        println!("✅ Registro nuevo. Procediendo a guardar...");
        db.append(nuevo_ts, payload)?;
    }
    //=========================================================================
    let logs = db.range_around_cursor(10, 10)?;
    if logs.is_empty() {
        println!("No se encontraron registros en el rango.");
    } else {
        println!("Ventana de {} registros obtenida.", logs.len());
    }

    //=========================================================================

    if let Some(rec) = db.move_to_first()? {
        println!("Moviendo first: {:?}", rec.timestamp);
    }

    let mut count: i32 = 0;

    // Este bucle se detiene si llegamos al final (Ok(None))
    // pero lanza error si hay corrupción (Err)
    while count < 5 {
        if let Some(record) = db.next()? {
            println!("Iteración {}: Timestamp {}", count, record.timestamp);
            count += 1;
        } else {
            println!("Llegamos al final prematuramente");
            break;
        }
    }

    if let Some(rec) = db.current()? {
        println!("Registro actual: {:?}", rec.timestamp);
    }

    match db.move_to_last()? {
        Some(record) => println!("Moved to last:   {}", record.timestamp),
        None => println!("Database empty"),
    }

    if let Some(rec) = db.current()? {
        println!("Registro actual: {:?}", rec.timestamp);
    }

    Ok(())
}
