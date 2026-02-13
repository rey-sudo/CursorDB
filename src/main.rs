use cursor_db::cursor::CursorDB;
use cursor_db::cursor::DBStats;
use std::fs;
use std::path::Path;
use std::thread;
use std::time::Duration;

fn main() -> std::io::Result<()> {
    // Definimos las rutas
    let data_path = "data/data.cdb";
    let index_path = "data/index.cdbi";

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
        None => println!("Timestamp fuera de rango"),
    }

    // Ejemplo usando if let para un flujo más limpio
    if let Some(r) = db.back()? {
        println!("Anterior: {}", r.timestamp);
    } else {
        println!("Ya estás en el inicio.");
    }

    // Actualizamos el acceso al registro actual
    if let Some(rec) = db.current()? {
        println!("Registro actual: {:?}", rec.timestamp);
    }

    // --- NAVEGACIÓN ---
    if let Some(r) = db.next()? {
        println!("Siguiente: {}", r.timestamp);
    }

    // --- RANGOS ---
    // range_around_cursor ahora devuelve Result<Vec<Record>>
    let logs = db.range_around_cursor(10, 10)?;
    if logs.is_empty() {
        println!("No se encontraron registros en el rango.");
    } else {
        println!("Ventana de {} registros obtenida.", logs.len());
    }

    // --- BUCLE DE ITERACIÓN SEGURO ---
    println!("--- Iniciando iteración de prueba ---");
    db.move_cursor_at(0)?;
    let mut count = 0;

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
        Some(record) => {
            println!("Moved to last:   {}", record.timestamp);
        }
        None => println!("The database is empty."),
    }

    if let Some(rec) = db.current()? {
        println!("Registro actual: {:?}", rec.timestamp);
    }

    Ok(())
}
