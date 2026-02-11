use crate::record::Record;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};

const INDEX_STRIDE: u64 = 1024;

#[derive(Debug, Clone)]
pub struct IndexEntry {
    pub row_number: u64,
    pub timestamp: i64,
    pub file_offset: u64,
}

pub struct CursorDB {
    data_file: File,
    index_file: File,
    index: Vec<IndexEntry>,

    current_row: u64,
    current_offset: u64,

    total_rows: u64,
}

impl CursorDB {
    pub fn current_row(&self) -> u64 {
        self.current_row
    }
    pub fn total_rows(&self) -> u64 {
        self.total_rows
    }
    pub fn index_len(&self) -> usize {
        self.index.len()
    }
    
    pub fn current_offset(&self) -> u64 {
        self.current_offset
    }

    pub fn open_or_create(data_path: &str, index_path: &str) -> std::io::Result<Self> {
        let data_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true) // Usamos write en lugar de append para control total del cursor
            .open(data_path)?;

        let index_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(index_path)?;

        let mut db: CursorDB = Self {
            data_file,
            index_file,
            index: Vec::new(),
            current_row: 0,
            current_offset: 0,
            total_rows: 0,
        };

        // Cargar lo que existe en el índice y luego buscar registros huérfanos
        db.load_index()?;

        Ok(db)
    }

    /// Lee el archivo de índice y puebla la memoria.
    fn load_index(&mut self) -> std::io::Result<()> {
        let mut buf = Vec::new();
        self.index_file.seek(SeekFrom::Start(0))?;
        self.index_file.read_to_end(&mut buf)?;

        let entry_size = 24;
        let mut offset = 0;

        while offset + entry_size <= buf.len() {
            // Conversión segura sin panics si el archivo está truncado
            let row = u64::from_le_bytes(buf[offset..offset + 8].try_into().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "Error en row_number")
            })?);
            let ts = i64::from_le_bytes(buf[offset + 8..offset + 16].try_into().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "Error en timestamp")
            })?);
            let file_offset =
                u64::from_le_bytes(buf[offset + 16..offset + 24].try_into().map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "Error en file_offset")
                })?);

            self.index.push(IndexEntry {
                row_number: row,
                timestamp: ts,
                file_offset,
            });

            self.total_rows = row + 1;
            offset += entry_size;
        }

        // Una vez cargado el índice, conciliamos con los datos reales en el log
        self.reconcile_total_rows()
    }

    fn reconcile_total_rows(&mut self) -> std::io::Result<()> {
        // 1. Extraer info del último registro indexado (si existe)
        let last_indexed_info = self.index.last().map(|e| (e.row_number, e.file_offset));

        // 2. Establecer el estado base
        if let Some((row, offset)) = last_indexed_info {
            self.total_rows = row + 1;
            self.current_row = row; // Por defecto, el cursor es el último indexado
            self.current_offset = offset;
        } else {
            self.total_rows = 0;
            self.current_row = 0;
            self.current_offset = 0;
        }

        // 3. Calcular desde dónde empezar a escanear registros huérfanos
        let mut scan_offset = match last_indexed_info {
            Some((_, offset)) => {
                // Saltamos el registro que YA está en el índice para ver qué hay después
                if let Some((_, next_off)) = self.read_record_at(offset) {
                    next_off
                } else {
                    offset // Si falló la lectura, el archivo termina ahí
                }
            }
            None => 0,
        };

        let file_len = self.data_file.metadata()?.len();

        // 4. Bucle de recuperación de registros no indexados
        while scan_offset < file_len {
            // Intentamos leer el registro potencial en scan_offset
            if let Some((_record, next_offset)) = self.read_record_at(scan_offset) {
                // Actualizamos el cursor al nuevo registro encontrado
                self.current_offset = scan_offset;
                self.current_row = self.total_rows;
                self.total_rows += 1;

                scan_offset = next_offset;
            } else {
                // Registro truncado/corrupto
                break;
            }
        }

        Ok(())
    }

    pub fn append(&mut self, timestamp: i64, payload: &[u8]) -> std::io::Result<()> {
        let offset = self.data_file.seek(SeekFrom::End(0))?;

        // Escribir datos
        self.data_file.write_all(&timestamp.to_le_bytes())?;
        let size = payload.len() as u32;
        self.data_file.write_all(&size.to_le_bytes())?;
        self.data_file.write_all(payload)?;

        if self.total_rows % INDEX_STRIDE == 0 {
            let entry = IndexEntry {
                row_number: self.total_rows,
                timestamp,
                file_offset: offset,
            };

            self.index_file.write_all(&entry.row_number.to_le_bytes())?;
            self.index_file.write_all(&entry.timestamp.to_le_bytes())?;
            self.index_file
                .write_all(&entry.file_offset.to_le_bytes())?;

            // 3. Garantizar que el índice toque el disco
            self.index_file.flush()?;
            self.index.push(entry);
        }

        self.total_rows += 1;
        Ok(())
    }

    pub fn current(&mut self) -> Option<Record> {
        if self.total_rows == 0 {
            return None;
        }
        // read_record_at ya hace el seek interno, así que es seguro.
        let (record, _) = self.read_record_at(self.current_offset)?;
        Some(record)
    }

    fn read_record_at(&mut self, offset: u64) -> Option<(Record, u64)> {
        // Forzamos al archivo a ir a la posición que dicta nuestro estado lógico
        self.data_file.seek(SeekFrom::Start(offset)).ok()?;

        let mut ts_buf = [0u8; 8];
        let mut size_buf = [0u8; 4];

        // Si no hay suficientes bytes para las cabeceras, es EOF o corrupción
        self.data_file.read_exact(&mut ts_buf).ok()?;
        self.data_file.read_exact(&mut size_buf).ok()?;

        let timestamp = i64::from_le_bytes(ts_buf);
        let size = u32::from_le_bytes(size_buf) as usize;

        let mut payload = vec![0u8; size];
        if self.data_file.read_exact(&mut payload).is_err() {
            return None; // Payload incompleto
        }

        let next_offset = offset + 8 + 4 + size as u64;

        Some((Record { timestamp, payload }, next_offset))
    }

    pub fn next(&mut self) -> Option<Record> {
        // Si ya estamos en la última fila o la DB está vacía, no hay siguiente.
        if self.total_rows == 0 || self.current_row + 1 >= self.total_rows {
            return None;
        }

        // 1. Obtener el offset del registro que sigue al actual
        let (_, next_offset) = self.read_record_at(self.current_offset)?;

        // 2. Intentar leer el registro en esa nueva posición
        if let Some((record, _)) = self.read_record_at(next_offset) {
            self.current_row += 1;
            self.current_offset = next_offset;
            Some(record)
        } else {
            None
        }
    }

    pub fn back(&mut self) -> Option<Record> {
        // Si estamos al inicio, no hay nada atrás
        if self.current_row == 0 || self.total_rows == 0 {
            return None;
        }

        // BUSQUEDA BINARIA: Encontrar la entrada de índice más cercana que sea < current_row
        let pos = self
            .index
            .partition_point(|e| e.row_number < self.current_row);
        if pos == 0 {
            return None;
        }
        let idx_entry = &self.index[pos - 1];

        let mut row = idx_entry.row_number;
        let mut offset = idx_entry.file_offset;

        // Escaneo lineal corto (máximo INDEX_STRIDE pasos)
        let target_row = self.current_row - 1;
        while row < target_row {
            let (_, next) = self.read_record_at(offset)?;
            offset = next;
            row += 1;
        }

        self.current_row = target_row;
        self.current_offset = offset;

        self.current()
    }

    pub fn move_cursor_at(&mut self, ts: i64) -> Option<Record> {
        if self.total_rows == 0 {
            return None;
        }

        // Encontrar la última entrada de índice cuyo timestamp sea <= ts
        let pos = self.index.partition_point(|e| e.timestamp <= ts);
        if pos == 0 {
            // Si el TS es menor que el primer índice, empezamos desde el principio
            self.current_row = 0;
            self.current_offset = 0;
        } else {
            let idx = &self.index[pos - 1];
            self.current_row = idx.row_number;
            self.current_offset = idx.file_offset;
        }

        // Escaneo lineal hasta encontrar el timestamp exacto o el siguiente superior
        loop {
            let (record, next_offset) = self.read_record_at(self.current_offset)?;
            if record.timestamp >= ts {
                return Some(record);
            }

            if self.current_row + 1 >= self.total_rows {
                return None; // Llegamos al final sin encontrarlo
            }

            self.current_offset = next_offset;
            self.current_row += 1;
        }
    }

    pub fn range_around_cursor(&mut self, before: u64, after: u64) -> Vec<Record> {
        if self.total_rows == 0 {
            return Vec::new();
        }

        // 1. Calcular límites lógicos
        let start_row = self.current_row.saturating_sub(before);
        let end_row = (self.current_row + after).min(self.total_rows - 1);

        // 2. Guardar estado para restaurarlo (Determinismo)
        let saved_row = self.current_row;
        let saved_offset = self.current_offset;

        // 3. BUSQUEDA BINARIA para encontrar el punto de partida más cercano en el índice
        let pos = self.index.partition_point(|e| e.row_number <= start_row);
        let (mut row, mut offset) = if pos == 0 {
            (0, 0)
        } else {
            let idx = &self.index[pos - 1];
            (idx.row_number, idx.file_offset)
        };

        let mut results = Vec::new();

        // 4. Escaneo lineal optimizado (solo desde el índice más cercano hasta end_row)
        while row <= end_row {
            if let Some((record, next_offset)) = self.read_record_at(offset) {
                if row >= start_row {
                    results.push(record);
                }
                offset = next_offset;
                row += 1;
            } else {
                break;
            }
        }

        // 5. Restaurar cursor original
        self.current_row = saved_row;
        self.current_offset = saved_offset;

        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;

    #[test]
    fn test_open_or_create_persistence_and_recovery() -> std::io::Result<()> {
        let data_path = "test_persistence.db";
        let index_path = "test_persistence.idx";

        // Limpieza previa por si acaso
        let _ = fs::remove_file(data_path);
        let _ = fs::remove_file(index_path);

        // --- ESCENARIO 1: Crear y escribir datos ---
        {
            let mut db = CursorDB::open_or_create(data_path, index_path)?;
            // Escribimos 1 registro. Al ser el primero (row 0), se indexará.
            db.append(100, b"primer registro")?;

            // Forzamos un segundo registro que NO se indexará (porque INDEX_STRIDE es 1024)
            db.append(200, b"segundo registro")?;

            assert_eq!(db.total_rows, 2);
            assert_eq!(db.index.len(), 1);
        } // Aquí la DB se cierra y los archivos se guardan

        // --- ESCENARIO 2: Reabrir y verificar persistencia ---
        {
            let mut db = CursorDB::open_or_create(data_path, index_path)?;

            // Verificamos que cargó el índice (1 entrada)
            assert_eq!(
                db.index.len(),
                1,
                "Debería haber cargado 1 entrada de índice"
            );

            // Verificamos que reconcilió el segundo registro (huérfano)
            assert_eq!(
                db.total_rows, 2,
                "Debería haber recuperado el registro no indexado"
            );

            // Verificamos que el cursor apunta al último registro (el huérfano)
            let last_rec = db.current().expect("Debería existir el registro actual");
            assert_eq!(last_rec.timestamp, 200);
            assert_eq!(last_rec.payload, b"segundo registro");
        }

        // --- ESCENARIO 3: Verificar que el cursor de escritura está al final ---
        {
            let mut db = CursorDB::open_or_create(data_path, index_path)?;
            // Si el puntero de escritura no se movió al final en open_or_create,
            // este append sobrescribiría datos antiguos.
            db.append(300, b"tercer registro")?;

            assert_eq!(db.total_rows, 3);

            // Movemos el cursor al principio para leer todo y verificar integridad
            // (Nota: asumiendo que implementarás un reset_cursor o similar)
            db.move_cursor_at(100);
            assert_eq!(db.current().unwrap().timestamp, 100);
            db.next(); // registro 200
            db.next(); // registro 300
            assert_eq!(db.current().unwrap().timestamp, 300);
        }

        // Limpieza final
        fs::remove_file(data_path)?;
        fs::remove_file(index_path)?;
        Ok(())
    }

    #[test]
    fn test_load_index_and_reconciliation() -> std::io::Result<()> {
        let data_path = "test_data.db";
        let index_path = "test_index.idx";

        // 1. Limpieza inicial
        let _ = fs::remove_file(data_path);
        let _ = fs::remove_file(index_path);

        {
            // 2. Simulamos una escritura manual en los archivos
            // Registro 0 (Indexado)
            let mut d_file = File::create(data_path)?;
            let mut i_file = File::create(index_path)?;

            let ts0: i64 = 1000;
            let payload0 = b"hola";
            d_file.write_all(&ts0.to_le_bytes())?;
            d_file.write_all(&(payload0.len() as u32).to_le_bytes())?;
            d_file.write_all(payload0)?;

            // Escribimos entrada en el índice para el registro 0
            i_file.write_all(&0u64.to_le_bytes())?; // row_number
            i_file.write_all(&ts0.to_le_bytes())?; // timestamp
            i_file.write_all(&0u64.to_le_bytes())?; // file_offset (empieza en 0)

            // Registro 1 (NO INDEXADO - Para probar reconciliación)
            // Offset actual: 8 (ts) + 4 (size) + 4 (hola) = 16
            let ts1: i64 = 2000;
            let payload1 = b"mundo";
            d_file.write_all(&ts1.to_le_bytes())?;
            d_file.write_all(&(payload1.len() as u32).to_le_bytes())?;
            d_file.write_all(payload1)?;

            d_file.flush()?;
            i_file.flush()?;
        }

        // 3. Abrimos la base de datos (esto dispara load_index)
        let mut db = CursorDB::open_or_create(data_path, index_path)?;

        // 4. Verificaciones
        // Debería haber cargado 1 entrada en el índice de memoria
        assert_eq!(db.index.len(), 1, "El índice debería tener 1 entrada");

        // Debería haber reconciliado total_rows a 2
        assert_eq!(
            db.total_rows, 2,
            "Debería haber detectado 2 filas totales tras la reconciliación"
        );

        // El cursor debería estar en la última fila válida (fila 1, offset 16)
        assert_eq!(db.current_row, 1);

        // Verificar que podemos leer el registro no indexado
        let record = db.current().expect("Debería poder leer el registro actual");
        assert_eq!(record.payload, b"mundo");

        // 5. Limpieza final
        fs::remove_file(data_path)?;
        fs::remove_file(index_path)?;
        Ok(())
    }

    #[test]
    fn test_read_immediately_after_opening() -> std::io::Result<()> {
        let data_path = "test_instant_read.db";
        let index_path = "test_instant_read.idx";
        let _ = fs::remove_file(data_path);
        let _ = fs::remove_file(index_path);

        // 1. Crear y cerrar
        {
            let mut db = CursorDB::open_or_create(data_path, index_path)?;
            db.append(12345, b"data")?;
        }

        // 2. Reabrir y leer SIN hacer append
        let mut db = CursorDB::open_or_create(data_path, index_path)?;
        let rec = db
            .current()
            .expect("Debería leer el registro aunque no se haya hecho append en esta sesión");
        assert_eq!(rec.timestamp, 12345);

        let _ = fs::remove_file(data_path);
        let _ = fs::remove_file(index_path);
        Ok(())
    }

    #[test]
    fn test_cursor_navigation_cycle() -> std::io::Result<()> {
        let data_path = "test_cycle.db";
        let index_path = "test_cycle.idx";

        // Limpieza
        let _ = fs::remove_file(data_path).ok();
        let _ = fs::remove_file(index_path).ok();

        {
            let mut db = CursorDB::open_or_create(data_path, index_path)?;

            // Insertamos 3 registros para tener un recorrido claro (0, 1, 2)
            db.append(100, b"data_0")?; // Indexado (si es el primero)
            db.append(200, b"data_1")?; // Huérfano
            db.append(300, b"data_2")?; // Huérfano

            // Forzamos inicio (Fila 0)
            db.current_row = 0;
            db.current_offset = 0;

            // 1. CURRENT (Fila 0)
            let c1 = db.current().expect("Debería leer registro 0");
            assert_eq!(c1.timestamp, 100);
            assert_eq!(db.current_row, 0);

            // 2. NEXT -> Fila 1
            let n1 = db.next().expect("Debería avanzar a fila 1");
            assert_eq!(n1.timestamp, 200);
            assert_eq!(db.current_row, 1);

            // 3. CURRENT (Fila 1) - Verificar que no se movió al leer
            let c2 = db.current().expect("Debería seguir en fila 1");
            assert_eq!(c2.timestamp, 200);
            assert_eq!(db.current_row, 1);

            // 4. BACK -> Fila 0
            // Aquí el motor usará el índice para volver atrás
            let b1 = db.back().expect("Debería volver a fila 0");
            assert_eq!(b1.timestamp, 100);
            assert_eq!(db.current_row, 0);

            // 5. CURRENT (Fila 0) - Verificación final de ciclo
            let c3 = db.current().expect("Debería estar de vuelta en fila 0");
            assert_eq!(c3.timestamp, 100);
            assert_eq!(db.current_row, 0);
        }

        fs::remove_file(data_path)?;
        fs::remove_file(index_path)?;
        Ok(())
    }

    #[test]
    fn test_cursor_next_navigation() -> std::io::Result<()> {
        let data_path = "test_next.db";
        let index_path = "test_next.idx";

        // Limpieza
        let _ = fs::remove_file(data_path);
        let _ = fs::remove_file(index_path);

        {
            let mut db = CursorDB::open_or_create(data_path, index_path)?;

            // Insertamos 3 registros
            db.append(1000, b"rec_0")?; // Este será indexado (row 0)
            db.append(2000, b"rec_1")?; // Huérfano
            db.append(3000, b"rec_2")?; // Huérfano

            // Forzamos el cursor al inicio para probar next()
            // (Si no tienes move_to_start, lo hacemos manualmente para el test)
            db.current_row = 0;
            db.current_offset = 0;

            // --- Prueba 1: Avance de 0 a 1 ---
            let res1 = db.next().expect("Debería avanzar al registro 1");
            assert_eq!(res1.timestamp, 2000);
            assert_eq!(res1.payload, b"rec_1");
            assert_eq!(db.current_row, 1);

            // --- Prueba 2: Avance de 1 a 2 (último) ---
            let res2 = db.next().expect("Debería avanzar al registro 2");
            assert_eq!(res2.timestamp, 3000);
            assert_eq!(res2.payload, b"rec_2");
            assert_eq!(db.current_row, 2);

            // --- Prueba 3: Intento de avanzar más allá del final ---
            let res3 = db.next();
            assert!(res3.is_none(), "Debería devolver None al llegar al final");
            assert_eq!(
                db.current_row, 2,
                "El contador de fila no debería haber aumentado"
            );
        }

        fs::remove_file(data_path)?;
        fs::remove_file(index_path)?;
        Ok(())
    }
}
