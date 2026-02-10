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

    // Cursor state (determinista)
    current_row: u64,
    current_offset: u64,

    total_rows: u64,
}

impl CursorDB {
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

        let mut db = Self {
            data_file,
            index_file,
            index: Vec::new(),
            current_row: 0,
            current_offset: 0,
            total_rows: 0,
        };

        // Cargar lo que existe en el índice y luego buscar registros huérfanos
        db.load_index()?;

        // IMPORTANTE: Mover el puntero al final para que los próximos 'append' no sobrescriban
        db.data_file.seek(SeekFrom::End(0))?;
        db.index_file.seek(SeekFrom::End(0))?;

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

    /// Escanea registros que existen en el archivo de datos pero no en el índice.
    fn reconcile_total_rows(&mut self) -> std::io::Result<()> {
        // 1. Copiamos los datos necesarios para liberar el préstamo de 'self.index'
        let last_indexed_info = self.index.last().map(|e| (e.row_number, e.file_offset));

        let mut scan_offset = match last_indexed_info {
            Some((_, offset)) => {
                // Usamos el offset para saltar el registro que YA está en el índice
                if let Some((_, next_off)) = self.read_record_at(offset) {
                    next_off
                } else {
                    offset
                }
            }
            None => 0,
        };

        // 2. Sincronizamos el estado inicial del cursor antes del escaneo
        if let Some((row, offset)) = last_indexed_info {
            self.current_row = row;
            self.current_offset = offset;
            self.total_rows = row + 1;
        } else {
            self.total_rows = 0;
        }

        let file_len = self.data_file.metadata()?.len();

        // 3. Bucle de recuperación de registros huérfanos
        while scan_offset < file_len {
            if let Some((_record, next_offset)) = self.read_record_at(scan_offset) {
                // Ahora sí podemos actualizar el estado
                self.current_offset = scan_offset;
                self.current_row = self.total_rows;
                self.total_rows += 1;

                scan_offset = next_offset;
            } else {
                // Registro truncado o corrupto encontrado
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
        let saved_row: u64 = self.current_row;
        let saved_offset: u64 = self.current_offset;

        let (record, _) = self.read_record_at(self.current_offset)?;

        // restaurar estado (determinismo total)
        self.current_row = saved_row;
        self.current_offset = saved_offset;

        Some(record)
    }

    // --------------------------------------------------
    // LOW-LEVEL RECORD READ
    // --------------------------------------------------
    fn read_record_at(&mut self, offset: u64) -> Option<(Record, u64)> {
        self.data_file.seek(SeekFrom::Start(offset)).ok()?;

        let mut ts_buf = [0u8; 8];
        let mut size_buf = [0u8; 4];

        self.data_file.read_exact(&mut ts_buf).ok()?;
        self.data_file.read_exact(&mut size_buf).ok()?;

        let timestamp = i64::from_le_bytes(ts_buf);
        let size = u32::from_le_bytes(size_buf) as usize;

        let mut payload = vec![0u8; size];
        self.data_file.read_exact(&mut payload).ok()?;

        let next_offset = offset + 8 + 4 + size as u64;

        Some((Record { timestamp, payload }, next_offset))
    }

    pub fn next(&mut self) -> Option<Record> {
        if self.current_row + 1 >= self.total_rows {
            return None;
        }

        let (_, next_offset) = self.read_record_at(self.current_offset)?;

        self.current_row += 1;
        self.current_offset = next_offset;

        let (record, _) = self.read_record_at(self.current_offset)?;
        Some(record)
    }

    pub fn back(&mut self) -> Option<Record> {
        if self.current_row == 0 {
            return None;
        }

        // buscar índice anterior
        let idx = self
            .index
            .iter()
            .rev()
            .find(|e| e.row_number < self.current_row)?;

        let mut row = idx.row_number;
        let mut offset = idx.file_offset;

        while row < self.current_row - 1 {
            let (_, next) = self.read_record_at(offset)?;
            offset = next;
            row += 1;
        }

        self.current_row -= 1;
        self.current_offset = offset;

        let (record, _) = self.read_record_at(self.current_offset)?;
        Some(record)
    }

    // --------------------------------------------------
    // SEEK BY TIMESTAMP
    // --------------------------------------------------
    pub fn move_cursor_at(&mut self, ts: i64) -> Option<Record> {
        let idx = self.index.iter().rev().find(|e| e.timestamp <= ts)?;

        let mut row = idx.row_number;
        let mut offset = idx.file_offset;

        loop {
            let (record, next_offset) = self.read_record_at(offset)?;
            if record.timestamp >= ts {
                self.current_row = row;
                self.current_offset = offset;
                return Some(record);
            }
            offset = next_offset;
            row += 1;
        }
    }

    // --------------------------------------------------
    // RANGE RELATIVO AL CURSOR
    // --------------------------------------------------
    pub fn range_around_cursor(&mut self, before: u64, after: u64) -> Vec<Record> {
        let start = self.current_row.saturating_sub(before);
        let end = (self.current_row + after).min(self.total_rows - 1);

        let mut results = Vec::new();

        // Posicionar cursor temporalmente
        let saved_row = self.current_row;
        let saved_offset = self.current_offset;

        // Seek al inicio
        let idx = self
            .index
            .iter()
            .rev()
            .find(|e| e.row_number <= start)
            .unwrap();

        let mut row = idx.row_number;
        let mut offset = idx.file_offset;

        while row <= end {
            let (record, next_offset) = self.read_record_at(offset).unwrap();
            if row >= start {
                results.push(record);
            }
            offset = next_offset;
            row += 1;
        }

        // Restaurar cursor
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
}
