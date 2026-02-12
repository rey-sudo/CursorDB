use crate::record::Record;
use crc32fast::Hasher;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};

const INDEX_STRIDE: u64 = 1024;
const MAX_PAYLOAD_SIZE: usize = 16 * 1024 * 1024;

/// Represents a pointer in the sparse index of the database.
///
/// Instead of mapping every single record, the sparse index stores "navigational markers"
/// at fixed intervals. This allows the engine to use binary search to jump to a
/// specific neighborhood in the file, significantly reducing linear scan time.
#[derive(Debug, Clone)]
pub struct IndexEntry {
    /// The logical zero-based sequence number of the record.
    ///
    /// Used for row-based navigation and range queries. This acts as a
    /// stable identifier for the record within the append-only stream.
    pub row_number: u64,
    /// High-precision Unix timestamp (Microseconds).
    ///
    /// Facilitates time-series queries and chronological sorting. Using an `i64`
    /// ensures compatibility with standard time libraries and allows for
    /// signed duration arithmetic.
    pub timestamp: i64,
    /// The physical starting position (in bytes) of the record within the data file.
    ///
    /// This offset is used by the disk controller to perform a `seek` operation
    /// directly to the record's header before reading the payload.
    pub file_offset: u64,
}

/// The core engine of the database, managing a seekable cursor over an append-only data stream.
///
/// It coordinates a primary data file and a sparse index to allow efficient
/// O(1) writes and fast O(log n) searches via binary search.
pub struct CursorDB {
    /// The primary handle for the binary data file (.cdb).
    /// Stores the actual record payloads and headers.
    data_file: File,
    /// The persistent handle for the sparse index file (.cdbi).
    /// Stores periodic snapshots of row positions to speed up cold starts.
    index_file: File,
    /// The in-memory sparse index.
    /// Acts as a jump-table for binary searching timestamps or row numbers
    /// without scanning the entire file from disk.    
    index: Vec<IndexEntry>,
    /// The logical position of the cursor (0-based row index).
    /// Represents the ID of the record currently "under" the cursor.
    current_row: u64,
    /// The physical byte offset of the current_row within the data_file.
    /// Synchronized with current_row to ensure immediate access to data.    
    current_offset: u64,
    /// The total count of records successfully identified in the database.
    /// This is the source of truth for bounds checking during iteration.
    total_rows: u64,
    /// The "high-water mark" of the database.
    /// Points to the exact byte where the last valid record ends.
    /// Used by health checks (stats) to detect orphan data or corruption.
    last_valid_offset: u64,
}

#[derive(Debug)]
pub struct DBStats {
    /// Total count of records currently tracked by the cursor logic.
    pub total_records: u64,
    /// Physical size of the .cdb file on disk.
    pub data_file_size_bytes: u64,
    /// Number of checkpoints in the .cdbi file.
    pub index_entries: usize,
    /// Exact memory allocation for the sparse index in RAM.
    pub index_ram_usage_bytes: usize,
    /// Theoretical average size of a record (Total Bytes / Total Records).
    pub average_record_size: u64,
    /// Index Density: Average physical distance (in bytes) between index entries.
    /// Lower values mean faster seeking but higher RAM usage.
    pub index_interval_bytes: u64,
    /// Orphan Ratio: Percentage of the data file that is not yet indexed.
    /// Useful for triggering an automated index flush.
    pub orphan_data_ratio: f64,
}

impl DBStats {
    /// Converts a raw byte count into a human-readable string (e.g., 300.00 GB).
    ///
    /// This function uses the JEDEC binary standard (1024-based) to scale the
    /// value through units: B, KB, MB, GB, and TB.    
    fn format_bytes(bytes: u64) -> String {
        // Define the magnitude labels for the binary system.
        let labels: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];

        // Convert to f64 to maintain precision during successive divisions.
        let mut size: f64 = bytes as f64;
        let mut unit_idx: usize = 0;

        // Iterate while the value is at least one unit of the next magnitude
        // AND we haven't reached the end of our unit labels (to avoid index out of bounds).
        while size >= 1024.0 && unit_idx < labels.len() - 1 {
            size /= 1024.0; // Compound assignment: scale down the value by 1024.
            unit_idx += 1; // Move to the next unit index (e.g., from MB to GB).
        }

        format!("{:.2} {}", size, labels[unit_idx])
    }
}

impl fmt::Display for DBStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "\n🗎 CursorDB Statistics Report")?;
        writeln!(f, "{}", "━".repeat(35))?;
        writeln!(f, "{:<22}: {}", "Total Records", self.total_records)?;
        writeln!(
            f,
            "{:<22}: {}",
            "Data Storage Size",
            Self::format_bytes(self.data_file_size_bytes)
        )?;
        writeln!(f, "{:<22}: {}", "Index Entries", self.index_entries)?;
        writeln!(
            f,
            "{:<22}: {}",
            "Index RAM Usage",
            Self::format_bytes(self.index_ram_usage_bytes as u64)
        )?;
        writeln!(
            f,
            "{:<22}: {}",
            "Avg Record Size",
            Self::format_bytes(self.average_record_size)
        )?;
        writeln!(f, "{}", "━".repeat(35))?;

        if self.orphan_data_ratio > 0.0 {
            writeln!(
                f,
                "⚠️  Warning: {:.5}% of data is unindexed",
                self.orphan_data_ratio
            )?;
        } else {
            writeln!(f, "✔ Index is synchronized")?;
        }

        Ok(())
    }
}

impl CursorDB {
    /// Returns the current logical row index where the cursor is positioned.
    pub fn current_row(&self) -> u64 {
        self.current_row
    }
    /// Returns the total number of records found in the database.
    pub fn total_rows(&self) -> u64 {
        self.total_rows
    }
    /// Returns the number of entries currently stored in the sparse index.
    pub fn index_len(&self) -> usize {
        self.index.len()
    }
    /// Returns the current physical byte offset in the data file.
    pub fn current_offset(&self) -> u64 {
        self.current_offset
    }
    /// Generates a snapshot of the database health and storage metrics.
    pub fn stats(&self) -> std::io::Result<DBStats> {
        let data_len: u64 = self.data_file.metadata()?.len();
        let index_len: usize = self.index.len();

        let last_indexed_offset: u64 = self
            .index
            .last()
            .map(|e: &IndexEntry| e.file_offset)
            .unwrap_or(0);

        let orphan_bytes: u64 = if data_len > self.last_valid_offset {
            data_len - self.last_valid_offset
        } else {
            0
        };

        let orphan_ratio: f64 = if data_len > 0 {
            (orphan_bytes as f64 / data_len as f64) * 100.0
        } else {
            0.0
        };

        Ok(DBStats {
            total_records: self.total_rows,
            data_file_size_bytes: data_len,
            index_entries: index_len,
            index_ram_usage_bytes: self.index.capacity() * std::mem::size_of::<IndexEntry>(),
            average_record_size: if self.total_rows > 0 {
                data_len / self.total_rows
            } else {
                0
            },
            index_interval_bytes: if index_len > 1 {
                last_indexed_offset / (index_len as u64)
            } else {
                0
            },

            orphan_data_ratio: orphan_ratio,
        })
    }

    /// Opens the database files or creates them if they do not exist.
    ///
    /// This constructor initializes the raw data storage and the sparse index tracker.
    /// It assumes a "pure data" format where the record stream begins at the very
    /// first byte of the file (offset 0).
    ///
    /// # Arguments
    /// * `data_path` - Path to the `.cdb` binary data file.
    /// * `index_path` - Path to the `.cdbi` sparse index file.
    ///
    /// # Implementation Details
    /// * Uses `write(true)` instead of `append` to allow granular cursor positioning
    ///   and overwriting if necessary.
    /// * Initializes the cursor at the "Kilometer Zero" (offset 0, row 0).
    /// * Triggers an immediate `load_index()` to reconcile memory state with disk content.
    pub fn open_or_create(data_path: &str, index_path: &str) -> std::io::Result<Self> {
        // Open the primary data container. CRW access is required for cursor-based navigation.
        let data_file: File = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(data_path)?;

        // Open the companion index file.
        let index_file: File = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(index_path)?;

        // Initialize the database instance with a clean state.
        let mut db: CursorDB = Self {
            data_file,
            index_file,
            index: Vec::new(),
            current_row: 0,
            current_offset: 0,
            total_rows: 0,
            last_valid_offset: 0,
        };

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
        let file_len = self.data_file.metadata()?.len();

        // 1. EXTRAER DATOS PRIMERO (Copiamos los valores fuera de self)
        // Usamos .copied() o extraemos los campos manualmente para liberar el préstamo de self.index
        let last_entry = self.index.last().map(|e| (e.row_number, e.file_offset));

        let (mut row_count, mut current_off) = if let Some((row_num, offset)) = last_entry {
            // Si hay índice, necesitamos saber dónde TERMINA ese registro indexado
            // Llamamos a read_record_at FUERA de cualquier cierre sobre self.index
            let (_, next_off) = self.read_record_at(offset).ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Last indexed record truncated",
                )
            })?;

            (row_num + 1, next_off)
        } else {
            (0, 0)
        };

        // 2. ESCANEO (Ya no hay conflictos de préstamo)
        while current_off < file_len {
            if let Some((_, next_off)) = self.read_record_at(current_off) {
                current_off = next_off;
                row_count += 1;
            } else {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "Integrity violation at offset {}. Partial record found.",
                        current_off
                    ),
                ));
            }
        }

        // 3. ACTUALIZAR ESTADO
        self.total_rows = row_count;
        self.last_valid_offset = current_off;
        self.current_row = 0;
        self.current_offset = 0;

        Ok(())
    }

    pub fn append(&mut self, timestamp: i64, payload: &[u8]) -> std::io::Result<()> {
        if payload.len() > MAX_PAYLOAD_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Payload size {} exceeds the maximum allowed ({} bytes)",
                    payload.len(),
                    MAX_PAYLOAD_SIZE
                ),
            ));
        }

        let offset = self.data_file.seek(SeekFrom::End(0))?;

        let mut hasher: Hasher = Hasher::new();
        hasher.update(payload);
        let checksum: u32 = hasher.finalize();

        // Escribir datos
        self.data_file.write_all(&timestamp.to_le_bytes())?;
        let size = payload.len() as u32;
        self.data_file.write_all(&size.to_le_bytes())?;
        self.data_file.write_all(&checksum.to_le_bytes())?;
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

        self.last_valid_offset = self.data_file.seek(SeekFrom::Current(0))?;
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
        // 1. Posicionarnos en el inicio del registro
        self.data_file.seek(SeekFrom::Start(offset)).ok()?;

        // 2. Leer el Header completo (16 bytes: 8 ts + 4 size + 4 crc)
        let mut header_buf = [0u8; 16];
        if self.data_file.read_exact(&mut header_buf).is_err() {
            return None; // No hay suficientes bytes para un header completo
        }

        let timestamp = i64::from_le_bytes(header_buf[0..8].try_into().unwrap());
        let size = u32::from_le_bytes(header_buf[8..12].try_into().unwrap()) as usize;
        let stored_checksum = u32::from_le_bytes(header_buf[12..16].try_into().unwrap());

        let file_len: u64 = self.data_file.metadata().ok()?.len();
        if size > MAX_PAYLOAD_SIZE || (offset + 16 + size as u64) > file_len {
            eprintln!(
                "ERROR: Registro en offset {} excede límites de tamaño o está truncado.",
                offset
            );
            return None;
        }

        // 3. Leer el Payload
        let mut payload = vec![0u8; size];
        if self.data_file.read_exact(&mut payload).is_err() {
            return None; // Payload incompleto o archivo truncado
        }

        // 4. VALIDACIÓN DE INTEGRIDAD (CRC32)
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&payload);
        let computed_checksum = hasher.finalize();

        if computed_checksum != stored_checksum {
            // Corrupción de datos detectada: el contenido no coincide con su firma
            return None;
        }

        // 5. Calcular el siguiente offset: Header (16) + Payload (size)
        let next_offset = offset + 16 + size as u64;

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
    fn test_total_records_integrity_and_persistence() {
        let db_path = "data/test_data.cdb";
        let index_path = "data/test_index.cdbi";

        // Limpieza inicial para asegurar un test limpio
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(index_path);

        {
            // 1. Usamos tu método guardado en memoria
            let mut db = CursorDB::open_or_create(db_path, index_path).unwrap();
            db.append(1000, b"data 1").unwrap();
            db.append(2000, b"data 2").unwrap();

            let stats = db.stats().unwrap();
            assert_eq!(stats.total_records, 2, "Debe haber 2 registros");
        } // Flush automático al salir del scope

        {
            // 2. Reapertura: Aquí es donde reconcile_total_rows hace su magia
            let db = CursorDB::open_or_create(db_path, index_path).unwrap();
            let stats = db.stats().unwrap();

            assert_eq!(
                stats.total_records, 2,
                "Persistencia: Debe seguir habiendo 2 registros"
            );
        }

        // Limpieza final
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(index_path);
    }
}
