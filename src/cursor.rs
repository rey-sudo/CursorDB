use crate::record::Record;
use crc32fast::Hasher;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::io::{Error, ErrorKind, Result};
use std::io::{Read, Seek, SeekFrom};

const INDEX_STRIDE: u64 = 1024;
const MAX_PAYLOAD_SIZE: usize = 16 * 1024 * 1024;

/// Represents a pointer in the sparse index of the database.
///
/// Instead of mapping every single record, the sparse index stores "navigational markers"
/// at fixed intervals. This allows the engine to use binary search to jump to a
/// specific neighborhood in the file, significantly reducing linear scan time.
#[derive(Debug, Clone)]
pub struct IndexEntry {
    /// The logical zero-based sequence number of the record 8-bytes.
    ///
    /// Used for row-based navigation and range queries. This acts as a
    /// stable identifier for the record within the append-only stream.
    pub row_number: u64,
    /// High-precision Unix timestamp (Microseconds) 8-bytes.
    ///
    /// Facilitates time-series queries and chronological sorting. Using an `i64`
    /// ensures compatibility with standard time libraries and allows for
    /// signed duration arithmetic.
    pub timestamp: i64,
    /// The physical starting position (8-bytes) of the record within the data file.
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

        let entries = self.index.len();

        let index_interval_bytes = if entries == 0 {
            0
        } else {
            // Aseguramos que file_size sea u64 antes de dividir
            self.last_valid_offset / entries as u64
        };

        let orphan_bytes: u64 = if data_len > self.last_valid_offset {
            data_len - self.last_valid_offset
        } else {
            0
        };

        let orphan_data_ratio: f64 = if data_len > 0 {
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
            index_interval_bytes,
            orphan_data_ratio,
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

        // Open the index .cbi file
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

    /// Loads the sparse index from the `.cdbi` file into memory.
    ///
    /// This function performs the following steps:
    /// 1. Reads the entire index file into a memory buffer for fast sequential processing.
    /// 2. Iteratively parses 24-byte chunks into `IndexEntry` structures.
    /// 3. Updates `self.total_rows` based on the last indexed row to provide a starting point.
    /// 4. Calls `reconcile_total_rows` to sync the index with the data in the main .cdb file.
    fn load_index(&mut self) -> std::io::Result<()> {
        let mut buf: Vec<u8> = Vec::new();

        // Reset file pointer to the beginning to ensure a full read
        self.index_file.seek(SeekFrom::Start(0))?;
        self.index_file.read_to_end(&mut buf)?;

        // Each entry consists of: row_number(8) + timestamp(8) + file_offset(8) = 24 bytes
        let entry_size: usize = 24;
        let mut offset: usize = 0;

        // Process the buffer while there is at least one full entry remaining
        while offset + entry_size <= buf.len() {
            // 1. buf[offset..offset + 8]: Extract a 8-byte "slice" (offsets 0-7) from the buffer starting at the current offset.
            // 2. .try_into(): Attempt to convert the dynamic slice into a fixed-size array [u8; 8].
            //    This is required because the conversion function needs a guaranteed size at compile time.
            // 3. u64::from_le_bytes(...): Interpret those 8 bytes as a 64-bit integer using
            //    Little Endian byte order (the standard for modern CPUs like Intel, AMD, and ARM).
            let row: u64 =
                u64::from_le_bytes(buf[offset..offset + 8].try_into().map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "Error in row_number")
                })?);

            // Extracts 8 bytes from the buffer (offsets 8-15) and safely converts them from Little Endian to a 64-bit signed integer timestamp.
            let ts: i64 =
                i64::from_le_bytes(buf[offset + 8..offset + 16].try_into().map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "Error in timestamp")
                })?);

            // Parse file_offset (Bytes 16-23): Byte position in the .cdb data file
            let file_offset: u64 =
                u64::from_le_bytes(buf[offset + 16..offset + 24].try_into().map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "Error in file_offset")
                })?);

            // Store the entry in the in-memory sparse index
            self.index.push(IndexEntry {
                row_number: row,
                timestamp: ts,
                file_offset,
            });

            // Set total_rows to the next available ID.
            // If the last indexed row ID is N, then the total count of records is N + 1.
            // This ensures the next append() uses the correct subsequent ID.
            self.total_rows = row + 1;

            // Advance buffer pointer
            offset += entry_size;
        }

        // Scan the data file from the last indexed position to find and count
        // any records that were added but haven't been indexed yet.
        self.reconcile_total_rows()
    }

    fn reconcile_total_rows(&mut self) -> std::io::Result<()> {
        let file_len = self.data_file.metadata()?.len();

        // 1. EXTRAER DATOS PRIMERO (Copiamos los valores fuera de self)
        // Usamos .copied() o extraemos los campos manualmente para liberar el préstamo de self.index
        let last_entry = self.index.last().map(|e| (e.row_number, e.file_offset));

        let (mut row_count, mut current_off) = if let Some((row_num, offset)) = last_entry {
            // Si el último registro del índice está corrupto, la apertura fallará con el motivo real.
            let (_, next_off) = self.read_record_at(offset)?;

            (row_num + 1, next_off)
        } else {
            (0, 0)
        };

        // 2. ESCANEO (Ya no hay conflictos de préstamo)
        while current_off < file_len {
            // Ya no necesitamos un 'else' manual para lanzar errores.
            // Si read_record_at encuentra un CRC inválido o un archivo truncado,
            // detendrá el bucle y saldrá de la función devolviendo ese error exacto.
            let (_, next_off) = self.read_record_at(current_off)?;

            current_off = next_off;
            row_count += 1;
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

        self.data_file.flush()?;

        self.total_rows += 1;
        self.last_valid_offset = self.data_file.seek(SeekFrom::Current(0))?;
        Ok(())
    }

    pub fn current(&mut self) -> std::io::Result<Option<Record>> {
        // Si no hay registros, no es un error, simplemente no hay nada que mostrar.
        if self.total_rows == 0 {
            return Ok(None);
        }

        // Ahora el '?' de read_record_at funciona perfectamente porque
        // la función devuelve Result.
        // Si el registro está corrupto, el error sube al usuario.
        let (record, _) = self.read_record_at(self.current_offset)?;

        Ok(Some(record))
    }

    fn read_record_at(&mut self, offset: u64) -> Result<(Record, u64)> {
        // 1. Posicionarnos en el inicio del registro
        self.data_file.seek(SeekFrom::Start(offset))?;

        // 2. Leer el Header completo (16 bytes: 8 ts + 4 size + 4 crc)
        let mut header_buf: [u8; 16] = [0u8; 16];

        if let Err(e) = self.data_file.read_exact(&mut header_buf) {
            return if e.kind() == ErrorKind::UnexpectedEof {
                // Si el archivo se acaba aquí, es un error de datos inválidos (truncado)
                Err(Error::new(ErrorKind::InvalidData, "Header incompleto"))
            } else {
                Err(e)
            };
        }

        let timestamp: i64 = i64::from_le_bytes(header_buf[0..8].try_into().unwrap());
        let size: usize = u32::from_le_bytes(header_buf[8..12].try_into().unwrap()) as usize;
        let stored_checksum: u32 = u32::from_le_bytes(header_buf[12..16].try_into().unwrap());

        let file_len: u64 = self.data_file.metadata()?.len();

        if size > MAX_PAYLOAD_SIZE {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!(
                    "El registro indica un tamaño de {} bytes, lo cual supera el límite de {} bytes",
                    size, MAX_PAYLOAD_SIZE
                ),
            ));
        }

        if (offset + 16 + size as u64) > file_len {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                format!(
                    "Registro en offset {} está truncado: se esperaban {} bytes totales pero el archivo mide {}",
                    offset,
                    16 + size,
                    file_len
                ),
            ));
        }

        // 3. Leer el Payload
        let mut payload = vec![0u8; size];

        self.data_file.read_exact(&mut payload)?;

        // 4. VALIDACIÓN DE INTEGRIDAD (CRC32)
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&payload);
        let computed_checksum = hasher.finalize();

        if computed_checksum != stored_checksum {
            // El error de InvalidData es perfecto aquí porque el archivo existe
            // y se leyó bien, pero el contenido "no es válido".
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!(
                    "Corrupción de datos: El CRC32 calculado ({:08x}) no coincide con el guardado ({:08x}) en el offset {}",
                    computed_checksum, stored_checksum, offset
                ),
            ));
        }

        // 5. Calcular el siguiente offset: Header (16) + Payload (size)
        let next_offset: u64 = offset + 16 + size as u64;

        Ok((Record { timestamp, payload }, next_offset))
    }

    pub fn next(&mut self) -> std::io::Result<Option<Record>> {
        // 1. Verificar si ya llegamos al final lógico
        if self.total_rows == 0 || self.current_row + 1 >= self.total_rows {
            return Ok(None);
        }

        // 2. Calcular dónde empieza el siguiente registro.
        // Primero necesitamos saber dónde termina el actual para saltar al siguiente.
        // Nota: Si quisiéramos ser más eficientes, podríamos guardar 'next_offset' en el struct.
        let (_, next_offset) = self.read_record_at(self.current_offset)?;

        // 3. Leer el registro que sigue
        // El '?' aquí capturará cualquier error de CRC o de lectura física.
        let (record, _new_next_offset) = self.read_record_at(next_offset)?;

        // 4. Actualizar el estado del cursor solo si la lectura fue exitosa
        self.current_row += 1;
        self.current_offset = next_offset;

        Ok(Some(record))
    }

    pub fn back(&mut self) -> std::io::Result<Option<Record>> {
        // Si estamos al inicio, no hay nada atrás
        if self.current_row == 0 || self.total_rows == 0 {
            return Ok(None);
        }

        // BUSQUEDA BINARIA: Encontrar la entrada de índice más cercana que sea < current_row
        let pos = self
            .index
            .partition_point(|e| e.row_number < self.current_row);

        if pos == 0 {
            return Ok(None);
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

    pub fn move_cursor_at(&mut self, ts: i64) -> std::io::Result<Option<Record>> {
        if self.total_rows == 0 {
            return Ok(None);
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
            // El '?' captura fallos de lectura o corrupción en el proceso de búsqueda
            let (record, next_offset) = self.read_record_at(self.current_offset)?;

            // Si encontramos el registro o nos pasamos (lo que significa que el TS exacto no existe)
            if record.timestamp >= ts {
                return Ok(Some(record));
            }

            // Si llegamos al último registro de la base de datos sin encontrar un TS >= ts
            if self.current_row + 1 >= self.total_rows {
                return Ok(None);
            }

            // Avanzamos el cursor interno
            self.current_offset = next_offset;
            self.current_row += 1;
        }
    }

    fn execute_range_scan(&mut self, before: u64, after: u64) -> std::io::Result<Vec<Record>> {
        let start_row = self.current_row.saturating_sub(before);
        let end_row = (self.current_row + after).min(self.total_rows - 1);

        // 1. Localizar punto de inicio en el índice
        let pos = self.index.partition_point(|e| e.row_number <= start_row);
        let (mut row, mut offset) = if pos == 0 {
            (0, 0)
        } else {
            let idx = &self.index[pos - 1];
            (idx.row_number, idx.file_offset)
        };

        let mut results = Vec::with_capacity((before + after + 1) as usize);

        // 2. Escaneo lineal con validación
        while row <= end_row {
            // El '?' detiene el escaneo si hay un error de CRC o lectura
            let (record, next_offset) = self.read_record_at(offset)?;

            if row >= start_row {
                results.push(record);
            }

            offset = next_offset;
            row += 1;
        }

        Ok(results)
    }

    pub fn range_around_cursor(&mut self, before: u64, after: u64) -> std::io::Result<Vec<Record>> {
        if self.total_rows == 0 {
            return Ok(Vec::new());
        }

        // Guardar estado inicial para restaurarlo al final
        let saved_row = self.current_row;
        let saved_offset = self.current_offset;

        // Usamos un patrón de "defer" manual para asegurar la restauración incluso si hay errores
        let result = self.execute_range_scan(before, after);

        // Restaurar siempre el cursor original
        self.current_row = saved_row;
        self.current_offset = saved_offset;

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;

    #[test]
    fn test_total_records_integrity_and_persistence() -> std::io::Result<()> {
        let db_path: &str = "data/test_data.cdb";
        let index_path: &str = "data/test_index.cdbi";

        // Initial cleaning
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(index_path);

        {
            // Creation and writing
            let mut db: CursorDB = CursorDB::open_or_create(db_path, index_path)?;
            db.append(1000, b"data 1")?;
            db.append(2000, b"data 2")?;

            let stats: DBStats = db.stats()?;
            assert_eq!(stats.total_records, 2, "There must be 2 records");
        }

        {
            // Reopening and Validation of Reconciliation
            let mut db: CursorDB = CursorDB::open_or_create(db_path, index_path)?;
            let stats: DBStats = db.stats()?;

            assert_eq!(
                stats.total_records, 2,
                "Persistence: There must still be 2 records after reopening"
            );

            // Proving individual existence.
            let first: Record = db.move_cursor_at(0)?.expect("The first record must exist");

            assert_eq!(first.timestamp, 1000);

            let second: Record = db.next()?.expect("The second record must exist");
            assert_eq!(second.timestamp, 2000);
        }

        // 4. Testing for corruption
        {
            // Open the file manually
            let mut f: File = std::fs::OpenOptions::new().write(true).open(db_path)?;

            // Move the file pointer to byte 20 from the beginning.
            // Since the header is 16 bytes, byte 20 is exactly 4 bytes into the first payload.
            // This targets the data itself rather than the structural metadata.
            f.seek(std::io::SeekFrom::Start(20))?;

            // Overwrite the original byte at that position with the character 'X'.
            // This "bit-flip" simulation ensures that the stored CRC32 checksum
            // will no longer match the recalculated checksum of the modified payload.
            f.write_all(b"X")?;

            let result: std::result::Result<CursorDB, Error> =
                CursorDB::open_or_create(db_path, index_path);

            assert!(
                result.is_err(),
                "The database should not be opened due to checksum corruption"
            );
            println!("The system found a checksum error: {:?}", result.err());
        }

        // Final cleaning
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(index_path);
        Ok(())
    }

    #[test]
    fn test_data_file_size_logic() -> std::io::Result<()> {
        let db_path = "data/test_size.cdb";
        let index_path = "data/test_size_index.cdbi";

        // Limpieza inicial
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(index_path);

        {
            let mut db = CursorDB::open_or_create(db_path, index_path)?;

            // 1. Verificar tamaño inicial (0 bytes)
            let stats = db.stats()?;
            assert_eq!(
                stats.data_file_size_bytes, 0,
                "El archivo nuevo debe pesar 0 bytes"
            );

            // 2. Escribir un registro y calcular tamaño esperado
            // Registro = 16 bytes (Header) + 4 bytes (Payload) = 20 bytes
            let payload = b"test";
            db.append(123456789, payload)?;

            let stats_after_one = db.stats()?;
            let expected_size = 16 + payload.len() as u64;

            assert_eq!(
                stats_after_one.data_file_size_bytes,
                expected_size,
                "El tamaño debe ser Header(16) + Payload({})",
                payload.len()
            );

            // 3. Comparar con el tamaño real del sistema de archivos
            let actual_file_size = std::fs::metadata(db_path)?.len();
            assert_eq!(
                stats_after_one.data_file_size_bytes, actual_file_size,
                "El valor de stats debe coincidir con el tamaño físico en disco"
            );
        }

        {
            // 4. Reapertura: Verificar que al abrir, el tamaño se mantiene
            let mut db = CursorDB::open_or_create(db_path, index_path)?;
            let stats_reopen = db.stats()?;
            let actual_file_size = std::fs::metadata(db_path)?.len();

            assert_eq!(
                stats_reopen.data_file_size_bytes, actual_file_size,
                "Tras reapertura, el tamaño detectado debe ser igual al del disco"
            );

            // 5. Verificación de crecimiento acumulado
            let extra_payload = b"more data"; // 9 bytes
            db.append(123456790, extra_payload)?; // +25 bytes (16 + 9)

            let final_stats = db.stats()?;
            let final_expected = (16 + 4) + (16 + 9); // 45 bytes totales

            assert_eq!(final_stats.data_file_size_bytes, final_expected);
        }

        // Limpieza final
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(index_path);
        Ok(())
    }

    #[test]
    fn test_index_entries_counting_logic() -> std::io::Result<()> {
        let db_path = "data/test_idx.cdb";
        let index_path = "data/test_idx.cdbi";

        // USAMOS LA CONSTANTE REAL
        let stride = INDEX_STRIDE;

        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(index_path);

        {
            let mut db = CursorDB::open_or_create(db_path, index_path)?;

            // 1. Registro 0: Crea la entrada 1
            db.append(1000, b"registro 0")?;
            assert_eq!(db.stats()?.index_entries, 1);

            // 2. Llenar hasta JUSTO ANTES del trigger
            // Si stride es 1024, llenamos hasta el 1023
            for i in 1..stride {
                db.append(1000 + i as i64, b"data")?;
            }

            // Con 1024 registros (0 a 1023), aún solo debe haber 1 entrada
            assert_eq!(
                db.stats()?.index_entries,
                1,
                "Aún no llegamos al registro {}",
                stride
            );

            // 3. Registro 1024: ¡TRIGGER!
            // Aquí total_rows es 1024, por lo que 1024 % 1024 == 0
            db.append(2000, b"registro trigger")?;

            let stats = db.stats()?;
            assert_eq!(
                stats.index_entries, 2,
                "El registro {} debió crear la segunda entrada",
                stride
            );
        }

        {
            // 4. Verificación física (Cada entrada ocupa 24 bytes)
            let actual_file_size = std::fs::metadata(index_path)?.len();
            assert_eq!(actual_file_size, 2 * 24);
        }

        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(index_path);
        Ok(())
    }

    #[test]
    fn test_index_ram_usage_logic() -> std::io::Result<()> {
        let db_path = "data/test_ram.cdb";
        let index_path = "data/test_ram.cdbi";

        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(index_path);

        {
            let mut db = CursorDB::open_or_create(db_path, index_path)?;

            // 1. Estado inicial: El primer registro siempre se indexa
            db.append(1000, b"data")?;
            let stats = db.stats()?;

            // Cada entrada: row(8) + ts(8) + offset(8) = 24 bytes
            let entry_size = std::mem::size_of::<IndexEntry>();
            assert_eq!(
                entry_size, 24,
                "La estructura IndexEntry debería medir 24 bytes"
            );

            // El uso de RAM debe ser al menos (entradas * 24)
            assert!(stats.index_ram_usage_bytes >= 1 * 24);

            // 2. Llenar la DB para forzar múltiples entradas
            // Con INDEX_STRIDE = 1024, insertamos 3000 registros para tener 3 entradas (0, 1024, 2048)
            for i in 1..3000 {
                db.append(1000 + i as i64, b"data")?;
            }

            let stats_multi = db.stats()?;
            let num_entries = stats_multi.index_entries as usize;

            // La lógica correcta de RAM usage debería considerar la CAPACIDAD del Vec,
            // no solo el número de elementos, para ser 100% honesta.
            // Pero el mínimo absoluto es num_entries * 24.
            assert_eq!(num_entries, 3, "Debería haber 3 entradas de índice");
            assert!(stats_multi.index_ram_usage_bytes >= 3 * 24);

            println!(
                "RAM Usage reportado: {} bytes",
                stats_multi.index_ram_usage_bytes
            );
        }

        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(index_path);
        Ok(())
    }

    #[test]
    fn test_average_record_size_logic() -> std::io::Result<()> {
        let db_path = "data/test_avg.cdb";
        let index_path = "data/test_avg.cdbi";

        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(index_path);

        {
            let mut db = CursorDB::open_or_create(db_path, index_path)?;

            // 1. Caso base: Un solo registro
            // Header (16) + Payload (4) = 20 bytes
            let payload1 = b"1234";
            db.append(1000, payload1)?;

            let stats1 = db.stats()?;
            assert_eq!(
                stats1.average_record_size, 20,
                "Con un solo registro de 20 bytes, el promedio debe ser 20"
            );

            // 2. Registros heterogéneos (mezcla de tamaños)
            // Añadimos un registro grande: Header (16) + Payload (84) = 100 bytes
            let payload2 = vec![0u8; 84];
            db.append(2000, &payload2)?;

            let stats2 = db.stats()?;
            // Total bytes = 20 + 100 = 120. Total records = 2.
            // Promedio esperado = 120 / 2 = 60
            assert_eq!(
                stats2.average_record_size, 60,
                "El promedio de 20 y 100 debe ser 60"
            );

            // 3. Verificación de redondeo (División entera)
            // Añadimos un registro de 21 bytes: Header (16) + Payload (5)
            // Total bytes = 120 + 21 = 141. Total records = 3.
            // 141 / 3 = 47
            db.append(3000, b"12345")?;
            let stats3 = db.stats()?;
            assert_eq!(stats3.average_record_size, 47);
        }

        {
            // 4. Persistencia: ¿Sigue siendo correcto tras reabrir?
            let db = CursorDB::open_or_create(db_path, index_path)?;
            let stats_reopen = db.stats()?;

            assert_eq!(stats_reopen.total_records, 3);
            assert_eq!(stats_reopen.average_record_size, 47);

            // Comprobación cruzada manual
            let manual_avg = stats_reopen.data_file_size_bytes / stats_reopen.total_records;
            assert_eq!(stats_reopen.average_record_size, manual_avg);
        }

        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(index_path);
        Ok(())
    }

    #[test]
    fn test_index_interval_bytes_logic() -> std::io::Result<()> {
        let db_path = "data/test_interval.cdb";
        let index_path = "data/test_interval.cdbi";

        // Limpieza inicial
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(index_path);

        {
            let mut db = CursorDB::open_or_create(db_path, index_path)?;

            // 1. Caso base: Primer registro (Fila 0)
            // Header(16 bytes) + Payload "1234"(4 bytes) = 20 bytes totales.
            db.append(1000, b"1234")?;

            let stats1 = db.stats()?;
            println!(
                "DEBUG 1: size={}, entries={}, interval={}",
                stats1.data_file_size_bytes, stats1.index_entries, stats1.index_interval_bytes
            );

            assert_eq!(stats1.index_entries, 1, "Debe haber 1 entrada");
            assert_eq!(
                stats1.data_file_size_bytes, 20,
                "El tamaño debe ser 20 bytes"
            );
            assert_eq!(
                stats1.index_interval_bytes, 20,
                "Intervalo inicial: 20 / 1 = 20"
            );

            // 2. Llenar registros sin disparar nuevo índice
            // Añadimos 9 registros más del mismo tamaño (9 * 20 = 180 bytes extra)
            // Total acumulado: 200 bytes. Entries: sigue siendo 1.
            for i in 1..10 {
                db.append(1000 + i, b"1234")?;
            }

            let stats2 = db.stats()?;
            println!(
                "DEBUG 2: size={}, entries={}, interval={}",
                stats2.data_file_size_bytes, stats2.index_entries, stats2.index_interval_bytes
            );

            assert_eq!(stats2.index_entries, 1);
            assert_eq!(stats2.index_interval_bytes, 200, "Intervalo: 200 / 1 = 200");

            // 3. Cruzar el Stride (INDEX_STRIDE = 1024)
            // Necesitamos llegar al registro 1024 para que se cree la segunda entrada.
            // Ya tenemos 10 registros, faltan 1014 para llegar al 'trigger'
            for i in 10..INDEX_STRIDE {
                db.append(2000 + i as i64, b"1234")?;
            }

            // Este registro (el 1024) dispara la entrada 2
            db.append(3000, b"trigger")?;

            let stats3 = db.stats()?;
            println!(
                "DEBUG 3: size={}, entries={}, interval={}",
                stats3.data_file_size_bytes, stats3.index_entries, stats3.index_interval_bytes
            );

            assert_eq!(
                stats3.index_entries, 2,
                "Debe haber 2 entradas de índice ahora"
            );

            // El intervalo debe ser (Total Bytes / 2 entradas)
            let expected_interval = stats3.data_file_size_bytes / 2;
            assert_eq!(
                stats3.index_interval_bytes, expected_interval,
                "El intervalo debe ser el tamaño total entre las 2 entradas"
            );
        }

        // Limpieza final
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(index_path);
        Ok(())
    }

    #[test]
    fn test_orphan_data_ratio_percentage_logic() -> std::io::Result<()> {
        let db_path = "data/test_orphan_ratio.cdb";
        let index_path = "data/test_orphan_ratio.cdbi";

        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(index_path);

        // 1. Crear DB sana
        {
            let mut db = CursorDB::open_or_create(db_path, index_path)?;
            db.append(1000, vec![0u8; 84].as_slice())?; // 100 bytes totales
            assert_eq!(db.stats()?.orphan_data_ratio, 0.0);
        }

        // 2. Añadir basura (Simulamos crash)
        {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new().append(true).open(db_path)?;
            f.write_all(b"basura_incompleta")?; // 17 bytes de basura
            f.flush()?;
        }

        // 3. Verificación
        // Si tu CursorDB es estricto, esto devolverá Err.
        // Si quieres que stats() funcione, reconcile_total_rows debería atrapar el error
        // y simplemente dejar de contar registros, manteniendo el last_valid_offset.

        let db_res = CursorDB::open_or_create(db_path, index_path);

        match db_res {
            Ok(db) => {
                let stats = db.stats()?;
                assert!(stats.orphan_data_ratio > 0.0);
            }
            Err(e) => {
                let error_msg = e.to_string();
                // Verificamos que el error sea uno de los de integridad
                assert!(
                    error_msg.contains("supera el límite")
                        || error_msg.contains("Header incompleto"),
                    "El error debería ser de integridad, pero fue: {}",
                    error_msg
                );
                println!("Éxito: El sistema bloqueó el archivo corrupto correctamente.");
            }
        }

        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(index_path);
        Ok(())
    }
}
