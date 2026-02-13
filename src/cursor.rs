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

    data_path: String,
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

#[derive(Debug)]
pub struct AuditReport {
    pub total_records_found: u64,
    pub total_bytes_processed: u64,
    pub index_validation_count: usize,
    pub status: String,
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
    #[cfg(test)]
    pub fn clear_index_in_memory(&mut self) {
        self.index.clear();
    }

    fn get_file_size(&self) -> std::io::Result<u64> {
        Ok(std::fs::metadata(&self.data_path)?.len())
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
            data_path: data_path.to_string(),
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
        let file_len: u64 = self.data_file.metadata()?.len();

        // 1. EXTRAER DATOS PRIMERO (Copiamos los valores fuera de self)
        // Usamos .copied() o extraemos los campos manualmente para liberar el préstamo de self.index
        let last_entry: Option<(u64, u64)> = self
            .index
            .last()
            .map(|e: &IndexEntry| (e.row_number, e.file_offset));

        let (mut row_count, mut current_off, mut last_start_offset) = match last_entry {
            Some((row_num, offset)) => {
                // Validamos que el registro apuntado por el índice sea legible
                let (_, next_off) = self.read_record_at(offset)?;
                (row_num + 1, next_off, offset)
            }
            None => (0, 0, 0),
        };

        // 2. ESCANEO (Ya no hay conflictos de préstamo)
        while current_off < file_len {
            last_start_offset = current_off;

            // Ya no necesitamos un 'else' manual para lanzar errores.
            // Si read_record_at encuentra un CRC inválido o un archivo truncado,
            // detendrá el bucle y saldrá de la función devolviendo ese error exacto.
            let (_, next_off) = self.read_record_at(current_off)?;

            current_off = next_off;
            row_count += 1;
        }

        if current_off != file_len {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "Corruption detected: File has {} bytes of trailing garbage/incomplete data.",
                    file_len - current_off
                ),
            ));
        }

        // 3. ACTUALIZAR ESTADO
        self.total_rows = row_count;
        self.last_valid_offset = last_start_offset;
        self.current_row = 0;
        self.current_offset = 0;

        Ok(())
    }

    pub fn append(&mut self, timestamp: i64, payload: &[u8]) -> std::io::Result<()> {
        // 1. CAPTURAR ESTADO INICIAL PARA POSIBLE ROLLBACK
        // Guardamos las posiciones actuales para poder truncar si algo falla
        let initial_data_size = self.data_file.seek(SeekFrom::End(0))?;
        let initial_index_size = self.index_file.seek(SeekFrom::End(0))?;
        let initial_index_len = self.index.len();

        // 2. EJECUTAR ESCRITURA CON MECANISMO DE ERROR
        // Usamos una clausura interna para capturar cualquier error de I/O
        let write_result = (|| -> std::io::Result<()> {
            if payload.len() > MAX_PAYLOAD_SIZE {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("Payload size {} exceeds limit", payload.len()),
                ));
            }

            let mut hasher = Hasher::new();
            hasher.update(payload);
            let checksum = hasher.finalize();

            // Escribir en Data File
            self.data_file.write_all(&timestamp.to_le_bytes())?;
            let size = payload.len() as u32;
            self.data_file.write_all(&size.to_le_bytes())?;
            self.data_file.write_all(&checksum.to_le_bytes())?;
            self.data_file.write_all(payload)?;
            self.data_file.flush()?; // Asegurar datos en disco

            // Lógica de Índice
            if self.total_rows % INDEX_STRIDE == 0 {
                let entry = IndexEntry {
                    row_number: self.total_rows,
                    timestamp,
                    file_offset: initial_data_size,
                };

                self.index_file.write_all(&entry.row_number.to_le_bytes())?;
                self.index_file.write_all(&entry.timestamp.to_le_bytes())?;
                self.index_file
                    .write_all(&entry.file_offset.to_le_bytes())?;
                self.index_file.flush()?;
                self.index.push(entry);
            }
            Ok(())
        })();

        // 3. GESTIÓN DE RESULTADO (COMMIT O ROLLBACK)
        match write_result {
            Ok(_) => {
                // COMMIT: La escritura fue exitosa, actualizamos estado en RAM
                self.last_valid_offset = initial_data_size;

                self.total_rows += 1;
                self.current_row = self.total_rows - 1;
                self.current_offset = initial_data_size;

                println!("DEBUG: initial_data_size {}", initial_data_size);
                Ok(())
            }
            Err(e) => {
                // ROLLBACK: Algo falló, limpiamos los archivos para evitar corrupción

                // Truncar archivo de datos al tamaño original
                self.data_file.set_len(initial_data_size)?;
                self.data_file.seek(SeekFrom::Start(initial_data_size))?;

                // Truncar archivo de índice al tamaño original
                self.index_file.set_len(initial_index_size)?;
                self.index_file.seek(SeekFrom::Start(initial_index_size))?;

                // Revertir el vector de índice en RAM si se alcanzó a pushear
                if self.index.len() > initial_index_len {
                    self.index.pop();
                }

                Err(e) // Devolvemos el error original para que el usuario sepa que falló
            }
        }
    }

    pub fn current(&mut self) -> std::io::Result<Option<Record>> {
        // Si no hay registros o el cursor se pasó del final, devolvemos None sin leer el disco
        if self.total_rows == 0 || self.current_row >= self.total_rows {
            return Ok(None);
        }

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
        // 1. Si ya procesamos todas las filas, no hay nada más que leer
        // Nota: Si current_row es 0 y total_rows es 1, aún debemos leer la fila 0.
        if self.total_rows == 0 || self.current_row >= self.total_rows {
            return Ok(None);
        }

        // 2. Leer el registro donde está parado el cursor actualmente
        let (record, next_offset) = self.read_record_at(self.current_offset)?;

        // 3. Avanzar el cursor para la PRÓXIMA llamada
        self.current_row += 1;
        self.current_offset = next_offset;

        Ok(Some(record))
    }

    pub fn back(&mut self) -> std::io::Result<Option<Record>> {
        // 1. Validación de límites: Si estamos en la fila 0, no hay nada atrás.
        if self.current_row == 0 || self.total_rows == 0 {
            return Ok(None);
        }

        // El objetivo es posicionarnos en la fila inmediatamente anterior.
        let target_row = self.current_row - 1;

        // 2. Localizar el mejor punto de partida usando el índice.
        // Buscamos la entrada más cercana que sea menor o igual al target.
        let pos = self.index.partition_point(|e| e.row_number <= target_row);

        let (mut row, mut offset) = if pos == 0 {
            // CORRECCIÓN CLAVE: Si no hay índice previo, empezamos desde el inicio (fila 0, offset 0)
            // en lugar de retornar None.
            (0, 0)
        } else {
            // Tomamos la entrada de índice inmediatamente anterior o igual al target.
            let idx_entry = &self.index[pos - 1];
            (idx_entry.row_number, idx_entry.file_offset)
        };

        // 3. Escaneo lineal hacia adelante hasta alcanzar la fila objetivo.
        // Esto es necesario porque en archivos de tamaño variable no se puede leer hacia atrás físicamente.
        while row < target_row {
            // Usamos read_record_at para obtener el offset del siguiente registro.
            let (_, next_offset) = self.read_record_at(offset)?;
            offset = next_offset;
            row += 1;
        }

        // 4. Sincronizamos el cursor con la nueva posición.
        self.current_row = target_row;
        self.current_offset = offset;

        // 5. Devolvemos el registro actual (el que acabamos de alcanzar).
        self.current()
    }

    /// Positions the cursor at the very first record and returns it.
    pub fn move_to_first(&mut self) -> std::io::Result<Option<Record>> {
        // Safety check: Si no hay registros, no hay nada que leer.
        if self.total_rows == 0 {
            return Ok(None);
        }

        // En este motor, el kilómetro cero es el offset 0.
        let target_offset: u64 = 0;

        // Leemos el registro en la posición inicial.
        // next_off nos servirá para dejar el cursor listo para el siguiente next().
        let (record, _) = self.read_record_at(target_offset)?;

        // Actualizamos el estado interno al primer registro.
        self.current_row = 0;
        self.current_offset = target_offset;

        println!(
            "DEBUG: Moviendo a row {} en offset {}",
            self.current_row, self.current_offset
        );

        Ok(Some(record))
    }

    /// Positions the cursor at the last valid record and returns it.
    pub fn move_to_last(&mut self) -> std::io::Result<Option<Record>> {
        // Safety check: Return early if the database contains no records.
        if self.total_rows == 0 {
            return Ok(None);
        }

        // Use the pre-calculated offset of the last successful entry.
        // This value is maintained during load, reconcile, and append operations.
        let target_offset: u64 = self.last_valid_offset;

        // Read the record from the data file.
        // We ignore the second return value (next_offset) as we are already at the end.
        let (record, _) = self.read_record_at(target_offset)?;

        // 4. Update the internal cursor state.
        // Since the Record struct doesn't store the ID, we derive it from total_rows.
        self.current_offset = self.last_valid_offset;
        self.current_row = self.total_rows - 1;

        println!(
            "DEBUG: Moviendo a row {} en offset {}",
            self.current_row, self.current_offset
        );

        Ok(Some(record))
    }

    pub fn move_cursor_at(&mut self, ts: i64) -> std::io::Result<Option<Record>> {
        if self.total_rows == 0 {
            return Ok(None);
        }

        // 1. Salto rápido: Encontrar la última entrada de índice cuyo timestamp sea <= ts
        let pos = self.index.partition_point(|e| e.timestamp <= ts);
        if pos == 0 {
            self.current_row = 0;
            self.current_offset = 0;
        } else {
            let idx = &self.index[pos - 1];
            self.current_row = idx.row_number;
            self.current_offset = idx.file_offset;
        }

        // 2. Escaneo lineal estricto
        loop {
            let (record, next_offset) = self.read_record_at(self.current_offset)?;

            // COINCIDENCIA EXACTA: Actualizamos estado y devolvemos el registro
            if record.timestamp == ts {
                return Ok(Some(record));
            }

            // CASO A: Nos pasamos del timestamp (no existe en el archivo ordenado)
            if record.timestamp > ts {
                return Ok(None);
            }

            // CASO B: Llegamos al último registro sin encontrar el timestamp
            if self.current_row + 1 >= self.total_rows {
                self.current_row = self.total_rows;
                self.current_offset = next_offset; // Este ya es el final físico (EOF)
                return Ok(None);
            }

            // Avanzamos el cursor interno para la siguiente iteración
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

    pub fn verify_deterministic_integrity(&mut self) -> std::io::Result<AuditReport> {
        let file_len = self.data_file.metadata()?.len();
        let mut current_pos: u64 = 0;
        let mut row_counter: u64 = 0;

        // 1. Usamos un clon local del vector para iterar sin bloquear a 'self'
        let index_snapshot = self.index.clone();
        let mut index_it = index_snapshot.iter().peekable();

        while current_pos < file_len {
            // --- INICIO DE VALIDACIÓN DE ÍNDICE ---
            // Extraemos los datos del índice que necesitamos ANTES de la llamada mutable
            let mut expected_offset = None;
            let mut expected_ts = None;

            if let Some(entry) = index_it.peek() {
                if entry.row_number == row_counter {
                    expected_offset = Some(entry.file_offset);
                    expected_ts = Some(entry.timestamp);
                    index_it.next(); // Avanzamos el iterador del clon
                }
            }
            // --- FIN DE VALIDACIÓN DE ÍNDICE (el préstamo de index_it sobre el clon no afecta a self) ---

            // Ahora self está libre para ser prestado como mutable
            let (record, next_pos) = self.read_record_at(current_pos).map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "Falla determinista en fila {} (offset {}): {}",
                        row_counter, current_pos, e
                    ),
                )
            })?;

            // Verificación cruzada con los datos extraídos
            if let Some(off) = expected_offset {
                if off != current_pos {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "Índice corrupto: Fila {} apunta a {}, real: {}",
                            row_counter, off, current_pos
                        ),
                    ));
                }
            }

            if let Some(ts) = expected_ts {
                if ts != record.timestamp {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "Timestamp incoherente en fila {}: Índice {}, Real {}",
                            row_counter, ts, record.timestamp
                        ),
                    ));
                }
            }

            current_pos = next_pos;
            row_counter += 1;
        }

        Ok(AuditReport {
            total_records_found: row_counter,
            total_bytes_processed: current_pos,
            index_validation_count: index_snapshot.len(),
            status: "Sincronía Determinista Perfecta".to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn setup_db(name: &str) -> CursorDB {
        let d = format!("{}.cdb", name);
        let i = format!("{}.cdbi", name);
        let _ = fs::remove_file(&d);
        let _ = fs::remove_file(&i);
        CursorDB::open_or_create(&d, &i).expect("Error al crear DB de test")
    }

    #[test]
    fn test_cursor_logical_consistency() -> std::io::Result<()> {
        let db_name: &str = "logical_test";
        let mut db = setup_db(db_name);

        // 1. Inserción masiva para forzar creación de varios índices
        // Con INDEX_STRIDE = 1024, insertamos 3000 registros
        let total_to_insert = 3000;
        for i in 0..total_to_insert {
            db.append(1000 + i as i64, format!("payload-{}", i).as_bytes())?;
        }

        // 2. Verificación de Límite Superior (Next)
        db.move_to_last()?;
        let last_row = db.current_row();
        assert_eq!(last_row, (total_to_insert - 1) as u64);

        let last_rec = db.next()?.unwrap();
        assert_eq!(last_rec.timestamp, 1000 + (total_to_insert - 1) as i64);
        assert!(db.next()?.is_none(), "Debería ser None después del último");

        // 3. Verificación de Simetría (Next -> Back)
        // Si estamos al final (None), back() debería devolver el último registro (2999)
        let back_rec = db
            .back()?
            .expect("Back desde EOF debe devolver el último registro");
        assert_eq!(back_rec.timestamp, 1000 + 2999);
        assert_eq!(db.current_row(), 2999);

        // 4. Verificación de Salto de Índice (Cruzar fronteras de INDEX_STRIDE)
        // Saltamos al registro 2000 (que está después del segundo marcador de índice)
        db.move_cursor_at(1000 + 2000)?;
        assert_eq!(db.current_row(), 2000);

        // Retroceder un paso debería llevarnos al 1999
        let prev = db.back()?.unwrap();
        assert_eq!(prev.timestamp, 1000 + 1999);
        assert_eq!(db.current_row(), 1999);

        // 5. Verificación de Límite Inferior (Fila 0)
        db.move_cursor_at(1000)?; // Ir al inicio
        assert_eq!(db.current_row(), 0);
        assert!(db.back()?.is_none(), "Back desde la fila 0 debe ser None");

        // El cursor debe seguir en 0 después del None
        assert_eq!(db.current_row(), 0);
        let first = db.current()?.unwrap();
        assert_eq!(first.timestamp, 1000);

        let _ = fs::remove_file(format!("{}.cdb", db_name));
        let _ = fs::remove_file(format!("{}.cdbi", db_name));
        Ok(())
    }

    #[test]
    fn test_deterministic_cursor_boundaries() -> std::io::Result<()> {
        let db_name: &str = "boundary_test";

        let mut db = setup_db(db_name);
        // Insertamos 10 registros (0 al 9)
        for i in 0..10 {
            db.append(i as i64, format!("p-{}", i).as_bytes())?;
        }

        // --- ESCENARIO 1: UNDERFLOW EN RANGOS ---
        db.move_cursor_at(2)?; // Estamos en la fila 2
        // Pedimos 5 antes y 5 después.
        // Debe limitarse automáticamente a [0, 7] sin crashear.
        let range = db.range_around_cursor(5, 5)?;
        assert_eq!(range.len(), 8); // Filas: 0, 1, 2, 3, 4, 5, 6, 7
        assert_eq!(range[0].timestamp, 0);

        // --- ESCENARIO 2: RE-ENTRADA DESDE EL FINAL ---
        db.move_to_last()?; // Fila 9
        db.next()?; // Cursor ahora en Fila 10 (EOF)
        assert!(db.current()?.is_none());

        // ¿Puede volver al archivo?
        let back_to_9 = db.back()?.expect("Debe poder volver desde EOF");
        assert_eq!(back_to_9.timestamp, 9);
        assert_eq!(db.current_row(), 9);

        // --- ESCENARIO 3: SALTOS DISCONTINUOS ---
        // Saltar del final al principio y viceversa
        db.move_cursor_at(0)?;
        assert_eq!(db.current_row(), 0);
        db.move_to_last()?;
        assert_eq!(db.current_row(), 9);

        let _ = fs::remove_file(format!("{}.cdb", db_name));
        let _ = fs::remove_file(format!("{}.cdbi", db_name));
        Ok(())
    }

    #[test]
    fn test_cursor_complete_universe() -> std::io::Result<()> {
        let db_name: &str = "complete_universe_test";

        let mut db = setup_db(db_name);

        let payloads = vec![
            vec![1u8; 10],   // Fila 0
            vec![2u8; 1000], // Fila 1
            vec![3u8; 0],    // Fila 2
            vec![4u8; 50],   // Fila 3
        ];

        let mut expected_offsets = Vec::new();

        // --- FASE DE ESCRITURA ---
        for (i, p) in payloads.iter().enumerate() {
            // Obtenemos el tamaño del archivo directamente del sistema operativo
            // para saber exactamente dónde se escribirá el siguiente registro.
            let offset_fisico = std::fs::metadata(&format!("{}.cdb", db_name))?.len();
            expected_offsets.push(offset_fisico);

            db.append(1000 + i as i64, p)?;
            println!("LOG: Fila {} escrita en offset físico {}", i, offset_fisico);
        }

        // --- FASE DE RETROCESO ---
        db.move_to_last()?;
        db.next()?; // Saltamos al EOF (Fila 4)

        for i in (0..payloads.len()).rev() {
            let rec = db.back()?.expect("Debe existir registro al retroceder");

            let actual_row = db.current_row();
            let actual_offset = db.current_offset();

            println!(
                "Validando Fila {}: Actual [Row: {}, Offset: {}], Esperado Offset: {}",
                i, actual_row, actual_offset, expected_offsets[i]
            );

            assert_eq!(actual_row, i as u64, "Error en número de fila");
            assert_eq!(
                actual_offset, expected_offsets[i],
                "Error en offset: el cursor no aterrizó en el inicio del header"
            );
            assert_eq!(rec.timestamp, 1000 + i as i64, "Datos corruptos");
        }

        // Límite BOF
        assert!(db.back()?.is_none());

        let _ = fs::remove_file(format!("{}.cdb", db_name));
        let _ = fs::remove_file(format!("{}.cdbi", db_name));
        Ok(())
    }

    #[test]
    fn test_cursor_persistence_and_recovery() -> std::io::Result<()> {
        let db_path = "pers_test.cdb";
        let idx_path = "pers_test.cdbi";
        let _ = fs::remove_file(db_path);
        let _ = fs::remove_file(idx_path);

        // 1. Sesión A: Escribir datos y crear índice
        {
            let mut db = CursorDB::open_or_create(db_path, idx_path)?;
            for i in 0..2000 {
                db.append(2000 + i as i64, b"payload")?;
            }
            // Al cerrarse aquí, el índice se guarda en disco
        }

        // 2. Sesión B: Abrir y verificar navegación atrás
        {
            let mut db = CursorDB::open_or_create(db_path, idx_path)?;

            // El total de filas debe ser 2000
            assert_eq!(db.total_rows(), 2000);

            // Ir al final y retroceder para forzar el uso del índice cargado de disco
            db.move_to_last()?;
            for i in (1990..2000).rev() {
                let rec = db.current()?.unwrap();
                assert_eq!(rec.timestamp, 2000 + i as i64);
                db.back()?;
            }
        }

        let _ = fs::remove_file(format!("{}", db_path));
        let _ = fs::remove_file(format!("{}", idx_path));

        Ok(())
    }

    #[test]
    fn test_cursor_ultimate_robustness() -> std::io::Result<()> {
        let db_name = "ultimate_robust_test";
        let mut db = setup_db(db_name);

        // --- 1. PREPARACIÓN: Insertamos 1500 registros ---
        // Usamos timestamps pares (1000, 1002, 1004...) para probar búsquedas de impares
        for i in 0..1500 {
            db.append(1000 + (i * 2) as i64, b"payload")?;
        }

        // --- 2. TEST DE PERSISTENCIA Y CARGA ---
        // Cerramos y reabrimos para forzar la carga del índice desde el disco (.cdbi)
        drop(db);
        let mut db =
            CursorDB::open_or_create(&format!("{}.cdb", db_name), &format!("{}.cdbi", db_name))?;
        assert_eq!(
            db.total_rows, 1500,
            "La persistencia falló al recuperar el conteo de filas"
        );

        // --- 3. TEST DE NAVEGACIÓN HACIA ATRÁS (BACK) ---
        db.move_to_last()?;
        let rec = db.current()?.unwrap();
        assert_eq!(rec.timestamp, 1000 + (1499 * 2) as i64);

        // Retrocedemos 5 pasos
        for _ in 0..5 {
            db.back()?;
        }
        assert_eq!(db.current()?.unwrap().timestamp, 1000 + (1494 * 2) as i64);

        // --- 4. TEST DE ÍNDICE MUTILADO (FALLBACK) ---
        // Borramos el índice de la memoria. back() ahora debe usar escaneo lineal desde 0.
        db.clear_index_in_memory();
        let rec_fallback = db
            .back()?
            .expect("Back debe funcionar mediante escaneo lineal si no hay índice");
        assert_eq!(rec_fallback.timestamp, 1000 + (1493 * 2) as i64);

        // --- 5. TEST DE BÚSQUEDA (MOVE_CURSOR_AT) ---

        // CASO A: Timestamp exacto
        let found = db.move_cursor_at(1100)?; // 1000 + (50 * 2)
        assert!(found.is_some());
        assert_eq!(db.current_row, 50);

        // CASO B: Timestamp inexistente (hueco entre registros)
        // Buscamos 1101, pero solo existen 1100 y 1102. Debe devolver None y mover a EOF.
        let missing_gap = db.move_cursor_at(1101)?;
        assert!(
            missing_gap.is_none(),
            "Debe ser None porque el TS 1101 no existe"
        );
        assert_eq!(
            db.current_row, 1500,
            "El cursor debe quedar en el EOF tras búsqueda fallida"
        );

        // CASO C: Timestamp inexistente (pasado lejano)
        let missing_past = db.move_cursor_at(500)?;
        assert!(missing_past.is_none());
        assert_eq!(
            db.current_row, 1500,
            "Búsqueda en el pasado fallida debe llevar al EOF"
        );

        // CASO D: Timestamp inexistente (futuro lejano)
        let missing_future = db.move_cursor_at(9999)?;
        assert!(missing_future.is_none());
        assert_eq!(
            db.current_row, 1500,
            "Búsqueda en el futuro fallida debe llevar al EOF"
        );

        // --- 6. RECUPERACIÓN TRAS FALLO ---
        // Verificamos que tras un fallo de búsqueda, back() puede traernos de vuelta al último registro
        let rescue = db
            .back()?
            .expect("Back debe poder rescatar al cursor del EOF");
        assert_eq!(rescue.timestamp, 1000 + (1499 * 2) as i64);

        // Limpieza
        fs::remove_file(format!("{}.cdb", db_name))?;
        fs::remove_file(format!("{}.cdbi", db_name))?;
        Ok(())
    }

    #[test]
    fn test_cursor_stride_boundaries() -> std::io::Result<()> {
        let db_name: &str = "stride_test";

        let mut db = setup_db(db_name);
        let stride = 1024; // Asumiendo que tu constante es esta

        // 1. Insertamos justo hasta el límite del primer salto de índice
        for i in 0..=stride {
            db.append(2000 + i as i64, b"test")?;
        }

        // 2. Probamos que el salto al registro indexado (1023)
        // y al no indexado (1024) funciona perfectamente.
        db.move_cursor_at(2000 + stride as i64)?;
        assert_eq!(db.current_row(), stride as u64);

        db.back()?;
        assert_eq!(db.current_row(), (stride - 1) as u64);

        let _ = fs::remove_file(format!("{}.cdb", db_name));
        let _ = fs::remove_file(format!("{}.cdbi", db_name));
        Ok(())
    }

    #[test]
    fn test_cursor_exhaustive_traversal() -> std::io::Result<()> {
        let db_name: &str = "exhaustive_test";
        let mut db = setup_db(db_name);

        for i in 1..=10 {
            db.append((i * 10) as i64, format!("data_{}", i).as_bytes())?;
        }

        // --- IDA: Hasta el final ---
        db.move_cursor_at(10)?;
        for i in 0..10 {
            assert_eq!(db.current_row, i as u64, "Error de fila en avance");
            if i < 9 {
                db.next()?;
            }
        }

        // Forzamos EOF
        db.next()?;
        assert_eq!(db.current_row, 10, "Debe estar en la fila 10 (EOF)");

        // --- VUELTA: Hasta el inicio real ---
        // El primer back() nos lleva de la 10 (EOF) a la 9.
        // Los siguientes 9 back() nos llevan de la 9 a la 0.
        for i in (0..10).rev() {
            let rec = db.back()?.expect("Debe poder retroceder");
            assert_eq!(db.current_row, i as u64, "Error de fila en retroceso");
            assert_eq!(rec.timestamp, ((i + 1) * 10) as i64);
        }

        // --- LA PRUEBA DE FUEGO ---
        // Ahora estamos REALMENTE en la fila 0.
        let final_back = db.back()?;
        assert!(
            final_back.is_none(),
            "Back desde la fila 0 DEBE devolver None"
        );
        assert_eq!(db.current_row, 0, "El cursor no debe moverse de 0");

        fs::remove_file(format!("{}.cdb", db_name))?;
        fs::remove_file(format!("{}.cdbi", db_name))?;

        Ok(())
    }

    #[test]
    fn test_cursor_resilience_to_truncation() -> std::io::Result<()> {
        let db_name = "resilience_test";
        let mut db = setup_db(db_name);

        // 1. Insertamos 10 registros (cada uno ocupa 20 bytes: 16 header + 4 data)
        for i in 1..=10 {
            db.append(i as i64, b"data")?;
        }

        // 2. Truncamos el archivo a la mitad (50 bytes)
        // Esto deja 2 registros perfectos (40 bytes) y el 3ero mutilado (solo 10 bytes de header)
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(format!("{}.cdb", db_name))?;
        file.set_len(50)?;
        drop(file);

        // --- ESCENARIO A: Intentar leer el registro dañado ---
        // Intentamos buscar el registro 8. El índice nos mandará a un offset que ya no existe o está incompleto.
        let result = db.move_cursor_at(8);

        // Validamos que NO hubo pánico, sino un error controlado
        assert!(
            result.is_err(),
            "La DB debería haber detectado que el registro está incompleto"
        );

        if let Err(e) = result {
            assert_eq!(e.kind(), std::io::ErrorKind::InvalidData);
            println!(
                "Resiliencia confirmada: Error detectado correctamente -> {}",
                e
            );
        }

        // --- ESCENARIO B: Recuperación ---
        // Probamos que el cursor puede volver a una zona segura (registro 1)
        let recovered = db.move_cursor_at(1)?;
        assert!(recovered.is_some());
        assert_eq!(
            recovered.unwrap().timestamp,
            1,
            "La DB debe poder recuperarse y leer zonas sanas"
        );

        // Limpieza
        let _ = fs::remove_file(format!("{}.cdb", db_name));
        let _ = fs::remove_file(format!("{}.cdbi", db_name));
        Ok(())
    }

    #[test]
    fn test_cursor_full_lifecycle_integration() -> std::io::Result<()> {
        let db_name = "full_lifecycle_test";
        let mut db = setup_db(db_name);

        // 1. APPEND: 10 registros (Timestamps 100 al 1000)
        for i in 1..=10 {
            db.append((i * 100) as i64, format!("payload_{}", i).as_bytes())?;
        }

        // 2. MOVE_CURSOR_AT & CURRENT
        let rec = db.move_cursor_at(500)?.expect("Debe encontrar TS 500");
        assert_eq!(rec.timestamp, 500);
        assert_eq!(db.current_row, 4);

        let curr = db.current()?.unwrap();
        assert_eq!(curr.timestamp, 500);

        // 3. NEXT & BACK
        db.next()?; // 600
        db.next()?; // 700
        assert_eq!(db.current()?.unwrap().timestamp, 700);

        db.back()?; // 600
        assert_eq!(db.current()?.unwrap().timestamp, 600);

        // 4. READ_RECORD_AT (Sin romper el índice)
        // En lugar de acceder al índice[1], leemos el offset del registro actual
        // para verificar que read_record_at funciona sin mover el cursor.
        let current_off = db.current_offset;
        let (rec_static, _) = db.read_record_at(current_off)?;
        assert_eq!(rec_static.timestamp, 600);
        assert_eq!(db.current_row, 5, "El cursor no debe haberse movido");

        // 5. RANGE_AROUND_CURSOR
        // En 600 (fila 5), pedimos 1 antes y 1 después: [500, 600, 700]
        let range = db.range_around_cursor(1, 1)?;
        assert_eq!(range.len(), 3);
        assert_eq!(range[0].timestamp, 500);
        assert_eq!(range[1].timestamp, 600);
        assert_eq!(range[2].timestamp, 700);

        // 6. MOVE_TO_LAST
        db.move_to_last()?;
        assert_eq!(db.current()?.unwrap().timestamp, 1000);
        assert_eq!(db.current_row, 9);

        // 7. COMPORTAMIENTO EN FRONTERA FINAL (EOF)
        db.next()?;
        assert!(db.current()?.is_none());
        assert_eq!(db.current_row, 10);

        // 8. REGRESO DESDE EL VACÍO
        db.back()?;
        assert_eq!(db.current()?.unwrap().timestamp, 1000);
        assert_eq!(db.current_row, 9);

        // Limpieza
        let _ = fs::remove_file(format!("{}.cdb", db_name));
        let _ = fs::remove_file(format!("{}.cdbi", db_name));

        Ok(())
    }

    #[test]
    fn test_verify_deterministic_integrity() -> std::io::Result<()> {
        let data_path = "deterministic.cdb";
        let index_path = "deterministic.cdbi";

        // Limpieza previa
        let _ = std::fs::remove_file(data_path);
        let _ = std::fs::remove_file(index_path);

        {
            let mut db = CursorDB::open_or_create(data_path, index_path)?;
            // Insertamos datos suficientes para cruzar el INDEX_STRIDE (1024)
            for i in 0..1050 {
                db.append(i as i64, b"datos_deterministas")?;
            }

            // 1. Verificar estado limpio
            let report = db.verify_deterministic_integrity()?;
            assert_eq!(report.total_records_found, 1050);
            println!("✔ Fase 1: Datos limpios verificados.");
        }

        // 2. ESCENARIO: Corrupción de Bit (Bit Rot)
        // Alteramos un solo byte en el cuerpo del archivo
        {
            let mut file = OpenOptions::new().write(true).open(data_path)?;
            file.seek(SeekFrom::Start(5000))?; // Offset arbitrario
            file.write_all(&[0xFF])?;
        }

        {
            let mut db = CursorDB::open_or_create(data_path, index_path)?;
            let result = db.verify_deterministic_integrity();
            assert!(result.is_err(), "Debería detectar CRC inválido");
            println!("✔ Fase 2: Bit Rot detectado correctamente.");
        }

        // 3. ESCENARIO: Índice mentiroso
        // Restauramos el archivo y corrompemos el índice manualmente
        let _ = std::fs::remove_file(data_path);
        let _ = std::fs::remove_file(index_path);
        {
            let mut db = CursorDB::open_or_create(data_path, index_path)?;
            db.append(100, b"dato1")?; // Fila 0
            db.append(200, b"dato2")?; // Fila 1

            // Forzamos un error de desfase:
            // Cambiamos el offset del índice para que apunte a cualquier lugar menos al 0
            db.index[0].file_offset = 500;

            let result = db.verify_deterministic_integrity();
            assert!(result.is_err(), "Debería fallar porque el índice miente");

            if let Err(e) = result {
                let msg = e.to_string();
                println!("DEBUG: Error real detectado: {}", msg);
                // Verificamos que sea un error de integridad o de datos inválidos
                assert!(
                    msg.contains("Falla determinista")
                        || msg.contains("Índice corrupto")
                        || msg.contains("Desfase")
                );
            }
            println!("✔ Fase 3: Índice inconsistente detectado.");
        }

        // 4. ESCENARIO: Basura al final (Trailing Garbage)
        {
            let mut file = std::fs::OpenOptions::new().append(true).open(data_path)?;
            file.write_all(b"basura")?;
        }

        // Intentamos abrir la DB.
        // Si tu motor es determinista, debería fallar AQUÍ o en el audit.
        let db_result = CursorDB::open_or_create(data_path, index_path);

        match db_result {
            // Caso A: El motor detectó la basura al intentar abrir (reconcile)
            Err(e) => {
                println!("✔ Fase 4: Éxito. El motor detectó basura al abrir: {}", e);
                assert!(
                    e.to_string().contains("Header incompleto") || e.to_string().contains("basura")
                );
            }
            // Caso B: El motor abrió, pero la auditoría debe fallar
            Ok(mut db) => {
                let audit_result = db.verify_deterministic_integrity();
                match audit_result {
                    Err(e) => {
                        println!("✔ Fase 4: Éxito. La auditoría detectó la basura: {}", e);
                        assert!(
                            e.to_string().contains("Header incompleto")
                                || e.to_string().contains("basura")
                        );
                    }
                    Ok(_) => panic!("Falla de Determinismo: El motor aceptó basura sin rechistar"),
                }
            }
        }

        let _ = fs::remove_file(format!("{}", data_path));
        let _ = fs::remove_file(format!("{}", index_path));
        Ok(())
    }
}
