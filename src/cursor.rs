use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};

use crate::record::Record;

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
    // --------------------------------------------------
    // OPEN / CREATE
    // --------------------------------------------------
    pub fn open_or_create(data_path: &str, index_path: &str) -> std::io::Result<Self> {
        let data_file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(data_path)?;

        let index_file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(index_path)?;

        let mut db = Self {
            data_file,
            index_file,
            index: Vec::new(),
            current_row: 0,
            current_offset: 0,
            total_rows: 0,
        };

        db.load_index()?;
        Ok(db)
    }

    fn load_index(&mut self) -> std::io::Result<()> {
        let mut buf = Vec::new();
        self.index_file.seek(SeekFrom::Start(0))?;
        self.index_file.read_to_end(&mut buf)?;

        let mut offset = 0;
        while offset + 24 <= buf.len() {
            let row = u64::from_le_bytes(buf[offset..offset + 8].try_into().unwrap());
            let ts = i64::from_le_bytes(buf[offset + 8..offset + 16].try_into().unwrap());
            let file_offset = u64::from_le_bytes(buf[offset + 16..offset + 24].try_into().unwrap());

            self.index.push(IndexEntry {
                row_number: row,
                timestamp: ts,
                file_offset,
            });

            offset += 24;
            self.total_rows = row + 1;
        }

        if let Some(last) = self.index.last() {
            self.current_row = last.row_number;
            self.current_offset = last.file_offset;
        }

        Ok(())
    }

    // --------------------------------------------------
    // APPEND
    // --------------------------------------------------
    pub fn append(&mut self, timestamp: i64, payload: &[u8]) -> std::io::Result<()> {
        let offset = self.data_file.seek(SeekFrom::End(0))?;

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
