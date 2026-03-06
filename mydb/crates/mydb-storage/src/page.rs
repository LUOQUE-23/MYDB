use mydb_core::config::PAGE_SIZE;
use mydb_core::{MyDbError, Result};

pub type PageId = u32;
pub type SlotId = u16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Rid {
    pub page_id: PageId,
    pub slot_id: SlotId,
}

const HEADER_SIZE: usize = 16;
const SLOT_SIZE: usize = 4;
const PAGE_ID_OFFSET: usize = 0;
const LSN_OFFSET: usize = 4;
const FREE_SPACE_OFFSET: usize = 12;
const SLOT_COUNT_OFFSET: usize = 14;

#[derive(Debug, Clone)]
pub struct Page {
    data: [u8; PAGE_SIZE],
}

impl Page {
    pub fn new(page_id: PageId) -> Self {
        let mut page = Self {
            data: [0; PAGE_SIZE],
        };
        page.set_page_id(page_id);
        page.set_lsn(0);
        page.set_free_space_offset(PAGE_SIZE as u16);
        page.set_slot_count(0);
        page
    }

    pub fn from_bytes(data: [u8; PAGE_SIZE]) -> Result<Self> {
        let page = Self { data };
        page.validate_layout()?;
        Ok(page)
    }

    pub fn as_bytes(&self) -> &[u8; PAGE_SIZE] {
        &self.data
    }

    pub fn page_id(&self) -> PageId {
        u32::from_le_bytes(
            self.data[PAGE_ID_OFFSET..PAGE_ID_OFFSET + 4]
                .try_into()
                .unwrap(),
        )
    }

    pub fn lsn(&self) -> u64 {
        u64::from_le_bytes(self.data[LSN_OFFSET..LSN_OFFSET + 8].try_into().unwrap())
    }

    pub fn set_lsn(&mut self, lsn: u64) {
        self.data[LSN_OFFSET..LSN_OFFSET + 8].copy_from_slice(&lsn.to_le_bytes());
    }

    pub fn slot_count(&self) -> u16 {
        u16::from_le_bytes(
            self.data[SLOT_COUNT_OFFSET..SLOT_COUNT_OFFSET + 2]
                .try_into()
                .unwrap(),
        )
    }

    pub fn free_space(&self) -> usize {
        let dir_end = self.slot_dir_end();
        let free_offset = usize::from(self.free_space_offset());
        free_offset.saturating_sub(dir_end)
    }

    pub fn insert_record(&mut self, record: &[u8]) -> Result<SlotId> {
        if record.is_empty() || record.len() > u16::MAX as usize {
            return Err(MyDbError::RecordTooLarge { size: record.len() });
        }

        let reusable_slot = self.find_reusable_slot();
        let required = record.len()
            + if reusable_slot.is_none() {
                SLOT_SIZE
            } else {
                0
            };
        if self.free_space() < required {
            self.compact();
            if self.free_space() < required {
                return Err(MyDbError::PageFull {
                    page_id: self.page_id(),
                });
            }
        }

        let free_end = usize::from(self.free_space_offset());
        let start = free_end - record.len();
        self.data[start..free_end].copy_from_slice(record);
        self.set_free_space_offset(start as u16);

        let slot_id = match reusable_slot {
            Some(id) => id,
            None => {
                let id = self.slot_count();
                self.set_slot_count(id + 1);
                id
            }
        };
        self.write_slot(slot_id, start as u16, record.len() as u16);
        Ok(slot_id)
    }

    pub fn get_record(&self, slot_id: SlotId) -> Option<Vec<u8>> {
        if slot_id >= self.slot_count() {
            return None;
        }
        let (offset, len) = self.read_slot(slot_id);
        if len == 0 {
            return None;
        }
        let start = usize::from(offset);
        let end = start + usize::from(len);
        Some(self.data[start..end].to_vec())
    }

    pub fn delete_record(&mut self, slot_id: SlotId) -> bool {
        if slot_id >= self.slot_count() {
            return false;
        }
        let (_, len) = self.read_slot(slot_id);
        if len == 0 {
            return false;
        }
        self.write_slot(slot_id, 0, 0);
        true
    }

    fn validate_layout(&self) -> Result<()> {
        let free_offset = usize::from(self.free_space_offset());
        let slot_count = usize::from(self.slot_count());
        let dir_end = HEADER_SIZE + slot_count * SLOT_SIZE;
        if free_offset < dir_end || free_offset > PAGE_SIZE {
            return Err(MyDbError::Corruption(format!(
                "page {} free offset {} out of bounds",
                self.page_id(),
                free_offset
            )));
        }

        for slot_id in 0..self.slot_count() {
            let (offset, len) = self.read_slot(slot_id);
            if len == 0 {
                continue;
            }
            let start = usize::from(offset);
            let end = start + usize::from(len);
            if start < dir_end || end > PAGE_SIZE || start >= end {
                return Err(MyDbError::Corruption(format!(
                    "page {} slot {} has invalid range {}..{}",
                    self.page_id(),
                    slot_id,
                    start,
                    end
                )));
            }
        }

        Ok(())
    }

    fn compact(&mut self) {
        let slot_count = self.slot_count();
        let mut records = Vec::new();
        for slot_id in 0..slot_count {
            if let Some(bytes) = self.get_record(slot_id) {
                records.push((slot_id, bytes));
            }
        }

        let page_id = self.page_id();
        let lsn = self.lsn();
        self.data = [0; PAGE_SIZE];
        self.set_page_id(page_id);
        self.set_lsn(lsn);
        self.set_slot_count(slot_count);
        self.set_free_space_offset(PAGE_SIZE as u16);
        for slot_id in 0..slot_count {
            self.write_slot(slot_id, 0, 0);
        }

        for (slot_id, record) in records {
            let free_end = usize::from(self.free_space_offset());
            let start = free_end - record.len();
            self.data[start..free_end].copy_from_slice(&record);
            self.set_free_space_offset(start as u16);
            self.write_slot(slot_id, start as u16, record.len() as u16);
        }
    }

    fn find_reusable_slot(&self) -> Option<SlotId> {
        (0..self.slot_count()).find(|&slot_id| {
            let (_, len) = self.read_slot(slot_id);
            len == 0
        })
    }

    fn slot_dir_end(&self) -> usize {
        HEADER_SIZE + usize::from(self.slot_count()) * SLOT_SIZE
    }

    fn free_space_offset(&self) -> u16 {
        u16::from_le_bytes(
            self.data[FREE_SPACE_OFFSET..FREE_SPACE_OFFSET + 2]
                .try_into()
                .unwrap(),
        )
    }

    fn set_page_id(&mut self, page_id: PageId) {
        self.data[PAGE_ID_OFFSET..PAGE_ID_OFFSET + 4].copy_from_slice(&page_id.to_le_bytes());
    }

    fn set_free_space_offset(&mut self, offset: u16) {
        self.data[FREE_SPACE_OFFSET..FREE_SPACE_OFFSET + 2].copy_from_slice(&offset.to_le_bytes());
    }

    fn set_slot_count(&mut self, slot_count: u16) {
        self.data[SLOT_COUNT_OFFSET..SLOT_COUNT_OFFSET + 2]
            .copy_from_slice(&slot_count.to_le_bytes());
    }

    fn slot_entry_offset(slot_id: SlotId) -> usize {
        HEADER_SIZE + usize::from(slot_id) * SLOT_SIZE
    }

    fn read_slot(&self, slot_id: SlotId) -> (u16, u16) {
        let offset = Self::slot_entry_offset(slot_id);
        let rec_offset = u16::from_le_bytes(
            self.data[offset..offset + 2]
                .try_into()
                .expect("slot offset"),
        );
        let rec_len = u16::from_le_bytes(
            self.data[offset + 2..offset + SLOT_SIZE]
                .try_into()
                .expect("slot len"),
        );
        (rec_offset, rec_len)
    }

    fn write_slot(&mut self, slot_id: SlotId, rec_offset: u16, rec_len: u16) {
        let offset = Self::slot_entry_offset(slot_id);
        self.data[offset..offset + 2].copy_from_slice(&rec_offset.to_le_bytes());
        self.data[offset + 2..offset + SLOT_SIZE].copy_from_slice(&rec_len.to_le_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_get_roundtrip() {
        let mut page = Page::new(1);
        let slot_a = page.insert_record(b"hello").unwrap();
        let slot_b = page.insert_record(b"world").unwrap();
        assert_eq!(page.get_record(slot_a).unwrap(), b"hello");
        assert_eq!(page.get_record(slot_b).unwrap(), b"world");
    }

    #[test]
    fn delete_reuses_slot() {
        let mut page = Page::new(7);
        let first = page.insert_record(b"a").unwrap();
        let _second = page.insert_record(b"b").unwrap();
        assert!(page.delete_record(first));
        let reused = page.insert_record(b"c").unwrap();
        assert_eq!(reused, first);
    }
}
