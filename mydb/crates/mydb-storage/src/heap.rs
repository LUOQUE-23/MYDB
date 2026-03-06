use std::path::Path;

use mydb_core::{MyDbError, Result};

use crate::disk::DiskManager;
use crate::page::Rid;

#[derive(Debug)]
pub struct HeapFile {
    disk: DiskManager,
}

impl HeapFile {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let disk = DiskManager::open(path)?;
        Ok(Self { disk })
    }

    pub fn insert_record(&mut self, record: &[u8]) -> Result<Rid> {
        for page_id in 0..self.disk.page_count() {
            let mut page = self.disk.read_page(page_id)?;
            match page.insert_record(record) {
                Ok(slot_id) => {
                    self.disk.write_page(&page)?;
                    return Ok(Rid { page_id, slot_id });
                }
                Err(MyDbError::PageFull { .. }) => continue,
                Err(err) => return Err(err),
            }
        }

        let mut page = self.disk.allocate_page();
        let page_id = page.page_id();
        let slot_id = page.insert_record(record)?;
        self.disk.write_page(&page)?;
        Ok(Rid { page_id, slot_id })
    }

    pub fn get_record(&mut self, rid: Rid) -> Result<Vec<u8>> {
        let page = self.disk.read_page(rid.page_id)?;
        page.get_record(rid.slot_id).ok_or(MyDbError::InvalidRid {
            page_id: rid.page_id,
            slot_id: rid.slot_id,
        })
    }

    pub fn delete_record(&mut self, rid: Rid) -> Result<()> {
        let mut page = self.disk.read_page(rid.page_id)?;
        if !page.delete_record(rid.slot_id) {
            return Err(MyDbError::InvalidRid {
                page_id: rid.page_id,
                slot_id: rid.slot_id,
            });
        }
        self.disk.write_page(&page)?;
        Ok(())
    }

    pub fn update_record(&mut self, rid: Rid, new_record: &[u8]) -> Result<Rid> {
        self.delete_record(rid)?;
        self.insert_record(new_record)
    }

    pub fn scan_records(&mut self) -> Result<Vec<(Rid, Vec<u8>)>> {
        let mut out = Vec::new();
        for page_id in 0..self.disk.page_count() {
            let page = self.disk.read_page(page_id)?;
            for slot_id in 0..page.slot_count() {
                if let Some(record) = page.get_record(slot_id) {
                    out.push((Rid { page_id, slot_id }, record));
                }
            }
        }
        Ok(out)
    }

    pub fn page_count(&self) -> u32 {
        self.disk.page_count()
    }

    pub fn sync(&mut self) -> Result<()> {
        self.disk.sync()
    }
}
