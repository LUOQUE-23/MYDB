use std::path::Path;

use rusedb_core::Result;

use crate::{DiskManager, Page, PageId};

#[derive(Debug)]
pub struct BufferPool {
    disk: DiskManager,
    capacity: usize,
}

impl BufferPool {
    pub fn open(path: impl AsRef<Path>, capacity: usize) -> Result<Self> {
        Ok(Self {
            disk: DiskManager::open(path)?,
            capacity,
        })
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn page_count(&self) -> PageId {
        self.disk.page_count()
    }

    pub fn read_page(&mut self, page_id: PageId) -> Result<Page> {
        self.disk.read_page(page_id)
    }

    pub fn write_page(&mut self, page: &Page) -> Result<()> {
        self.disk.write_page(page)
    }
}
