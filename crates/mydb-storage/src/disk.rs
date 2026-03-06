use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use mydb_core::config::PAGE_SIZE;
use mydb_core::{MyDbError, Result};

use crate::page::{Page, PageId};

#[derive(Debug)]
pub struct DiskManager {
    path: PathBuf,
    file: std::fs::File,
    next_page_id: PageId,
}

impl DiskManager {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        let file_len = file.metadata()?.len() as usize;
        if file_len % PAGE_SIZE != 0 {
            return Err(MyDbError::Corruption(format!(
                "file length {} is not multiple of page size {}",
                file_len, PAGE_SIZE
            )));
        }
        let next_page_id = (file_len / PAGE_SIZE) as PageId;
        Ok(Self {
            path,
            file,
            next_page_id,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn page_count(&self) -> PageId {
        self.next_page_id
    }

    pub fn allocate_page(&mut self) -> Page {
        let page_id = self.next_page_id;
        self.next_page_id += 1;
        Page::new(page_id)
    }

    pub fn read_page(&mut self, page_id: PageId) -> Result<Page> {
        if page_id >= self.next_page_id {
            return Err(MyDbError::PageOutOfRange {
                page_id,
                page_count: self.next_page_id,
            });
        }

        let mut bytes = [0u8; PAGE_SIZE];
        self.file
            .seek(SeekFrom::Start(page_id as u64 * PAGE_SIZE as u64))?;
        self.file.read_exact(&mut bytes)?;
        Page::from_bytes(bytes)
    }

    pub fn write_page(&mut self, page: &Page) -> Result<()> {
        let page_id = page.page_id();
        if page_id > self.next_page_id {
            return Err(MyDbError::PageOutOfRange {
                page_id,
                page_count: self.next_page_id,
            });
        }
        if page_id == self.next_page_id {
            self.next_page_id += 1;
        }

        self.file
            .seek(SeekFrom::Start(page_id as u64 * PAGE_SIZE as u64))?;
        self.file.write_all(page.as_bytes())?;
        self.file.flush()?;
        Ok(())
    }

    pub fn sync(&mut self) -> Result<()> {
        self.file.sync_data()?;
        Ok(())
    }
}
