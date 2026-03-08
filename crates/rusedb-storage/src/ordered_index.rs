use std::cmp::Ordering;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use rusedb_core::config::PAGE_SIZE;
use rusedb_core::{DataType, Result, RuseDbError, Value};

use crate::Rid;

const MAGIC: &[u8; 8] = b"RUSEDBI1";
const LEGACY_MAGIC: &[u8; 8] = &[77, 89, 68, 66, 66, 84, 73, 49];
const META_PAGE_ID: u32 = 0;
const NODE_LEAF: u8 = 1;
const NODE_INTERNAL: u8 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexKeyKind {
    Int = 1,
    Str = 2,
    Bool = 3,
}

impl IndexKeyKind {
    pub fn from_data_type(data_type: DataType) -> Result<Self> {
        match data_type {
            DataType::Int | DataType::BigInt => Ok(Self::Int),
            DataType::Varchar => Ok(Self::Str),
            DataType::Bool => Ok(Self::Bool),
            DataType::Double => Err(RuseDbError::Parse(
                "DOUBLE index key not supported yet".to_string(),
            )),
        }
    }

    fn from_u8(raw: u8) -> Result<Self> {
        match raw {
            1 => Ok(Self::Int),
            2 => Ok(Self::Str),
            3 => Ok(Self::Bool),
            _ => Err(RuseDbError::Corruption(format!(
                "unknown index key kind {}",
                raw
            ))),
        }
    }

    fn to_u8(self) -> u8 {
        self as u8
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum IndexKey {
    Int(i64),
    Str(String),
    Bool(bool),
}

#[derive(Debug, Clone)]
struct LeafEntry {
    key: IndexKey,
    rid: Rid,
}

#[derive(Debug, Clone)]
struct InternalEntry {
    key: IndexKey,
    child: u32,
}

#[derive(Debug, Clone)]
struct LeafNode {
    entries: Vec<LeafEntry>,
    next_leaf: Option<u32>,
}

#[derive(Debug, Clone)]
struct InternalNode {
    first_child: u32,
    keys: Vec<InternalEntry>,
}

#[derive(Debug, Clone)]
enum Node {
    Leaf(LeafNode),
    Internal(InternalNode),
}

#[derive(Debug, Clone)]
struct SplitInfo {
    separator: IndexKey,
    right_page_id: u32,
}

#[derive(Debug, Clone, Copy)]
struct ParentFrame {
    page_id: u32,
    child_index: usize,
}

#[derive(Debug)]
pub struct OrderedIndex {
    file: File,
    key_kind: IndexKeyKind,
    root_page_id: Option<u32>,
    next_page_id: u32,
}

impl OrderedIndex {
    pub fn open(path: impl AsRef<Path>, key_kind: IndexKeyKind) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        let file_len = file.metadata()?.len() as usize;
        if file_len == 0 {
            let mut index = Self {
                file,
                key_kind,
                root_page_id: None,
                next_page_id: 1,
            };
            index.write_meta()?;
            return Ok(index);
        }
        if file_len < PAGE_SIZE || file_len % PAGE_SIZE != 0 {
            return Err(RuseDbError::Corruption(format!(
                "index file '{}' has invalid length {}",
                path.display(),
                file_len
            )));
        }

        let mut meta = [0u8; PAGE_SIZE];
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut meta)?;
        let disk_magic = &meta[0..8];
        if disk_magic != MAGIC && disk_magic != LEGACY_MAGIC {
            return Err(RuseDbError::Corruption(format!(
                "invalid index magic in '{}'",
                path.display()
            )));
        }
        let disk_kind = IndexKeyKind::from_u8(meta[8])?;
        if disk_kind != key_kind {
            return Err(RuseDbError::Corruption(format!(
                "index key kind mismatch for '{}': on disk {:?}, expected {:?}",
                path.display(),
                disk_kind,
                key_kind
            )));
        }
        let root_raw = u32::from_le_bytes(meta[9..13].try_into().unwrap());
        let root_page_id = if root_raw == 0 { None } else { Some(root_raw) };
        let mut next_page_id = u32::from_le_bytes(meta[13..17].try_into().unwrap());
        let physical_page_count = (file_len / PAGE_SIZE) as u32;
        if next_page_id < 1 {
            next_page_id = 1;
        }
        if next_page_id < physical_page_count {
            next_page_id = physical_page_count;
        }

        Ok(Self {
            file,
            key_kind,
            root_page_id,
            next_page_id,
        })
    }

    pub fn key_kind(&self) -> IndexKeyKind {
        self.key_kind
    }

    pub fn key_from_value(&self, value: &Value) -> Result<Option<IndexKey>> {
        if matches!(value, Value::Null) {
            return Ok(None);
        }

        let key = match (self.key_kind, value) {
            (IndexKeyKind::Int, Value::Int(v)) => IndexKey::Int(i64::from(*v)),
            (IndexKeyKind::Int, Value::BigInt(v)) => IndexKey::Int(*v),
            (IndexKeyKind::Str, Value::Varchar(v)) => IndexKey::Str(v.clone()),
            (IndexKeyKind::Bool, Value::Bool(v)) => IndexKey::Bool(*v),
            _ => {
                return Err(RuseDbError::Parse(format!(
                    "value type {} does not match index key kind {:?}",
                    value.type_name(),
                    self.key_kind
                )));
            }
        };
        Ok(Some(key))
    }

    pub fn insert(&mut self, key: IndexKey, rid: Rid) -> Result<()> {
        if self.root_page_id.is_none() {
            let page_id = self.allocate_page()?;
            let node = Node::Leaf(LeafNode {
                entries: vec![LeafEntry { key, rid }],
                next_leaf: None,
            });
            self.write_node(page_id, &node)?;
            self.root_page_id = Some(page_id);
            self.write_meta()?;
            self.file.sync_data()?;
            return Ok(());
        }

        let root = self.root_page_id.unwrap();
        if let Some(split) = self.insert_into(root, key, rid)? {
            let new_root_page = self.allocate_page()?;
            let root_node = Node::Internal(InternalNode {
                first_child: root,
                keys: vec![InternalEntry {
                    key: split.separator,
                    child: split.right_page_id,
                }],
            });
            self.write_node(new_root_page, &root_node)?;
            self.root_page_id = Some(new_root_page);
            self.write_meta()?;
        }
        self.file.sync_data()?;
        Ok(())
    }

    pub fn remove(&mut self, key: &IndexKey, rid: Rid) -> Result<()> {
        let Some(root) = self.root_page_id else {
            return Ok(());
        };

        let (leaf_page, path) = self.find_leaf_with_path(root, key)?;
        let node = self.read_node(leaf_page)?;
        let Node::Leaf(mut leaf) = node else {
            return Err(RuseDbError::Corruption(
                "expected leaf node while removing index entry".to_string(),
            ));
        };

        let before = leaf.entries.len();
        leaf.entries
            .retain(|entry| !(entry.key == *key && entry.rid == rid));
        if leaf.entries.len() != before {
            self.write_node(leaf_page, &Node::Leaf(leaf))?;
            self.rebalance_after_delete(leaf_page, path)?;
            self.normalize_root_after_delete()?;
            self.rebuild_all_separators()?;
            self.file.sync_data()?;
        }
        Ok(())
    }

    pub fn search_eq(&mut self, key: &IndexKey) -> Result<Vec<Rid>> {
        let Some(root) = self.root_page_id else {
            return Ok(Vec::new());
        };
        let mut page = self.find_leaf_page(root, key)?;
        let mut out = Vec::new();

        loop {
            let node = self.read_node(page)?;
            let Node::Leaf(leaf) = node else {
                return Err(RuseDbError::Corruption(
                    "expected leaf node during equality lookup".to_string(),
                ));
            };

            let mut should_continue = false;
            for entry in &leaf.entries {
                match entry.key.cmp(key) {
                    Ordering::Less => continue,
                    Ordering::Equal => {
                        out.push(entry.rid);
                        should_continue = true;
                    }
                    Ordering::Greater => return Ok(out),
                }
            }

            if !should_continue {
                return Ok(out);
            }
            if let Some(next) = leaf.next_leaf {
                page = next;
            } else {
                return Ok(out);
            }
        }
    }

    pub fn search_range(
        &mut self,
        lower: Option<(IndexKey, bool)>,
        upper: Option<(IndexKey, bool)>,
    ) -> Result<Vec<Rid>> {
        let Some(root) = self.root_page_id else {
            return Ok(Vec::new());
        };

        let mut page = match &lower {
            Some((key, _)) => self.find_leaf_page(root, key)?,
            None => self.find_leftmost_leaf(root)?,
        };

        let mut out = Vec::new();
        loop {
            let node = self.read_node(page)?;
            let Node::Leaf(leaf) = node else {
                return Err(RuseDbError::Corruption(
                    "expected leaf node during range lookup".to_string(),
                ));
            };

            let mut end_reached = false;
            for entry in &leaf.entries {
                if !range_contains(&entry.key, lower.as_ref(), upper.as_ref()) {
                    if upper
                        .as_ref()
                        .map(|(bound, _)| entry.key > *bound)
                        .unwrap_or(false)
                    {
                        end_reached = true;
                        break;
                    }
                    continue;
                }
                out.push(entry.rid);
            }

            if end_reached {
                return Ok(out);
            }
            if let Some(next) = leaf.next_leaf {
                page = next;
            } else {
                return Ok(out);
            }
        }
    }

    fn insert_into(&mut self, page_id: u32, key: IndexKey, rid: Rid) -> Result<Option<SplitInfo>> {
        match self.read_node(page_id)? {
            Node::Leaf(mut leaf) => {
                let insert_pos = leaf
                    .entries
                    .binary_search_by(|entry| cmp_leaf_entry(entry, &key, rid))
                    .unwrap_or_else(|p| p);
                if leaf
                    .entries
                    .get(insert_pos)
                    .map(|entry| entry.key == key && entry.rid == rid)
                    .unwrap_or(false)
                {
                    return Ok(None);
                }
                leaf.entries.insert(insert_pos, LeafEntry { key, rid });

                let node = Node::Leaf(leaf.clone());
                if self.encoded_node_len(&node)? <= PAGE_SIZE {
                    self.write_node(page_id, &node)?;
                    return Ok(None);
                }

                if leaf.entries.len() < 2 {
                    return Err(RuseDbError::RecordTooLarge {
                        size: self.encoded_node_len(&node)?,
                    });
                }
                let mid = leaf.entries.len() / 2;
                let right_entries = leaf.entries.split_off(mid);
                let separator = right_entries.first().map(|entry| entry.key.clone()).ok_or(
                    RuseDbError::Corruption("leaf split generated empty right node".to_string()),
                )?;
                let right_page = self.allocate_page()?;
                let left_next = leaf.next_leaf;
                leaf.next_leaf = Some(right_page);

                let left_node = Node::Leaf(leaf);
                let right_node = Node::Leaf(LeafNode {
                    entries: right_entries,
                    next_leaf: left_next,
                });
                self.write_node(page_id, &left_node)?;
                self.write_node(right_page, &right_node)?;
                Ok(Some(SplitInfo {
                    separator,
                    right_page_id: right_page,
                }))
            }
            Node::Internal(mut internal) => {
                let (child, insert_at) = child_for_key(&internal, &key);
                let child_split = self.insert_into(child, key, rid)?;
                let Some(split) = child_split else {
                    return Ok(None);
                };

                internal.keys.insert(
                    insert_at,
                    InternalEntry {
                        key: split.separator,
                        child: split.right_page_id,
                    },
                );

                let node = Node::Internal(internal.clone());
                if self.encoded_node_len(&node)? <= PAGE_SIZE {
                    self.write_node(page_id, &node)?;
                    return Ok(None);
                }

                if internal.keys.len() < 2 {
                    return Err(RuseDbError::RecordTooLarge {
                        size: self.encoded_node_len(&node)?,
                    });
                }
                let mid = internal.keys.len() / 2;
                let right_keys = internal.keys.split_off(mid + 1);
                let separator_entry = internal.keys.pop().ok_or(RuseDbError::Corruption(
                    "internal split lost separator entry".to_string(),
                ))?;
                let separator = separator_entry.key;
                let right_first_child = separator_entry.child;

                let right_page = self.allocate_page()?;
                let left_node = Node::Internal(internal);
                let right_node = Node::Internal(InternalNode {
                    first_child: right_first_child,
                    keys: right_keys,
                });
                self.write_node(page_id, &left_node)?;
                self.write_node(right_page, &right_node)?;
                Ok(Some(SplitInfo {
                    separator,
                    right_page_id: right_page,
                }))
            }
        }
    }

    fn find_leaf_page(&mut self, mut page_id: u32, key: &IndexKey) -> Result<u32> {
        loop {
            match self.read_node(page_id)? {
                Node::Leaf(_) => return Ok(page_id),
                Node::Internal(internal) => {
                    page_id = child_for_key(&internal, key).0;
                }
            }
        }
    }

    fn find_leftmost_leaf(&mut self, mut page_id: u32) -> Result<u32> {
        loop {
            match self.read_node(page_id)? {
                Node::Leaf(_) => return Ok(page_id),
                Node::Internal(internal) => {
                    page_id = internal.first_child;
                }
            }
        }
    }

    fn find_leaf_with_path(
        &mut self,
        mut page_id: u32,
        key: &IndexKey,
    ) -> Result<(u32, Vec<ParentFrame>)> {
        let mut path = Vec::new();
        loop {
            match self.read_node(page_id)? {
                Node::Leaf(_) => return Ok((page_id, path)),
                Node::Internal(internal) => {
                    let (child, child_index) = child_for_key(&internal, key);
                    path.push(ParentFrame {
                        page_id,
                        child_index,
                    });
                    page_id = child;
                }
            }
        }
    }

    fn rebalance_after_delete(
        &mut self,
        leaf_page_id: u32,
        mut path: Vec<ParentFrame>,
    ) -> Result<()> {
        let mut current_page_id = leaf_page_id;
        while let Some(frame) = path.pop() {
            let node = self.read_node(current_page_id)?;
            if !is_underfull_non_root(&node) {
                return Ok(());
            }

            let parent_node = self.read_node(frame.page_id)?;
            let Node::Internal(mut parent) = parent_node else {
                return Err(RuseDbError::Corruption(format!(
                    "expected internal parent page {}, got non-internal",
                    frame.page_id
                )));
            };

            let child_count = parent.keys.len() + 1;
            if frame.child_index >= child_count {
                return Err(RuseDbError::Corruption(format!(
                    "child index {} out of range for parent page {} with {} children",
                    frame.child_index, frame.page_id, child_count
                )));
            }

            let left_sibling_page = if frame.child_index > 0 {
                Some(child_page_at(&parent, frame.child_index - 1)?)
            } else {
                None
            };
            let right_sibling_page = if frame.child_index + 1 < child_count {
                Some(child_page_at(&parent, frame.child_index + 1)?)
            } else {
                None
            };

            match node {
                Node::Leaf(mut leaf) => {
                    if let Some(left_page_id) = left_sibling_page {
                        let left_node = self.read_node(left_page_id)?;
                        let Node::Leaf(mut left_leaf) = left_node else {
                            return Err(RuseDbError::Corruption(format!(
                                "expected leaf sibling at page {}",
                                left_page_id
                            )));
                        };
                        if left_leaf.entries.len() > 1 {
                            if let Some(borrowed) = left_leaf.entries.pop() {
                                leaf.entries.insert(0, borrowed);
                                self.write_node(left_page_id, &Node::Leaf(left_leaf))?;
                                self.write_node(current_page_id, &Node::Leaf(leaf))?;
                                return Ok(());
                            }
                        }
                    }

                    if let Some(right_page_id) = right_sibling_page {
                        let right_node = self.read_node(right_page_id)?;
                        let Node::Leaf(mut right_leaf) = right_node else {
                            return Err(RuseDbError::Corruption(format!(
                                "expected leaf sibling at page {}",
                                right_page_id
                            )));
                        };
                        if right_leaf.entries.len() > 1 {
                            let borrowed = right_leaf.entries.remove(0);
                            leaf.entries.push(borrowed);
                            self.write_node(right_page_id, &Node::Leaf(right_leaf))?;
                            self.write_node(current_page_id, &Node::Leaf(leaf))?;
                            return Ok(());
                        }
                    }

                    if let Some(left_page_id) = left_sibling_page {
                        let left_node = self.read_node(left_page_id)?;
                        let Node::Leaf(mut left_leaf) = left_node else {
                            return Err(RuseDbError::Corruption(format!(
                                "expected leaf sibling at page {}",
                                left_page_id
                            )));
                        };
                        left_leaf.entries.extend(leaf.entries);
                        left_leaf.next_leaf = leaf.next_leaf;
                        self.write_node(left_page_id, &Node::Leaf(left_leaf))?;
                        remove_child_pointer(&mut parent, frame.child_index)?;
                        self.write_node(frame.page_id, &Node::Internal(parent))?;
                    } else if let Some(right_page_id) = right_sibling_page {
                        let right_node = self.read_node(right_page_id)?;
                        let Node::Leaf(right_leaf) = right_node else {
                            return Err(RuseDbError::Corruption(format!(
                                "expected leaf sibling at page {}",
                                right_page_id
                            )));
                        };
                        leaf.entries.extend(right_leaf.entries);
                        leaf.next_leaf = right_leaf.next_leaf;
                        self.write_node(current_page_id, &Node::Leaf(leaf))?;
                        remove_child_pointer(&mut parent, frame.child_index + 1)?;
                        self.write_node(frame.page_id, &Node::Internal(parent))?;
                    } else {
                        current_page_id = frame.page_id;
                        continue;
                    }
                }
                Node::Internal(mut internal) => {
                    if let Some(left_page_id) = left_sibling_page {
                        let left_node = self.read_node(left_page_id)?;
                        let Node::Internal(mut left_internal) = left_node else {
                            return Err(RuseDbError::Corruption(format!(
                                "expected internal sibling at page {}",
                                left_page_id
                            )));
                        };
                        if left_internal.keys.len() > 1 {
                            let sep_index =
                                frame
                                    .child_index
                                    .checked_sub(1)
                                    .ok_or(RuseDbError::Corruption(
                                        "missing separator key for left rotation".to_string(),
                                    ))?;
                            let parent_sep = parent.keys.get(sep_index).ok_or(
                                RuseDbError::Corruption(format!(
                                    "missing separator key {} in parent {}",
                                    sep_index, frame.page_id
                                )),
                            )?;
                            let borrowed =
                                left_internal.keys.pop().ok_or(RuseDbError::Corruption(
                                    "left internal sibling unexpectedly empty".to_string(),
                                ))?;
                            let old_first_child = internal.first_child;
                            internal.first_child = borrowed.child;
                            internal.keys.insert(
                                0,
                                InternalEntry {
                                    key: parent_sep.key.clone(),
                                    child: old_first_child,
                                },
                            );
                            if let Some(parent_sep_mut) = parent.keys.get_mut(sep_index) {
                                parent_sep_mut.key = borrowed.key;
                            }
                            self.write_node(left_page_id, &Node::Internal(left_internal))?;
                            self.write_node(current_page_id, &Node::Internal(internal))?;
                            self.write_node(frame.page_id, &Node::Internal(parent))?;
                            return Ok(());
                        }
                    }

                    if let Some(right_page_id) = right_sibling_page {
                        let right_node = self.read_node(right_page_id)?;
                        let Node::Internal(mut right_internal) = right_node else {
                            return Err(RuseDbError::Corruption(format!(
                                "expected internal sibling at page {}",
                                right_page_id
                            )));
                        };
                        if right_internal.keys.len() > 1 {
                            let parent_sep = parent.keys.get(frame.child_index).ok_or(
                                RuseDbError::Corruption(format!(
                                    "missing separator key {} in parent {}",
                                    frame.child_index, frame.page_id
                                )),
                            )?;
                            let borrowed_first_child = right_internal.first_child;
                            let new_sep = right_internal.keys.remove(0);
                            internal.keys.push(InternalEntry {
                                key: parent_sep.key.clone(),
                                child: borrowed_first_child,
                            });
                            right_internal.first_child = new_sep.child;
                            if let Some(parent_sep_mut) = parent.keys.get_mut(frame.child_index) {
                                parent_sep_mut.key = new_sep.key;
                            }
                            self.write_node(right_page_id, &Node::Internal(right_internal))?;
                            self.write_node(current_page_id, &Node::Internal(internal))?;
                            self.write_node(frame.page_id, &Node::Internal(parent))?;
                            return Ok(());
                        }
                    }

                    if let Some(left_page_id) = left_sibling_page {
                        let left_node = self.read_node(left_page_id)?;
                        let Node::Internal(mut left_internal) = left_node else {
                            return Err(RuseDbError::Corruption(format!(
                                "expected internal sibling at page {}",
                                left_page_id
                            )));
                        };
                        let sep_index =
                            frame
                                .child_index
                                .checked_sub(1)
                                .ok_or(RuseDbError::Corruption(
                                    "missing separator key for internal merge".to_string(),
                                ))?;
                        let sep_key =
                            parent
                                .keys
                                .get(sep_index)
                                .ok_or(RuseDbError::Corruption(format!(
                                    "missing separator key {} in parent {}",
                                    sep_index, frame.page_id
                                )))?;
                        left_internal.keys.push(InternalEntry {
                            key: sep_key.key.clone(),
                            child: internal.first_child,
                        });
                        left_internal.keys.extend(internal.keys);
                        self.write_node(left_page_id, &Node::Internal(left_internal))?;
                        remove_child_pointer(&mut parent, frame.child_index)?;
                        self.write_node(frame.page_id, &Node::Internal(parent))?;
                    } else if let Some(right_page_id) = right_sibling_page {
                        let right_node = self.read_node(right_page_id)?;
                        let Node::Internal(right_internal) = right_node else {
                            return Err(RuseDbError::Corruption(format!(
                                "expected internal sibling at page {}",
                                right_page_id
                            )));
                        };
                        let sep_key =
                            parent
                                .keys
                                .get(frame.child_index)
                                .ok_or(RuseDbError::Corruption(format!(
                                    "missing separator key {} in parent {}",
                                    frame.child_index, frame.page_id
                                )))?;
                        internal.keys.push(InternalEntry {
                            key: sep_key.key.clone(),
                            child: right_internal.first_child,
                        });
                        internal.keys.extend(right_internal.keys);
                        self.write_node(current_page_id, &Node::Internal(internal))?;
                        remove_child_pointer(&mut parent, frame.child_index + 1)?;
                        self.write_node(frame.page_id, &Node::Internal(parent))?;
                    } else {
                        current_page_id = frame.page_id;
                        continue;
                    }
                }
            }

            current_page_id = frame.page_id;
        }
        Ok(())
    }

    fn normalize_root_after_delete(&mut self) -> Result<()> {
        loop {
            let Some(root_page_id) = self.root_page_id else {
                return Ok(());
            };
            match self.read_node(root_page_id)? {
                Node::Leaf(leaf) => {
                    if leaf.entries.is_empty() {
                        self.root_page_id = None;
                        self.write_meta()?;
                    }
                    return Ok(());
                }
                Node::Internal(internal) => {
                    if internal.keys.is_empty() {
                        self.root_page_id = Some(internal.first_child);
                        self.write_meta()?;
                        continue;
                    }
                    return Ok(());
                }
            }
        }
    }

    fn rebuild_all_separators(&mut self) -> Result<()> {
        let Some(root_page_id) = self.root_page_id else {
            return Ok(());
        };
        let _ = self.rebuild_separators_from(root_page_id)?;
        Ok(())
    }

    fn rebuild_separators_from(&mut self, page_id: u32) -> Result<Option<IndexKey>> {
        match self.read_node(page_id)? {
            Node::Leaf(leaf) => Ok(leaf.entries.first().map(|entry| entry.key.clone())),
            Node::Internal(mut internal) => {
                let child_count = internal.keys.len() + 1;
                let mut child_mins = Vec::with_capacity(child_count);
                for child_index in 0..child_count {
                    let child_page_id = child_page_at(&internal, child_index)?;
                    child_mins.push(self.rebuild_separators_from(child_page_id)?);
                }
                let first_min = child_mins.first().cloned().flatten();
                let Some(first_key) = first_min.clone() else {
                    return Ok(None);
                };
                for child_index in 1..child_count {
                    let min_key = child_mins
                        .get(child_index)
                        .and_then(|key| key.clone())
                        .ok_or(RuseDbError::Corruption(format!(
                            "internal page {} points to empty child at index {}",
                            page_id, child_index
                        )))?;
                    internal.keys[child_index - 1].key = min_key;
                }
                self.write_node(page_id, &Node::Internal(internal))?;
                Ok(Some(first_key))
            }
        }
    }

    fn allocate_page(&mut self) -> Result<u32> {
        let page_id = self.next_page_id;
        self.next_page_id = self.next_page_id.saturating_add(1);
        self.write_page(page_id, &[0u8; PAGE_SIZE])?;
        self.write_meta()?;
        Ok(page_id)
    }

    fn read_node(&mut self, page_id: u32) -> Result<Node> {
        let page = self.read_page(page_id)?;
        match page[0] {
            NODE_LEAF => decode_leaf(&page, self.key_kind),
            NODE_INTERNAL => decode_internal(&page, self.key_kind),
            other => Err(RuseDbError::Corruption(format!(
                "unknown node type {} at page {}",
                other, page_id
            ))),
        }
    }

    fn write_node(&mut self, page_id: u32, node: &Node) -> Result<()> {
        let bytes = match node {
            Node::Leaf(leaf) => encode_leaf(leaf, self.key_kind)?,
            Node::Internal(internal) => encode_internal(internal, self.key_kind)?,
        };
        self.write_page(page_id, &bytes)?;
        Ok(())
    }

    fn encoded_node_len(&self, node: &Node) -> Result<usize> {
        match node {
            Node::Leaf(leaf) => Ok(encoded_leaf_len(leaf, self.key_kind)?),
            Node::Internal(internal) => Ok(encoded_internal_len(internal, self.key_kind)?),
        }
    }

    fn read_page(&mut self, page_id: u32) -> Result<[u8; PAGE_SIZE]> {
        let offset = page_id as u64 * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        let mut page = [0u8; PAGE_SIZE];
        self.file.read_exact(&mut page)?;
        Ok(page)
    }

    fn write_page(&mut self, page_id: u32, bytes: &[u8; PAGE_SIZE]) -> Result<()> {
        let offset = page_id as u64 * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(bytes)?;
        self.file.flush()?;
        Ok(())
    }

    fn write_meta(&mut self) -> Result<()> {
        let mut page = [0u8; PAGE_SIZE];
        page[0..8].copy_from_slice(MAGIC);
        page[8] = self.key_kind.to_u8();
        let root = self.root_page_id.unwrap_or(0);
        page[9..13].copy_from_slice(&root.to_le_bytes());
        page[13..17].copy_from_slice(&self.next_page_id.to_le_bytes());
        self.write_page(META_PAGE_ID, &page)?;
        Ok(())
    }
}

fn child_for_key(internal: &InternalNode, key: &IndexKey) -> (u32, usize) {
    let mut child = internal.first_child;
    let mut idx = 0usize;
    while idx < internal.keys.len() {
        if key < &internal.keys[idx].key {
            break;
        }
        child = internal.keys[idx].child;
        idx += 1;
    }
    (child, idx)
}

fn child_page_at(internal: &InternalNode, child_index: usize) -> Result<u32> {
    if child_index == 0 {
        return Ok(internal.first_child);
    }
    internal
        .keys
        .get(child_index - 1)
        .map(|entry| entry.child)
        .ok_or(RuseDbError::Corruption(format!(
            "child index {} out of range (children={})",
            child_index,
            internal.keys.len() + 1
        )))
}

fn remove_child_pointer(internal: &mut InternalNode, child_index: usize) -> Result<()> {
    let child_count = internal.keys.len() + 1;
    if child_index >= child_count {
        return Err(RuseDbError::Corruption(format!(
            "cannot remove child {} from internal node with {} children",
            child_index, child_count
        )));
    }

    if child_index == 0 {
        if internal.keys.is_empty() {
            return Err(RuseDbError::Corruption(
                "cannot remove only child from internal node".to_string(),
            ));
        }
        let replacement = internal.keys.remove(0);
        internal.first_child = replacement.child;
        return Ok(());
    }

    internal.keys.remove(child_index - 1);
    Ok(())
}

fn is_underfull_non_root(node: &Node) -> bool {
    match node {
        Node::Leaf(leaf) => leaf.entries.is_empty(),
        Node::Internal(internal) => internal.keys.is_empty(),
    }
}

fn cmp_leaf_entry(entry: &LeafEntry, key: &IndexKey, rid: Rid) -> Ordering {
    match entry.key.cmp(key) {
        Ordering::Equal => (entry.rid.page_id, entry.rid.slot_id).cmp(&(rid.page_id, rid.slot_id)),
        ord => ord,
    }
}

fn encoded_key_len(key: &IndexKey, key_kind: IndexKeyKind) -> Result<usize> {
    match (key_kind, key) {
        (IndexKeyKind::Int, IndexKey::Int(_)) => Ok(8),
        (IndexKeyKind::Bool, IndexKey::Bool(_)) => Ok(1),
        (IndexKeyKind::Str, IndexKey::Str(v)) => {
            let len = v.len();
            let _ = u16::try_from(len).map_err(|_| RuseDbError::RecordTooLarge { size: len })?;
            Ok(2 + len)
        }
        _ => Err(RuseDbError::Corruption(
            "index key variant does not match index kind".to_string(),
        )),
    }
}

fn write_key(dst: &mut Vec<u8>, key: &IndexKey, key_kind: IndexKeyKind) -> Result<()> {
    match (key_kind, key) {
        (IndexKeyKind::Int, IndexKey::Int(v)) => dst.extend_from_slice(&v.to_le_bytes()),
        (IndexKeyKind::Bool, IndexKey::Bool(v)) => dst.push(u8::from(*v)),
        (IndexKeyKind::Str, IndexKey::Str(v)) => {
            let bytes = v.as_bytes();
            let len = u16::try_from(bytes.len())
                .map_err(|_| RuseDbError::RecordTooLarge { size: bytes.len() })?;
            dst.extend_from_slice(&len.to_le_bytes());
            dst.extend_from_slice(bytes);
        }
        _ => {
            return Err(RuseDbError::Corruption(
                "index key variant does not match index kind".to_string(),
            ));
        }
    }
    Ok(())
}

fn read_key(src: &[u8], cursor: &mut usize, key_kind: IndexKeyKind) -> Result<IndexKey> {
    Ok(match key_kind {
        IndexKeyKind::Int => {
            let bytes = read_slice(src, cursor, 8)?;
            IndexKey::Int(i64::from_le_bytes(bytes.try_into().unwrap()))
        }
        IndexKeyKind::Bool => {
            let bytes = read_slice(src, cursor, 1)?;
            match bytes[0] {
                0 => IndexKey::Bool(false),
                1 => IndexKey::Bool(true),
                other => {
                    return Err(RuseDbError::Corruption(format!(
                        "invalid bool key byte {}",
                        other
                    )));
                }
            }
        }
        IndexKeyKind::Str => {
            let len_bytes = read_slice(src, cursor, 2)?;
            let len = u16::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
            let bytes = read_slice(src, cursor, len)?;
            let value = String::from_utf8(bytes.to_vec()).map_err(|err| {
                RuseDbError::Corruption(format!("invalid utf8 in index key: {err}"))
            })?;
            IndexKey::Str(value)
        }
    })
}

fn encoded_leaf_len(leaf: &LeafNode, key_kind: IndexKeyKind) -> Result<usize> {
    let mut len = 1 + 2 + 4;
    for entry in &leaf.entries {
        len += encoded_key_len(&entry.key, key_kind)? + 4 + 2;
    }
    Ok(len)
}

fn encoded_internal_len(internal: &InternalNode, key_kind: IndexKeyKind) -> Result<usize> {
    let mut len = 1 + 2 + 4;
    for entry in &internal.keys {
        len += encoded_key_len(&entry.key, key_kind)? + 4;
    }
    Ok(len)
}

fn encode_leaf(leaf: &LeafNode, key_kind: IndexKeyKind) -> Result<[u8; PAGE_SIZE]> {
    let needed = encoded_leaf_len(leaf, key_kind)?;
    if needed > PAGE_SIZE {
        return Err(RuseDbError::RecordTooLarge { size: needed });
    }

    let mut out = Vec::with_capacity(PAGE_SIZE);
    out.push(NODE_LEAF);
    out.extend_from_slice(&(leaf.entries.len() as u16).to_le_bytes());
    out.extend_from_slice(&leaf.next_leaf.unwrap_or(0).to_le_bytes());
    for entry in &leaf.entries {
        write_key(&mut out, &entry.key, key_kind)?;
        out.extend_from_slice(&entry.rid.page_id.to_le_bytes());
        out.extend_from_slice(&entry.rid.slot_id.to_le_bytes());
    }
    while out.len() < PAGE_SIZE {
        out.push(0);
    }
    Ok(out.try_into().unwrap())
}

fn encode_internal(internal: &InternalNode, key_kind: IndexKeyKind) -> Result<[u8; PAGE_SIZE]> {
    let needed = encoded_internal_len(internal, key_kind)?;
    if needed > PAGE_SIZE {
        return Err(RuseDbError::RecordTooLarge { size: needed });
    }

    let mut out = Vec::with_capacity(PAGE_SIZE);
    out.push(NODE_INTERNAL);
    out.extend_from_slice(&(internal.keys.len() as u16).to_le_bytes());
    out.extend_from_slice(&internal.first_child.to_le_bytes());
    for entry in &internal.keys {
        write_key(&mut out, &entry.key, key_kind)?;
        out.extend_from_slice(&entry.child.to_le_bytes());
    }
    while out.len() < PAGE_SIZE {
        out.push(0);
    }
    Ok(out.try_into().unwrap())
}

fn decode_leaf(page: &[u8; PAGE_SIZE], key_kind: IndexKeyKind) -> Result<Node> {
    let count = u16::from_le_bytes(page[1..3].try_into().unwrap()) as usize;
    let next_raw = u32::from_le_bytes(page[3..7].try_into().unwrap());
    let next_leaf = if next_raw == 0 { None } else { Some(next_raw) };
    let mut cursor = 7usize;
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let key = read_key(page, &mut cursor, key_kind)?;
        let page_id_bytes = read_slice(page, &mut cursor, 4)?;
        let slot_id_bytes = read_slice(page, &mut cursor, 2)?;
        entries.push(LeafEntry {
            key,
            rid: Rid {
                page_id: u32::from_le_bytes(page_id_bytes.try_into().unwrap()),
                slot_id: u16::from_le_bytes(slot_id_bytes.try_into().unwrap()),
            },
        });
    }
    Ok(Node::Leaf(LeafNode { entries, next_leaf }))
}

fn decode_internal(page: &[u8; PAGE_SIZE], key_kind: IndexKeyKind) -> Result<Node> {
    let count = u16::from_le_bytes(page[1..3].try_into().unwrap()) as usize;
    let first_child = u32::from_le_bytes(page[3..7].try_into().unwrap());
    let mut cursor = 7usize;
    let mut keys = Vec::with_capacity(count);
    for _ in 0..count {
        let key = read_key(page, &mut cursor, key_kind)?;
        let child_bytes = read_slice(page, &mut cursor, 4)?;
        keys.push(InternalEntry {
            key,
            child: u32::from_le_bytes(child_bytes.try_into().unwrap()),
        });
    }
    Ok(Node::Internal(InternalNode { first_child, keys }))
}

fn read_slice<'a>(src: &'a [u8], cursor: &mut usize, len: usize) -> Result<&'a [u8]> {
    let end = *cursor + len;
    if end > src.len() {
        return Err(RuseDbError::Corruption(
            "index page payload truncated".to_string(),
        ));
    }
    let out = &src[*cursor..end];
    *cursor = end;
    Ok(out)
}

fn range_contains(
    key: &IndexKey,
    lower: Option<&(IndexKey, bool)>,
    upper: Option<&(IndexKey, bool)>,
) -> bool {
    if let Some((lower_key, inclusive)) = lower {
        if *inclusive {
            if key < lower_key {
                return false;
            }
        } else if key <= lower_key {
            return false;
        }
    }

    if let Some((upper_key, inclusive)) = upper {
        if *inclusive {
            if key > upper_key {
                return false;
            }
        } else if key >= upper_key {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::{thread, time};

    use crate::Rid;

    use super::{IndexKey, IndexKeyKind, OrderedIndex};

    fn unique_index_dir(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let tick = time::SystemTime::now()
            .duration_since(time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        path.push(format!(
            "rusedb-ordered-index-{name}-{}-{tick}",
            std::process::id()
        ));
        fs::create_dir_all(&path).unwrap();
        path
    }

    fn cleanup_dir(path: &Path) {
        for _ in 0..5 {
            if fs::remove_dir_all(path).is_ok() {
                return;
            }
            thread::sleep(time::Duration::from_millis(50));
        }
    }

    fn rid_for(value: u32) -> Rid {
        Rid {
            page_id: value + 1,
            slot_id: (value % u16::MAX as u32) as u16,
        }
    }

    #[test]
    fn remove_rebalances_and_survives_restart() {
        let dir = unique_index_dir("remove-rebalance");
        let index_path = dir.join("index.idx");

        {
            let mut index = OrderedIndex::open(&index_path, IndexKeyKind::Int).unwrap();
            for value in 0..1200u32 {
                index
                    .insert(IndexKey::Int(value as i64), rid_for(value))
                    .unwrap();
            }

            for value in 0..1200u32 {
                if value == 777 {
                    continue;
                }
                index
                    .remove(&IndexKey::Int(value as i64), rid_for(value))
                    .unwrap();
            }

            assert_eq!(
                index.search_eq(&IndexKey::Int(777)).unwrap(),
                vec![rid_for(777)]
            );
            assert_eq!(
                index
                    .search_range(
                        Some((IndexKey::Int(700), true)),
                        Some((IndexKey::Int(800), true))
                    )
                    .unwrap(),
                vec![rid_for(777)]
            );
        }

        {
            let mut index = OrderedIndex::open(&index_path, IndexKeyKind::Int).unwrap();
            assert_eq!(
                index.search_eq(&IndexKey::Int(777)).unwrap(),
                vec![rid_for(777)]
            );
            assert!(index.search_eq(&IndexKey::Int(10)).unwrap().is_empty());
        }

        cleanup_dir(&dir);
    }

    #[test]
    fn remove_all_entries_resets_tree_and_allows_reinsert() {
        let dir = unique_index_dir("remove-all");
        let index_path = dir.join("index.idx");

        {
            let mut index = OrderedIndex::open(&index_path, IndexKeyKind::Int).unwrap();
            for value in 0..400u32 {
                index
                    .insert(IndexKey::Int(value as i64), rid_for(value))
                    .unwrap();
            }
            for value in 0..400u32 {
                index
                    .remove(&IndexKey::Int(value as i64), rid_for(value))
                    .unwrap();
            }

            assert!(index.search_range(None, None).unwrap().is_empty());

            let marker_rid = rid_for(10_000);
            index.insert(IndexKey::Int(42), marker_rid).unwrap();
            assert_eq!(
                index.search_eq(&IndexKey::Int(42)).unwrap(),
                vec![marker_rid]
            );
        }

        {
            let mut index = OrderedIndex::open(&index_path, IndexKeyKind::Int).unwrap();
            let marker_rid = rid_for(10_000);
            assert_eq!(
                index.search_eq(&IndexKey::Int(42)).unwrap(),
                vec![marker_rid]
            );
            assert!(index.search_eq(&IndexKey::Int(1)).unwrap().is_empty());
        }

        cleanup_dir(&dir);
    }
}
