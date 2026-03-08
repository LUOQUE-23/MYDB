use rusedb_storage::HeapFile;
use std::path::{Path, PathBuf};
use std::{fs, thread, time};

fn unique_test_dir(name: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let tick = time::SystemTime::now()
        .duration_since(time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    path.push(format!("rusedb-{name}-{}-{tick}", std::process::id()));
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

#[test]
fn heap_records_survive_restart() {
    let dir = unique_test_dir("restart");
    let data_path = dir.join("heap.db");

    let mut expected = Vec::new();
    {
        let mut heap = HeapFile::open(&data_path).unwrap();
        for i in 0..10_000u32 {
            let payload = format!("row-{i:05}").into_bytes();
            let rid = heap.insert_record(&payload).unwrap();
            expected.push((rid, payload));
        }
        heap.sync().unwrap();
    }

    {
        let mut reopened = HeapFile::open(&data_path).unwrap();
        for (idx, (rid, payload)) in expected.iter().enumerate() {
            if idx % 997 == 0 {
                let actual = reopened.get_record(*rid).unwrap();
                assert_eq!(&actual, payload);
            }
        }
    }

    cleanup_dir(&dir);
}

#[test]
fn delete_reuses_slot_in_same_page() {
    let dir = unique_test_dir("slot-reuse");
    let data_path = dir.join("heap.db");
    let mut heap = HeapFile::open(&data_path).unwrap();

    let rid_a = heap.insert_record(b"alpha").unwrap();
    let _rid_b = heap.insert_record(b"beta").unwrap();
    heap.delete_record(rid_a).unwrap();

    let rid_c = heap.insert_record(b"charlie").unwrap();
    assert_eq!(rid_c.page_id, rid_a.page_id);
    assert_eq!(rid_c.slot_id, rid_a.slot_id);

    cleanup_dir(&dir);
}
