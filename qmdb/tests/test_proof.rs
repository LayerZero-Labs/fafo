use std::{sync::Arc, thread};

use parking_lot::RwLock;
use qmdb::{
    config::Config,
    def::{ENTRY_BASE_LENGTH, SHARD_COUNT, TWIG_MASK},
    entryfile::entry,
    merkletree::{
        check,
        helpers::build_test_tree,
        proof::{self, check_proof, ProofPath},
    },
    tasks::{taskid::join_task_id, TasksManager},
    test_helper::task_builder::{SingleCsTask, TaskBuilder},
    utils::{byte0_to_shard_id, hasher},
    AdsCore, AdsWrap, ADS,
};

fn check_equal(pp: &ProofPath, other: &ProofPath) -> String {
    if pp.upper_path.len() != other.upper_path.len() {
        return String::from("UpperPath's length not equal");
    }
    if pp.serial_num != other.serial_num {
        return String::from("SerialNum not equal");
    }
    if pp.left_of_twig[0].self_hash != other.left_of_twig[0].self_hash {
        return String::from("LeftOfTwig.SelfHash not equal");
    }
    for i in 0..11 {
        if pp.left_of_twig[i].peer_hash != other.left_of_twig[i].peer_hash {
            return String::from("LeftOfTwig.PeerHash not equal");
        }
        if pp.left_of_twig[i].peer_at_left != other.left_of_twig[i].peer_at_left {
            return format!("LeftOfTwig.PeerAtLeft[{}] not equal", i);
        }
    }
    if pp.right_of_twig[0].self_hash != other.right_of_twig[0].self_hash {
        return String::from("RightOfTwig.SelfHash not equal");
    }
    for i in 0..3 {
        if pp.right_of_twig[i].peer_hash != other.right_of_twig[i].peer_hash {
            return String::from("RightOfTwig.PeerHash not equal");
        }
        if pp.right_of_twig[i].peer_at_left != other.right_of_twig[i].peer_at_left {
            return String::from("RightOfTwig.PeerAtLeft not equal");
        }
    }
    for i in 0..pp.upper_path.len() {
        if pp.upper_path[i].peer_hash != other.upper_path[i].peer_hash {
            return String::from("UpperPath.PeerHash not equal");
        }
        if pp.upper_path[i].peer_at_left != other.upper_path[i].peer_at_left {
            return String::from("UpperPath.PeerAtLeft not equal");
        }
    }
    if pp.root != other.root {
        return String::from("Root not equal");
    }
    String::from("")
}

#[test]
fn test_tree_proof() {
    let temp_dir = ::tempfile::Builder::new()
        .prefix("DataTree-proof")
        .tempdir()
        .unwrap();

    let deact_sn_list: Vec<u64> = (0..2048)
        .chain(vec![5000, 5500, 5700, 5813, 6001])
        .collect();

    let (mut tree, _, _, _) = build_test_tree(
        temp_dir.path().to_str().unwrap(),
        &deact_sn_list,
        TWIG_MASK as i32 * 4,
        1600,
    );
    let n_list = tree.flush_files(0, 0);
    let n_list = tree.upper_tree.evict_twigs(n_list, 0, 0);

    tree.upper_tree
        .sync_upper_nodes(n_list, tree.youngest_twig_id);
    check::check_hash_consistency(&tree);

    let max_sn = TWIG_MASK as i32 * 4 + 1600;
    for i in 0..max_sn {
        let mut proof_path = tree.get_proof(i as u64).unwrap();
        proof_path.check(false).unwrap();

        let bz = proof_path.to_bytes();
        let mut path2 = proof::bytes_to_proof_path(&bz).unwrap();
        let r = check_equal(&proof_path, &path2);
        assert_eq!(r, String::from(""));
        path2.check(true).unwrap();
    }

    // check null entries
    let _max_sn = max_sn + 452;
    let mut bz = [0u8; ENTRY_BASE_LENGTH + 8];
    let null_hash = entry::null_entry(&mut bz[..]).hash();
    for i in max_sn.._max_sn {
        let mut proof_path = tree.get_proof(i as u64).unwrap();
        assert_eq!(proof_path.left_of_twig[0].self_hash, null_hash);
        proof_path.check(false).unwrap();

        let bz = proof_path.to_bytes();
        let mut path2 = proof::bytes_to_proof_path(&bz).unwrap();
        let r = check_equal(&proof_path, &path2);
        assert_eq!(r, String::from(""));
        path2.check(true).unwrap();
    }
}

#[test]
fn test_tree_get_proof() {
    let temp_dir = ::tempfile::Builder::new()
        .prefix("test_tree_get_proof")
        .tempdir()
        .unwrap();

    let mut config = Config::from_dir(temp_dir.path().to_str().unwrap());
    AdsCore::init_dir(&config);
    config.set_with_twig_file(true);

    let mut ads = Box::new(AdsWrap::new(&config));
    let ads_p = &mut *ads as *mut AdsWrap<SingleCsTask>;

    let key = b"k12";
    let hash = hasher::hash(key);
    let shard_id = byte0_to_shard_id(hash[0]);

    let handle = thread::spawn(move || {
        if SHARD_COUNT == 16 {
            let mut proof = ads.get_proof(shard_id, 4096).unwrap();
            assert_eq!(
                proof.left_of_twig[0].self_hash,
                [
                    93, 178, 212, 56, 37, 103, 172, 205, 255, 184, 39, 231, 94, 228, 14, 210, 209,
                    165, 122, 253, 187, 100, 5, 13, 55, 169, 246, 181, 247, 201, 159, 152
                ]
            );
            check_proof(&mut proof).unwrap();
        } else {
            let mut proof = ads.get_proof(shard_id, 4096 * 2).unwrap();
            assert_eq!(
                proof.left_of_twig[0].self_hash,
                [
                    178, 204, 221, 231, 195, 49, 52, 140, 46, 37, 231, 156, 110, 125, 145, 158,
                    122, 105, 115, 96, 110, 38, 173, 198, 150, 249, 101, 216, 249, 137, 186, 163
                ]
            );
            check_proof(&mut proof).unwrap();
        }
    });

    unsafe {
        let ads = &mut (*ads_p);
        let task_id = join_task_id(1, 0, true);
        ads.start_block(
            1,
            Arc::new(TasksManager::new(
                vec![RwLock::new(Some(
                    TaskBuilder::new().create(key, b"v1").build(),
                ))],
                1,
            )),
        );
        let shared_ads = ads.get_shared();
        shared_ads.insert_extra_data(1, "".to_owned());
        shared_ads.add_task(task_id);
    }
    handle.join().unwrap();
}

#[test]
fn test_tree_get_proof_err() {
    let temp_dir = ::tempfile::Builder::new()
        .prefix("test_tree_get_proof_err")
        .tempdir()
        .unwrap();

    let config = Config::from_dir(temp_dir.path().to_str().unwrap());
    AdsCore::init_dir(&config);

    let mut ads = Box::new(AdsWrap::new(&config));
    let ads_p = &mut *ads as *mut AdsWrap<SingleCsTask>;

    let handle = thread::spawn(move || {
        let proof = ads.get_proof(5, 4095);
        assert!(proof.is_err());
        if let Err(e) = proof {
            assert_eq!(e, format!("get proof failed: \"twig_file is empty\""));
        }
    });

    unsafe {
        let ads = &mut (*ads_p);
        let task_id = join_task_id(1, 0, true);
        ads.start_block(
            1,
            Arc::new(TasksManager::new(
                vec![RwLock::new(Some(
                    TaskBuilder::new().create(b"k12", b"v1").build(),
                ))],
                1,
            )),
        );
        let shared_ads = ads.get_shared();
        shared_ads.insert_extra_data(1, "".to_owned());
        shared_ads.add_task(task_id);
    }
    handle.join().unwrap();
}
