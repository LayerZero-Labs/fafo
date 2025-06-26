use codedb::CodeDB;
use exepipe::context::block_context::BlockContext;
use exepipe::dispatcher::def::BATCH_SIZE;
use exepipe::exetask::{ExeTask, Tx};
use exepipe::test_helper::MockADS;
use exepipe::Block;
use exepipe_common::acc_data::decode_account_info_from_bz;
use exepipe_common::statecache::StateCache;
use parking_lot::RwLock;
use qmdb::entryfile::readbuf::ReadBuf;
use qmdb::{tasks::TasksManager, utils::hasher};

use revm::context::transaction::{AccessList, AccessListItem};
use revm::context::{TransactTo, TxEnv};
use revm::primitives::{hex::FromHex, keccak256, ruint::Uint, Address, U256};
use revm::primitives::{Bytes, B256};
use revm::state::{AccountInfo, Bytecode};
use std::collections::HashMap;
use std::sync::Arc;

fn check_account_info(
    block_ctx: &BlockContext<MockADS>,
    eoa_addr: &Address,
    balance: u128,
    nonce: u64,
) {
    let mut buf = ReadBuf::new();
    block_ctx
        .get_curr_state()
        .lookup_value(&hasher::hash(eoa_addr.to_vec().as_slice()), &mut buf);
    let acc = decode_account_info_from_bz(buf.as_slice());
    assert_eq!(acc.balance, U256::from(balance));
    assert_eq!(acc.nonce, nonce);
}

fn create_contract_call(
    dir: &str,
    eoa_addr: Address,
    ca_addr: Address,
    value: Uint<256, 4>,
    access_list: Vec<AccessListItem>,
    coinbase: Address,
    has_bytecode: bool,
) -> BlockContext<MockADS> {
    let bc_hex = "0x608060405234801561001057600080fd5b50600436106100415760003560e01c806361bc221a146100465780636299a6ef14610064578063ab470f0514610080575b600080fd5b61004e61009e565b60405161005b91906100e0565b60405180910390f35b61007e6004803603810190610079919061012c565b6100a4565b005b6100886100bf565b604051610095919061019a565b60405180910390f35b60005481565b806000808282546100b591906101e4565b9250508190555050565b600033905090565b6000819050919050565b6100da816100c7565b82525050565b60006020820190506100f560008301846100d1565b92915050565b600080fd5b610109816100c7565b811461011457600080fd5b50565b60008135905061012681610100565b92915050565b600060208284031215610142576101416100fb565b5b600061015084828501610117565b91505092915050565b600073ffffffffffffffffffffffffffffffffffffffff82169050919050565b600061018482610159565b9050919050565b61019481610179565b82525050565b60006020820190506101af600083018461018b565b92915050565b7f4e487b7100000000000000000000000000000000000000000000000000000000600052601160045260246000fd5b60006101ef826100c7565b91506101fa836100c7565b925082820190508281121560008312168382126000841215161715610222576102216101b5565b5b9291505056fea26469706673582212205d20d6227111b55922ff75f33c5a4680a5d9db8c9f9bbd6c0336f287e7174aea64736f6c63430008140033";
    let calldata = "0x6299a6ef0000000000000000000000000000000000000000000000000000000000000003";
    let bc_bytes = Bytes::from_hex(bc_hex).unwrap();
    let bytecode = Bytecode::new_raw(bc_bytes.clone());
    let bc_hash = bytecode.hash_slow();

    let eoa_info = AccountInfo {
        nonce: 0,
        balance: U256::from(1_000_000_000_000_000_000u128),
        code_hash: keccak256(Bytes::new()),
        code: None,
    };

    let mut ads = MockADS::new();
    let _ = ads.add_account(&eoa_addr, &eoa_info);
    let _ = ads.add_account(&coinbase, &AccountInfo::default());

    let mut contract_info = AccountInfo {
        nonce: 0,
        balance: U256::ZERO,
        code_hash: bc_hash.into(),
        code: Option::None,
    };
    let code_db = Arc::new(CodeDB::new(dir, 0, 256 * 16, 1024 * 16));
    if has_bytecode {
        code_db.append(&ca_addr, bytecode.clone(), 0).unwrap();
        code_db.flush().unwrap();
        contract_info.code = Option::Some(bytecode);
    }
    let _ = ads.add_account(&ca_addr, &contract_info);

    let mut tx_env = TxEnv::default();
    tx_env.caller = eoa_addr;
    tx_env.kind = TransactTo::Call(ca_addr);
    tx_env.data = Bytes::from_hex(calldata).unwrap();
    tx_env.value = value;
    tx_env.access_list = AccessList::from(access_list);
    tx_env.gas_price = 2;
    tx_env.gas_limit = 1_000_000_000u64 + (1 << 48); // 1 << 48 is minimum create slot dust

    let tx_list = vec![Box::new(Tx {
        tx_hash: B256::new([0u8; 32]),
        crw_sets: None,
        env: tx_env,
    })];

    let task = ExeTask::new(tx_list);

    let tasks = vec![RwLock::new(Some(task)), RwLock::new(None)];

    let mut block_ctx = BlockContext::new(ads, code_db, HashMap::new(), None);
    let tasksmanager = TasksManager::new(tasks, 1);
    let mut blk = Block::default();
    blk.env.beneficiary = coinbase;
    block_ctx.start_new_block(
        Arc::new(tasksmanager),
        blk,
        Arc::new(StateCache::new()),
        BATCH_SIZE,
    );
    block_ctx
}

fn create_transfer(
    dir: &str,
    from: Address,
    to_address: Address,
    _init_balance: u128,
    coinbase: Address,
    add_from_to_ads: bool,
    add_coinbase_to_ads: bool,
) -> BlockContext<MockADS> {
    let mut ads = MockADS::new();

    if add_from_to_ads {
        let init_balance = U256::from(_init_balance);
        let from_account_info = AccountInfo {
            nonce: 0,
            balance: init_balance,
            code_hash: keccak256(Bytes::new()),
            code: None,
        };
        ads.add_account(&from, &from_account_info);
    }

    if add_coinbase_to_ads {
        let coinbase_account_info = AccountInfo {
            nonce: 0,
            balance: U256::from(0),
            code_hash: keccak256(Bytes::new()),
            code: None,
        };
        ads.add_account(&coinbase, &coinbase_account_info);
    }

    let mut tx_env: TxEnv = TxEnv::default();
    if add_from_to_ads {
        // only for test
        tx_env.gas_price = 2;
        tx_env.gas_limit = 500_000_000_000_000_000u64;
        tx_env.value = U256::from(1);
    }
    tx_env.caller = from;
    tx_env.kind = TransactTo::Call(to_address);
    tx_env.data = Bytes::new();
    tx_env.access_list = AccessList::default();
    let tx_list = vec![
        Box::new(Tx {
            tx_hash: B256::new([0u8; 32]),
            crw_sets: None,
            env: tx_env.clone(),
        }),
        Box::new(Tx {
            tx_hash: B256::new([0u8; 32]),
            crw_sets: None,
            env: tx_env,
        }),
    ];
    let task = ExeTask::new(tx_list);
    let tasks = vec![RwLock::new(Some(task)), RwLock::new(None)];
    let code_db = Arc::new(CodeDB::new(dir, 0, 256 * 16, 1024 * 16));
    let mut block_ctx = BlockContext::new(ads, code_db, HashMap::new(), None);
    let tasksmanager = TasksManager::new(tasks, 1);
    let mut blk = Block::default();
    blk.env.beneficiary = coinbase;
    block_ctx.start_new_block(
        Arc::new(tasksmanager),
        blk,
        Arc::new(StateCache::new()),
        BATCH_SIZE,
    );
    block_ctx
}

#[cfg(test)]
mod tests {
    use std::ops::Deref;

    use exepipe_common::def::NEW_ADDR_BURN_GAS;
    use revm::primitives::{address, Address, U256};
    use serial_test::serial;

    use crate::{check_account_info, create_contract_call, create_transfer};

    #[test]
    #[ignore = "WIP"]
    #[serial]
    fn test_transfer() {
        let temp_dir = tempfile::Builder::new()
            .prefix("test_context")
            .tempdir()
            .unwrap();

        let from = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let to_address = address!("00000000000000000000000000000000000000a3");
        let coinbase = address!("000000000000000000000000000000000000cbcb");
        let init_balance = 1_000_000_000_000_000_000u128 + 2 * (2 * 21000 + 1);

        let block_ctx = create_transfer(
            temp_dir.path().to_str().unwrap(),
            from,
            to_address,
            init_balance,
            coinbase,
            true,
            true,
        );

        block_ctx.warmup(0);
        block_ctx.execute_task(0);
        block_ctx.end_block();

        let result0 = block_ctx.read_result(0);
        for r in result0.deref().as_ref().unwrap() {
            assert!(r.as_ref().unwrap().result.is_success());
        }
        check_account_info(
            &block_ctx,
            &from,
            1_000_000_000_000_000_000u128 - NEW_ADDR_BURN_GAS as u128 * 2,
            2,
        );
        check_account_info(&block_ctx, &to_address, 2u128, 0);
        check_account_info(&block_ctx, &coinbase, 84000u128, 0);
    }

    #[test]
    #[ignore = "WIP"]
    #[serial]
    fn test_transfer_when_coinbase_not_existed() {
        let temp_dir = tempfile::Builder::new()
            .prefix("test_context")
            .tempdir()
            .unwrap();

        let from = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let to_address = address!("00000000000000000000000000000000000000a3");
        let coinbase = address!("000000000000000000000000000000000000cbcb");
        let init_balance = 1_000_000_000_000_000_000u128 + 2 * (2 * 21000 + 1);

        let block_ctx = create_transfer(
            temp_dir.path().to_str().unwrap(),
            from,
            to_address,
            init_balance,
            coinbase,
            true,
            false,
        );

        block_ctx.warmup(0);
        block_ctx.execute_task(0);
        block_ctx.end_block();

        let result0 = block_ctx.read_result(0);
        for r in result0.deref().as_ref().unwrap() {
            assert!(r.as_ref().unwrap().result.is_success());
        }
        check_account_info(
            &block_ctx,
            &from,
            1_000_000_000_000_000_000u128 - NEW_ADDR_BURN_GAS as u128 * 2,
            2,
        );
        check_account_info(&block_ctx, &to_address, 2u128, 0);
        check_account_info(&block_ctx, &coinbase, 0, 0);

        let result1 = block_ctx.read_result(1);
        let s = result1.deref().as_ref().unwrap()[0].as_ref().unwrap_err();
        assert!(format!("{:?}", s).contains("Cannot find coinbase 0x000000000000000000000000000000000000cbCb account then 84000 gas fee will be burn"));
    }

    #[test]
    #[ignore = "WIP"]
    #[serial]
    fn test_transfer_to_coinbase() {
        let temp_dir = tempfile::Builder::new()
            .prefix("test_context")
            .tempdir()
            .unwrap();

        let from = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let to_address = Address::ZERO;
        let coinbase = to_address;
        let init_balance = 1_000_000_000_000_000_000u128 + 2 * (2 * 21000 + 1);

        let block_ctx = create_transfer(
            temp_dir.path().to_str().unwrap(),
            from,
            to_address,
            init_balance,
            coinbase,
            true,
            true,
        );

        block_ctx.warmup(0);
        block_ctx.execute_task(0);
        block_ctx.end_block();

        let result0 = block_ctx.read_result(0);
        for r in result0.deref().as_ref().unwrap() {
            assert!(r.as_ref().unwrap().result.is_success());
        }
        check_account_info(&block_ctx, &from, 1_000_000_000_000_000_000u128, 2);
        check_account_info(&block_ctx, &coinbase, 84002u128, 0);
    }

    #[test]
    #[ignore = "WIP"]
    #[serial]
    fn test_error_when_lock_of_fund_for_max_fee() {
        let temp_dir = tempfile::Builder::new()
            .prefix("test_context")
            .tempdir()
            .unwrap();

        let from = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let to_address = address!("00000000000000000000000000000000000000a3");
        let coinbase = address!("000000000000000000000000000000000000cbcb");

        let init_balance = 1_000_000_000_000_000_000u128;

        let block_ctx = create_transfer(
            temp_dir.path().to_str().unwrap(),
            from,
            to_address,
            init_balance,
            coinbase,
            true,
            true,
        );

        block_ctx.warmup(0);
        block_ctx.execute_task(0);
        block_ctx.end_block();

        let r = block_ctx.read_result(0);
        let s = r.deref().as_ref().unwrap()[0].as_ref().unwrap_err();
        assert!(format!("{:?}", s).contains("EVM transact error: Transaction(LackOfFundForMaxFee"));

        check_account_info(&block_ctx, &from, 0, 2);
        check_account_info(&block_ctx, &to_address, 0, 0);
        check_account_info(&block_ctx, &coinbase, init_balance, 0);
    }

    #[test]
    #[serial]
    #[ignore]
    fn test_error_when_warmup() {
        let temp_dir = tempfile::Builder::new()
            .prefix("test_context")
            .tempdir()
            .unwrap();

        let from = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let to_address = address!("00000000000000000000000000000000000000a3");
        let coinbase = address!("000000000000000000000000000000000000cbcb");

        let init_balance = 1_000_000_000_000_000_000u128;

        let block_ctx = create_transfer(
            temp_dir.path().to_str().unwrap(),
            from,
            to_address,
            init_balance,
            coinbase,
            false,
            true,
        );

        block_ctx.warmup(0);
        block_ctx.execute_task(0);
        block_ctx.end_block();

        // println!("block_ctx.results = {:?}", block_ctx.results);
        let r = block_ctx.read_result(0);
        let s = r.deref().as_ref().unwrap()[0].as_ref().unwrap_err();
        assert!(format!("{:?}", s).contains("Tx 0 warmup error: Cannot find caller account"));

        check_account_info(&block_ctx, &from, 0, 0);
        check_account_info(&block_ctx, &to_address, 0, 0);
        check_account_info(&block_ctx, &coinbase, 0, 0);
    }

    #[test]
    #[ignore = "WIP"]
    #[serial]
    fn test_error_when_create_empty_account() {
        let temp_dir = tempfile::Builder::new()
            .prefix("test_context")
            .tempdir()
            .unwrap();

        let from = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let to_address = address!("00000000000000000000000000000000000000a3");

        let init_balance = 1_000_000_000_000_000_000u128 + 2 * (2 * 21000 + 1);

        let block_ctx = create_transfer(
            temp_dir.path().to_str().unwrap(),
            from,
            to_address,
            init_balance,
            Address::ZERO,
            true,
            true,
        );
        {
            let mut opt = block_ctx.tasks_manager.task_for_write(0);
            let task = opt.as_mut().unwrap();
            task.tx_list[0].env.value = U256::ZERO;
            task.tx_list[1].env.value = U256::ZERO;
        }

        block_ctx.warmup(0);
        block_ctx.execute_task(0);
        block_ctx.end_block();

        // println!("block_ctx.results = {:?}", block_ctx.results);
        check_account_info(&block_ctx, &from, 1000000000000000002, 2);
        check_account_info(&block_ctx, &to_address, 0, 0);
        check_account_info(&block_ctx, &Address::ZERO, 84000, 0);
    }

    #[test]
    #[serial]
    #[ignore]
    fn test_counter() {
        // let temp_dir = tempfile::Builder::new()
        //     .prefix("test_context")
        //     .tempdir()
        //     .unwrap();

        // let eoa_addr = address!("0000000000000000000000000000000000000e0a");
        // let ca_addr = address!("0000000000000000000000000000000000000ccc");

        // let block_ctx = create_contract_call(
        //     temp_dir.path().to_str().unwrap(),
        //     eoa_addr,
        //     ca_addr,
        //     U256::ZERO,
        //     vec![AccessListItem {
        //         address: WRITE_SLOT,
        //         storage_keys: vec![B256::right_padding_from(&extract_short_hash(
        //             &addr_idx_hash(
        //                 &address!("0000000000000000000000000000000000000ccc"),
        //                 &U256::ZERO,
        //             )
        //             .to_be_bytes_vec(),
        //         ))],
        //     }],
        //     Address::ZERO,
        //     true,
        // );
        // block_ctx.warmup(0);
        // block_ctx.execute_task(0);
        // block_ctx.end_block();

        // // println!("block_ctx.results = {:?}", block_ctx.results);
        // let result0 = block_ctx.read_result(0);
        // let v = result0.deref().as_ref().unwrap();
        // let rs = v[0].as_ref().unwrap();
        // let contract_acc = rs.state.get(&ca_addr).unwrap();
        // let counter_slot = contract_acc.storage.get(&U256::ZERO).unwrap();
        // assert!(rs.result.is_success());
        // assert_eq!(U256::ZERO, counter_slot.original_value);
        // assert_eq!(U256::from(3), counter_slot.present_value);

        // check_account_info(&block_ctx, &eoa_addr, 999999999999908266 - (1 << 48), 1);
        // check_account_info(&block_ctx, &ca_addr, 0, 0);
        // check_account_info(&block_ctx, &Address::ZERO, 91734, 0);
    }

    #[test]
    #[ignore]
    #[serial]
    #[should_panic(
        expected = "Cannot find bytecode for 0xfc6558cefd6695e34c26c03b79349ed51debddd4342a5a0cf55016531d2e23c8"
    )]
    fn test_error_when_cannot_find_bytecode() {
        // let temp_dir = tempfile::Builder::new()
        //     .prefix("test_context")
        //     .tempdir()
        //     .unwrap();

        // let eoa_addr = address!("0000000000000000000000000000000000000e0a");
        // let ca_addr = address!("0000000000000000000000000000000000000ccc");

        // let block_ctx = create_contract_call(
        //     temp_dir.path().to_str().unwrap(),
        //     eoa_addr,
        //     ca_addr,
        //     U256::ZERO,
        //     vec![AccessListItem {
        //         address: WRITE_SLOT,
        //         storage_keys: vec![B256::right_padding_from(&extract_short_hash(
        //             &addr_idx_hash(
        //                 &address!("0000000000000000000000000000000000000ccc"),
        //                 &U256::ZERO,
        //             )
        //             .to_be_bytes_vec(),
        //         ))],
        //     }],
        //     Address::ZERO,
        //     false,
        // );
        // block_ctx.warmup(0);
        // block_ctx.execute_task(0);
        // block_ctx.end_block();
    }

    // read from a slot that is not in the access list
    #[test]
    #[serial]
    #[ignore]
    fn test_error_when_read_not_in_accesslist() {
        let temp_dir = tempfile::Builder::new()
            .prefix("test_context")
            .tempdir()
            .unwrap();

        let eoa_addr = address!("0000000000000000000000000000000000000e0a");
        let ca_addr = address!("0000000000000000000000000000000000000ccc");
        let coinbase = address!("0000000000000000000000000000000000000bbb");

        let block_ctx = create_contract_call(
            temp_dir.path().to_str().unwrap(),
            eoa_addr,
            ca_addr,
            U256::ZERO,
            vec![],
            coinbase,
            true,
        );
        block_ctx.warmup(0);
        block_ctx.execute_task(0);
        block_ctx.end_block();

        // println!("block_ctx.results = {:?}", block_ctx.results);
        let r = block_ctx.read_result(0);
        let s = r.deref().as_ref().unwrap()[0].as_ref().unwrap_err();
        assert!(format!("{:?}", s).contains("EVM transact error: Database(Slot 0x0000000000000000000000000000000000000CcC/0 is not in access set"));
        check_account_info(&block_ctx, &eoa_addr, 999437048046578688, 1);
        check_account_info(&block_ctx, &coinbase, 562951953421312u128, 0);
    }

    // write to a slot that is only read in the access list
    #[test]
    #[serial]
    #[ignore]
    fn test_error_when_write_only_read_in_accesslist() {
        // let temp_dir = tempfile::Builder::new()
        //     .prefix("test_context")
        //     .tempdir()
        //     .unwrap();

        // let eoa_addr = address!("0000000000000000000000000000000000000e0a");
        // let ca_addr = address!("0000000000000000000000000000000000000ccc");
        // let coinbase = address!("0000000000000000000000000000000000000bbb");

        // let block_ctx = create_contract_call(
        //     temp_dir.path().to_str().unwrap(),
        //     eoa_addr,
        //     ca_addr,
        //     U256::ZERO,
        //     vec![AccessListItem {
        //         address: READ_SLOT,
        //         storage_keys: vec![B256::right_padding_from(&extract_short_hash(
        //             &addr_idx_hash(
        //                 &address!("0000000000000000000000000000000000000ccc"),
        //                 &U256::ZERO,
        //             )
        //             .to_be_bytes_vec(),
        //         ))],
        //     }],
        //     coinbase,
        //     true,
        // );
        // block_ctx.warmup(0);
        // block_ctx.execute_task(0);
        // block_ctx.end_block();

        // let r = block_ctx.read_result(0);
        // let s = r.deref().as_ref().unwrap()[0].as_ref().unwrap_err();
        // assert!(format!("{:?}", s).contains("Commit state change error: Slot 0x0000000000000000000000000000000000000CcC/0 is not in write set"));

        // check_account_info(&block_ctx, &eoa_addr, 999437048046578688, 1);
        // check_account_info(&block_ctx, &coinbase, 562951953421312u128, 0);
    }
}
