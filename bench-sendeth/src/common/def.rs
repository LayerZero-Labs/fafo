use alloy_sol_types::SolValue;
use log::info;
use qmdb::utils::lfsr::GaloisLfsr;
use revm::primitives::{address, keccak256, Address, U256};
use serde::Serialize;

use super::cli;

pub const GROUP_SIZE_BITS: usize = 4;
// THREAD_COUNT is only used in commented code
// pub const THREAD_COUNT: usize = 64;

// Each txn is stored as 2x 6-bytes in blocks.dat
pub const BYTES_PER_TXN: usize = 6 * 2;

// adddress is created by test_deploy_erc20
pub const CA_ADDR: Address = address!("0x31256cb3d8cb35671f13b5b1680b2cf4fe55fc4f");
// erc20_hex is created by test_deploy_erc20
pub const ERC20_HEX : &str= "0x608060405234801561000f575f80fd5b5060043610610091575f3560e01c8063313ce56711610064578063313ce5671461013157806370a082311461014f57806395d89b411461017f578063a9059cbb1461019d578063dd62ed3e146101cd57610091565b806306fdde0314610095578063095ea7b3146100b357806318160ddd146100e357806323b872dd14610101575b5f80fd5b61009d6101fd565b6040516100aa9190610a5b565b60405180910390f35b6100cd60048036038101906100c89190610b0c565b61028d565b6040516100da9190610b64565b60405180910390f35b6100eb6102af565b6040516100f89190610b8c565b60405180910390f35b61011b60048036038101906101169190610ba5565b6102b8565b6040516101289190610b64565b60405180910390f35b6101396102e6565b6040516101469190610c10565b60405180910390f35b61016960048036038101906101649190610c29565b6102ee565b6040516101769190610b8c565b60405180910390f35b610187610333565b6040516101949190610a5b565b60405180910390f35b6101b760048036038101906101b29190610b0c565b6103c3565b6040516101c49190610b64565b60405180910390f35b6101e760048036038101906101e29190610c54565b6103e5565b6040516101f49190610b8c565b60405180910390f35b60606003805461020c90610cbf565b80601f016020809104026020016040519081016040528092919081815260200182805461023890610cbf565b80156102835780601f1061025a57610100808354040283529160200191610283565b820191905f5260205f20905b81548152906001019060200180831161026657829003601f168201915b5050505050905090565b5f80610297610467565b90506102a481858561046e565b600191505092915050565b5f600254905090565b5f806102c2610467565b90506102cf858285610480565b6102da858585610513565b60019150509392505050565b5f6012905090565b5f805f8373ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1681526020019081526020015f20549050919050565b60606004805461034290610cbf565b80601f016020809104026020016040519081016040528092919081815260200182805461036e90610cbf565b80156103b95780601f10610390576101008083540402835291602001916103b9565b820191905f5260205f20905b81548152906001019060200180831161039c57829003601f168201915b5050505050905090565b5f806103cd610467565b90506103da818585610513565b600191505092915050565b5f60015f8473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1681526020019081526020015f205f8373ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1681526020019081526020015f2054905092915050565b5f33905090565b61047b8383836001610603565b505050565b5f61048b84846103e556b90507fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff81101561050d57818110156104fe578281836040517ffb8f41b20000000000000000000000000000000000000000000000000000000081526004016104f593929190610cfe565b60405180910390fd5b61050c84848484035f61060356b50505050565b5f73ffffffffffffffffffffffffffffffffffffffff168373ffffffffffffffffffffffffffffffffffffffff1603610583575f6040517f96c6fd1e00000000000000000000000000000000000000000000000000000000815260040161057a9190610d33565b60405180910390fd5b5f73ffffffffffffffffffffffffffffffffffffffff168273ffffffffffffffffffffffffffffffffffffffff16036105f3575f6040517fec442f050000000000000000000000000000000000000000000000000000000081526004016105ea9190610d33565b60405180910390fd5b6105fe8383836107d2565b505050565b5f73ffffffffffffffffffffffffffffffffffffffff168473ffffffffffffffffffffffffffffffffffffffff1603610673575f6040517fe602df0500000000000000000000000000000000000000000000000000000000815260040161066a9190610d33565b60405180910390fd5b5f73ffffffffffffffffffffffffffffffffffffffff168373ffffffffffffffffffffffffffffffffffffffff16036106e3575f6040517f94280d620000000000000000000000000000000000000000000000000000000081526004016106da9190610d33565b60405180910390fd5b8160015f8673ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1681526020019081526020015f205f8573ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1681526020019081526020015f208190555080156107cc578273ffffffffffffffffffffffffffffffffffffffff168473ffffffffffffffffffffffffffffffffffffffff167f8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925846040516107c39190610b8c565b60405180910390a35b50505050565b5f73ffffffffffffffffffffffffffffffffffffffff168373ffffffffffffffffffffffffffffffffffffffff1603610822578060025f8282546108169190610d79565b925050819055506108f0565b5f805f8573ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1681526020019081526020015f20549050818110156108ab578381836040517fe450d38c0000000000000000000000000000000000000000000000000000000081526004016108a293929190610cfe565b60405180910390fd5b8181035f808673ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1681526020019081526020015f2081905550505b5f73ffffffffffffffffffffffffffffffffffffffff168273ffffffffffffffffffffffffffffffffffffffff1603610937578060025f8282540392505081905550610981565b805f808473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1681526020019081526020015f205f82825401925050819055505b8173ffffffffffffffffffffffffffffffffffffffff168373ffffffffffffffffffffffffffffffffffffffff167fddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef836040516109de9190610b8c565b60405180910390a3505050565b5f81519050919050565b5f82825260208201905092915050565b8281835e5f83830152505050565b5f601f19601f8301169050919050565b5f610a2d826109eb565b610a3781856109f5565b9350610a47818560208601610a05565b610a5081610a13565b840191505092915050565b5f6020820190508181035f830152610a738184610a23565b905092915050565b5f80fd5b5f73ffffffffffffffffffffffffffffffffffffffff82169050919050565b5f610aa882610a7f565b9050919050565b610ab881610a9e565b8114610ac2575f80fd5b50565b5f81359050610ad381610aaf565b92915050565b5f819050919050565b610aeb81610ad9565b8114610af5575f80fd5b50565b5f81359050610b0681610ae2565b92915050565b5f8060408385031215610b2257610b21610a7b565b5b5f610b2f85828601610ac556b9250506020610b4085828601610af8565b9150509250929050565b5f8115159050919050565b610b5e81610b4a565b82525050565b5f602082019050610b775f830184610b55565b92915050565b610b8681610ad9565b82525050565b5f602082019050610b9f5f830184610b7d565b92915050565b5f805f60608486031215610bbc57610bbb610a7b565b5b5f610bc986828701610ac556b9350506020610bda86828701610ac5565b9250506040610beb86828701610af8565b9150509250925092565b5f60ff82169050919050565b610c0a81610bf5565b82525050565b5f602082019050610c235f830184610c01565b92915050565b5f60208284031215610c3e57610c3d610a7b565b5b5f610c4b84828501610ac5565b91505092915050565b5f8060408385031215610c6a57610c69610a7b565b5b5f610c7785828601610ac5565b9250506020610c8885828601610ac5565b9150509250929050565b7f4e487b71000000000000000000000000000000000000000000000000000000005f52602260045260245ffd5b5f6002820490506001821680610cd657607f821691505b602082108103610ce957610ce8610c92565b5b50919050565b610cf881610a9e565b82525050565b5f606082019050610d115f830186610cef565b610d1e6020830185610b7d565b610d2b6040830184610b7d565b949350505050565b5f602082019050610d465f830184610cef565b92915050565b7f4e487b71000000000000000000000000000000000000000000000000000000005f52601160045260245ffd5b5f610d8382610ad9565b9150610d8e83610ad9565b9250828201905080821115610da657610da5610d4c565b5b9291505056fea2646970667358221220dad31183e9829269789a79f621d3d4dcbe2951a1d7994a9840bb7701c0aec35e64736f6c634300081a0033000000000000000000000000000000000000000000000000000000000000000000";

#[derive(Serialize, Debug)]
pub struct TokenSetup {
    pub count_in_block_bits: usize,
    pub create_block_count_bits: usize,
    pub group_size_bits: usize,
    pub lfsr_bits: usize,
    // Block size used in init-db
    pub count_in_block: usize,
    // Number of blocks used in init-db
    pub create_block_count: usize,
    // Number of accounts used in init-db (and also the maximum account that can be referenced)
    pub entry_count: u64,
    // Bytes in vec used in blocks.dat (prepare-workload)
    pub bytes_in_vec: usize,
    pub group_size: u64,
    // Total number of transactions used in prepare and replay
    pub transfer_txn_count: u64,
    // Block size used in prepare and replay
    pub transfer_block_count: usize,
    pub txn_per_transfer_block: usize,
    pub name: String,
    pub is_native_eth: bool, // true if the workload is native ETH, false if it is ERC20
}

impl TokenSetup {
    pub fn new(
        name: String,
        count_in_block_bits: usize,
        create_block_count_bits: usize,
        group_size_bits: usize,
        mut transfer_txn_count: u64,
        mut txn_per_transfer_block: usize,
        is_native_eth: bool,
    ) -> Self {
        let count_in_block = 1 << count_in_block_bits;
        let create_block_count = 1 << create_block_count_bits;
        let group_size = 1 << group_size_bits;
        // Number of accounts: 2^(count_in_block_bits+create_block_count_bits)
        let lfsr_bits = count_in_block_bits + create_block_count_bits - group_size_bits;

        if txn_per_transfer_block == 0 {
            txn_per_transfer_block = count_in_block / 2;
        }

        let transfer_block_count = if transfer_txn_count == 0 {
            create_block_count / group_size
        } else {
            transfer_txn_count as usize / txn_per_transfer_block
        };

        if transfer_txn_count == 0 {
            transfer_txn_count = (txn_per_transfer_block * transfer_block_count) as u64;
        }

        info!(
            "TokenSetup({}, {} create_blocks x {} transfers, {} transfer_blocks)",
            name,
            create_block_count,
            count_in_block,
            create_block_count / group_size
        );

        Self {
            count_in_block_bits,
            create_block_count_bits,
            group_size_bits,
            lfsr_bits,
            count_in_block: count_in_block,
            create_block_count,
            entry_count: count_in_block as u64 * create_block_count as u64,
            bytes_in_vec: count_in_block as usize * BYTES_PER_TXN,
            group_size: group_size as u64,
            transfer_txn_count,
            transfer_block_count,
            txn_per_transfer_block: txn_per_transfer_block,
            name,
            is_native_eth,
        }
    }

    pub fn create_lfsr_pair(&self, seed: u64) -> (GaloisLfsr, GaloisLfsr) {
        (
            GaloisLfsr::new(seed, self.lfsr_bits),
            GaloisLfsr::new(seed, 63),
        )
    }

    pub fn get_rand(&self, lfsr_pair: &mut (GaloisLfsr, GaloisLfsr)) -> u64 {
        // Output range: [1, 2^lfsr_bits - group_size + 1)
        // The purpose of having the second RNG is to avoid cycling when lfsr_bits is too low
        // We minus self.group_size because LSFRs do not produce 0. We want the missing values
        // to be at the top end of the range rather than the bottom
        lfsr_pair.0.next() * self.group_size + lfsr_pair.1.next() % self.group_size
            - self.group_size
            + 1
    }

    pub fn get_data_dir(&self, db_dir: &str) -> String {
        format!(
            "{}/exepipe_bencheth/{}_{}_{}",
            db_dir, self.name, self.count_in_block_bits, self.create_block_count_bits,
        )
    }
}

pub fn get_benchmark_constants(args: &cli::BenchmarkCli) -> TokenSetup {
    let count_in_block_bits = if args.create_txn_per_block_bits == 0 {
        match args.workload.as_str() {
            "token" => 18,
            "eth" => 21,
            _ => panic!("Invalid token type: {}", args.workload),
        }
    } else {
        args.create_txn_per_block_bits
    };
    assert!(
        args.entry_count_bits - count_in_block_bits > GROUP_SIZE_BITS as usize,
        "entry_count_bits={} - count_in_block_bits={} <= GROUP_SIZE_BITS={} (decrease entry_count_bits or increase count_in_block_bits)",
        args.entry_count_bits,
        count_in_block_bits,
        GROUP_SIZE_BITS
    );
    match args.workload.as_str() {
        // 2^18 accounts per block x 2^13 blocks = 2^31 txns (2 billion)
        // Transfer blocks: 2^13 / 2^4 = 2^9
        // If total_bits = 31, then create_block_count = 31 - 18 = 13
        "token" => TokenSetup::new(
            String::from("token"),
            count_in_block_bits,
            args.entry_count_bits - count_in_block_bits,
            GROUP_SIZE_BITS,
            args.transfer_txn_count,
            args.txn_per_transfer_block,
            false,
        ),
        // 2^21 accounts per block x 2^10 blocks = 2^31 txns (2 billion)
        // Transfer blocks: 2^10 / 2^4 = 2^6
        // If total_bits = 31, then create_block_count = 31 - 21 = 10
        "eth" => TokenSetup::new(
            String::from("eth"),
            count_in_block_bits,
            args.entry_count_bits - count_in_block_bits,
            GROUP_SIZE_BITS,
            args.transfer_txn_count,
            args.txn_per_transfer_block,
            true,
        ),
        _ => panic!("Invalid token type: {}", args.workload),
    }
}

pub fn erc_address_storage(address: Address) -> U256 {
    keccak256((address, U256::from(0)).abi_encode()).into()
}
