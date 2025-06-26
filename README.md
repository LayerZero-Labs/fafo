# FAFO: Fast Ahead-of-Formation Optimization

## Overview

FAFO is a blockchain transaction scheduler that optimizes transaction execution by reordering transactions before block formation for maximum concurrency.

- Over 1 million Merkleized EVM-Compatible transactions per second while maintaining state verifiability
- Near-linear throughput scaling up to 96 cores and beyond on a single machine
- 91% cheaper than sharded execution

Read the full FAFO paper here: https://layerzero.network/publications/FAFO_Whitepaper.pdf

_FAFO is ongoing research. Designed for high performance and practical use, some features are still evolving. We invite feedback and contributions from the community._

## Architecture

FAFO has three key components:

- **ParaLyze** preprocesses each transaction in the mempool to approximate the addresses of every storage slot it reads and writes.					
- **ParaFramer** uses our cache-friendly data structure, ParaBloom, to group non-conflicting (parallel-executable) transactions into frames. Frames are groups of transactions that do not have data conflicts and thus can be run in parallel.
- **ParaScheduler** extracts even more parallelism out of the stream by computing a collection of precedence graphs for each storage slot accessed by each transaction.

## Quick start

To get started, clone the repository and install the dependencies:

```bash
git clone https://github.com/LayerZero-Labs/fafo.git
cd fafo
./install-dependencies
```

Run a quick benchmark:

```bash
# No contention
./bench-sendeth/run.sh --size fast --generator none
# Contention
./bench-sendeth/run.sh --size fast --generator hotn
```

See [bench-sendeth/readme.md](./bench-sendeth/readme.md) for more benchmark details.

## Installation

### Prerequisites

- [Docker Desktop](https://docs.docker.com/desktop/) with the following recommended settings:
    - Virtual disk size: At least 256GB
    - Memory limit: At least 32GB
- [Visual Studio Code](https://code.visualstudio.com/)
- [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)

Due to the complexities of software versions and environments on your host, it can be challenging to resolve issues
related to these factors.

We strongly recommend using Dev Containers for development to ensure a consistent and isolated development environment
and avoid common setup issues, and that will be helpful to reproduce the issues also.

Setup steps:

1. Install Visual Studio Code (VSCode)
2. Install the "Dev Containers" extension in VSCode
3. Use VSCode's "Dev Containers: Clone Repository in Named Container Volume..." command to clone the repository
    - Open Command Palette (Ctrl+Shift+P or Cmd+Shift+P)
    - Type and select "Dev Containers: Clone Repository in Named Container Volume..."
    - Enter repository URL when prompted
    - Choose a meaningful name for the volume

This setup ensures optimal performance and consistent development environment.

For more details, see [Dev Containers in VS Code](./docs/devcontainers.md#visual-studio-code)

## Development

See [CONTRIBUTING.md](./CONTRIBUTING.md) for our full development workflow.

## Any questions?

[Please raise a GitHub issue](https://github.com/LayerZero-Labs/fafo/issues/new). For vulnerabilities, please report them privately via GitHub security page per our [security policy](./SECURITY.md).

## License

This project is dual licensed under the MIT License and the Apache License 2.0.

## See also

- **QMDB**: FAFO builds on top of [QMDB](https://github.com/LayerZero-Labs/qmdb), our fast Merkleized database.
- **vApps**: FAFO enables lights clients and stateless validation for ZK-based vApps. Read more about vApps in the paper: [vApps: A Unified Rust-Based DSL for Verifiable Blockchain Computing Applications](https://arxiv.org/abs/2504.14809).

## Acknowledgements

If you use FAFO in a publication, please cite it as:

**FAFO: Over 1 million TPS on a single node running EVM while still Merkleizing every block**<br>
Ryan Zarick, Isaac Zhang, Daniel Wong, Thomas Kim, Bryan Pellegrino, Mignon Li, Kelvin Wong<br>
<https://layerzero.network/research/fafo>

```bibtex
@article{zarick2025fafo,
  title={FAFO: Over 1 million TPS on a single node running EVM while still Merkleizing every block},
  author={Zarick, Ryan and Zhang, Isaac and Wong, Daniel and Kim, Thomas and Pellegrino, Bryan and Li, Mignon and Wong, Kelvin},
  journal={Under submission},
  year={2025}
}
```

QMDB is a product of [LayerZero Labs](https://layerzero.network) Research.

<!-- markdownlint-disable MD033 -->
<p align="center">
  <a href="https://layerzero.network#gh-dark-mode-only">
    <img alt="LayerZero" style="width: 50%" src="https://github.com/LayerZero-Labs/devtools/raw/main/assets/logo-dark.svg#gh-dark-mode-only"/>
  </a>  
  <a href="https://layerzero.network#gh-light-mode-only">
    <img alt="LayerZero" style="width: 50%" src="https://github.com/LayerZero-Labs/devtools/raw/main/assets/logo-light.svg#gh-light-mode-only"/>
  </a>
</p>

<p align="center">
  <a href="https://layerzero.network" style="color: #a77dff">Homepage</a> | <a href="https://docs.layerzero.network/" style="color: #a77dff">Docs</a> | <a href="https://layerzero.network/developers" style="color: #a77dff">Developers</a>
</p>