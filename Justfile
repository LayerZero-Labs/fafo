# Default recipe
default: build

# Function to generate space-separated -p flags for packages
[private]
pkg_flags packages:
    #!/usr/bin/env bash
    if [ -z "{{packages}}" ]; then
        echo ""
    else
        echo "{{packages}}" | tr ',' '\n' | sed 's/^/-p /' | tr '\n' ' '
    fi

# Show help about parameters
help:
    @echo ""
    @just --list
    @echo ""
    @echo "Parameter formats:"
    @echo "    packages: Comma-separated list of package names (e.g. pkg1,pkg2). Leave empty to apply to the entire workspace."
    @echo ""
    @echo "Examples:"
    @echo "    just build packages=pkg1,pkg2"
    @echo "    just test packages=pkg1"
    @echo "    just format packages=pkg1,pkg2,pkg3"
    @echo "    just massage pkg1,pkg2,pkg3"
    @echo ""

# Build target (default)
build packages='': build-release

# Install dependencies
install-deps:
    # Placeholder - add system setup or cargo install commands here

# Build in debug mode
build-debug packages='':
    #!/usr/bin/env bash
    packages={{packages}}
    packages="${packages#packages=}"
    if [ -z "${packages}" ]; then
        cargo build --verbose --workspace
    else
        cargo build --verbose $(just pkg_flags "${packages}")
    fi

# Build in release mode
build-release packages='':
    #!/usr/bin/env bash
    packages={{packages}}
    packages="${packages#packages=}"
    if [ -z "${packages}" ]; then
        cargo build --release --verbose --workspace
    else
        cargo build --release --verbose $(just pkg_flags "${packages}")
    fi

# Check code
check packages='':
    #!/usr/bin/env bash
    packages={{packages}}
    packages="${packages#packages=}"
    # RUSTFLAGS="-Awarnings" silences all compiler warnings
    if [ -z "${packages}" ]; then
        RUSTFLAGS="-A warnings" cargo check --quiet --all-targets
    else
        RUSTFLAGS="-A warnings" cargo check --quiet --all-targets $(just pkg_flags "${packages}")
    fi

# Run tests
test packages='':
    #!/usr/bin/env bash
    packages={{packages}}
    packages="${packages#packages=}"
    # RUSTFLAGS="-Awarnings" silences all compiler warnings
    if [ -z "${packages}" ]; then
        RUST_BACKTRACE=1 RUSTFLAGS="-Awarnings" cargo nextest run --workspace
    else
        RUST_BACKTRACE=1 RUSTFLAGS="-Awarnings" cargo nextest run $(just pkg_flags "${packages}")
    fi

# Coverage report
test-coverage packages='':
    #!/usr/bin/env bash
    packages={{packages}}
    packages="${packages#packages=}"
    if [ -z "${packages}" ]; then
        RUST_BACKTRACE=1 RUSTFLAGS="-Awarnings" cargo llvm-cov nextest --workspace --lcov --output-path lcov.info
    else
        RUST_BACKTRACE=1 RUSTFLAGS="-Awarnings" cargo llvm-cov nextest --lcov --output-path lcov.info $(just pkg_flags "${packages}")
    fi

# Format check
format packages='':
    #!/usr/bin/env bash
    packages={{packages}}
    packages="${packages#packages=}"
    if [ -z "${packages}" ]; then
        cargo fmt --all -- --check
    else
        cargo fmt --check $(just pkg_flags "${packages}")
    fi

# Fix formatting issues
format-fix packages='':
    #!/usr/bin/env bash
    packages={{packages}}
    packages="${packages#packages=}"
    if [ -z "${packages}" ]; then
        cargo fmt --all
    else
        cargo fmt $(just pkg_flags "${packages}")
    fi

# Lint check
lint packages='':
    #!/usr/bin/env bash
    packages={{packages}}
    packages="${packages#packages=}"
    if [ -z "${packages}" ]; then
        cargo clippy --workspace --
    else
        cargo clippy $(just pkg_flags "${packages}") --
    fi

# Fix linting issues
lint-fix packages='':
    #!/usr/bin/env bash
    packages={{packages}}
    packages="${packages#packages=}"
    if [ -z "${packages}" ]; then
        cargo clippy --fix --allow-dirty --allow-staged --workspace --
    else
        cargo clippy --fix --allow-dirty --allow-staged $(just pkg_flags "${packages}") --
    fi

# Run binary
run packages='':
    #!/usr/bin/env bash
    packages={{packages}}
    packages="${packages#packages=}"
    if [ -z "${packages}" ]; then
        cargo run --workspace --release --verbose
    else
        cargo run --release --verbose $(just pkg_flags "${packages}")
    fi

# Clean
clean:
    cargo clean

# CI task: lint + check + test
ci packages='':
    just lint {{packages}}
    just format {{packages}}    
    just check {{packages}}
    just test {{packages}}

# Git branch cleanup
smart-prune-branches:
    #!/usr/bin/env bash
    git fetch --prune
    # Delete merged branches
    git branch --merged main 2>/dev/null | grep -vE "(^\*|main|master)" | xargs -I{} git branch -d {} || true
    git branch --merged master 2>/dev/null | grep -vE "(^\*|main|master)" | xargs -I{} git branch -d {} || true
    # Delete local branches where remote branch is gone
    git branch -vv | grep ': gone]' | awk '{print $1}' | xargs -I{} git branch -D {} || true

# Cargo deny security audit
deny-check:
    cargo deny check -- all

# Code hygiene: fix, fmt, check, test
massage packages='':
    #!/usr/bin/env bash
    set -euo pipefail
    packages={{packages}}
    packages="${packages#packages=}"
    if [ -z "${packages}" ]; then
        echo "🧹 Massaging entire workspace..."
        cargo fix --allow-dirty --allow-staged
        cargo fmt --all
        cargo check --all-targets
        cargo test
    else
        echo "🧹 Massaging specific packages: ${packages}"
        cargo fix --allow-dirty --allow-staged $(just pkg_flags "${packages}")
        cargo fmt $(just pkg_flags "${packages}")
        RUSTFLAGS="-A warnings" cargo check --all-targets $(just pkg_flags "${packages}")
        RUST_BACKTRACE=1 RUSTFLAGS="-Awarnings" cargo nextest run $(just pkg_flags "${packages}")
    fi

# Format changed files
format-changed:
    #!/usr/bin/env bash
    set -euo pipefail
    # Get both staged and unstaged changes (only Added and Modified files)
    CHANGED_FILES=$(git diff --diff-filter=AM --name-only && git diff --cached --diff-filter=AM --name-only | sort | uniq)
    if [ -z "$CHANGED_FILES" ]; then
        echo "No changed files to format"
        exit 0
    fi
    
    # Filter for Rust files
    RUST_FILES=$(echo "$CHANGED_FILES" | grep '\.rs$' || true)
    if [ -z "$RUST_FILES" ]; then
        echo "No changed Rust files to format"
        exit 0
    fi
    
    echo "Formatting changed Rust files:"
    echo "$RUST_FILES" | while read -r file; do
        echo "  $file"
        cargo fmt -- "$file"
    done
