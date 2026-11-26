# Use sccache / mold only when installed. mold: unix except Apple (Apple's ld is already fast and mold does not support it).
# Empty RUSTC_WRAPPER is ignored by cargo, which then falls back to ~/.cargo/config.toml.
export RUSTC_WRAPPER := `command -v sccache || true`
export RUSTFLAGS := if os() == "macos" { "" } else if os_family() == "unix" { `command -v mold >/dev/null && echo "-C link-arg=-fuse-ld=mold" || true` } else { "" }

# Format the crate (rustfmt.toml). Comments and strings are not wrapped: keep them within 140 columns by hand.
fmt:
    cargo fmt

# Format, lints (default and loom cfg), docs: the fast gate for every change
check:
    cargo fmt --check && cargo clippy --all-targets -- -D warnings
    CARGO_TARGET_DIR=target/loom RUSTFLAGS="--cfg gk_loom" cargo clippy --all-targets -- -D warnings

# All tests, README.md and AGENTS.md examples included
test:
    cargo test

# Loom models of the parking types and the consumer protocol (separate target dir: the cfg changes every crate's build)
loom:
    CARGO_TARGET_DIR=target/loom RUSTFLAGS="--cfg gk_loom" cargo test --release --test loom_parking

# Miri (nightly) on the Treiber suites: UB, data races, leaks. Shrink workloads under cfg(miri), not here.
miri:
    CARGO_TARGET_DIR=target/miri cargo +nightly miri test --test treiber_parking --test treiber_publish --test treiber_iterator

# Rustdoc with warnings as errors, public API only
doc:
    RUSTDOCFLAGS="-D warnings" cargo doc --no-deps

# Minimum supported Rust version (rust-version in Cargo.toml); needs the 1.85 toolchain installed
msrv:
    cargo +1.85 check --all-targets

# Packaging dry run: the crate must build from its package contents
publish-dry:
    cargo publish --dry-run

# Benchmarks (criterion, benches/treiber.rs and the tests/bench_* files)
bench-rust:
    cargo bench

# Single gate before declaring work done: format, lints, tests, loom, docs, MSRV, packaging. Miri is separate: `just miri`
verify: check test loom doc msrv publish-dry
