.PHONY: ci fmt-check test clippy integration bench

ci: fmt-check test clippy integration bench

fmt-check:
	cargo fmt --all -- --check

test:
	cargo test --workspace

clippy:
	RUSTUP_TOOLCHAIN=stable cargo clippy --workspace --all-targets -- -D warnings

integration:
	cargo test -p legato-server --test end_to_end

bench:
	cargo bench -p legato-server --no-run
