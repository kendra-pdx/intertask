PUBLISH_FLAGS ?=

.PHONY: check release

check: clippy audit test

audit:
	cargo audit

clippy:
	cargo clippy --workspace

test:
	cargo test --workspace --all-features

release: check
	cargo ws publish --registry kellnr --git-remote gh-kendra $(PUBLISH_FLAGS)
