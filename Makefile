.PHONY: build test lint plugin clean generate e2e e2e-postgres e2e-sqlite

WASM_TARGET = wasm32-wasip1
WASM_BIN = target/$(WASM_TARGET)/release/sqlc-gen-gleam.wasm
SQLC_CONFIGS = examples/authors/sqlc.yaml examples/config/sqlc.yaml end2end/sqlc.yaml

build:
	cargo build --release --target $(WASM_TARGET)

test:
	cargo test

lint:
	cargo fmt --check
	cargo clippy --all-targets -- -D warnings

# Build the plugin and point every sqlc config at the new sha256.
# `sed -i.bak` works with both GNU and BSD sed.
plugin: build
	@mkdir -p dist
	cp $(WASM_BIN) dist/sqlc-gen-gleam.wasm
	@SHA=$$(shasum -a 256 dist/sqlc-gen-gleam.wasm | cut -d ' ' -f 1); \
	echo $$SHA > dist/sqlc-gen-gleam.wasm.sha256; \
	for f in $(SQLC_CONFIGS); do \
		sed -i.bak "s/sha256: .*/sha256: \"$$SHA\"/" $$f && rm -f $$f.bak; \
	done; \
	echo "Plugin built: dist/sqlc-gen-gleam.wasm"; \
	echo "SHA256: $$SHA"

generate: plugin
	cd examples/authors && sqlc generate
	cd examples/config && sqlc generate
	cd end2end && sqlc generate

e2e-postgres: generate
	cd end2end && docker compose up -d --wait
	cd end2end/gleam_test_project && gleam test; status=$$?; \
		cd .. && docker compose down; exit $$status

e2e-sqlite: generate
	cd end2end/gleam_sqlite_test_project && gleam test

e2e: e2e-postgres e2e-sqlite

clean:
	cargo clean
	rm -rf dist
