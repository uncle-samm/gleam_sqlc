.PHONY: build test plugin clean generate generate-e2e

WASM_TARGET = wasm32-wasip1
WASM_BIN = target/$(WASM_TARGET)/release/sqlc-gen-gleam.wasm

build:
	cargo build --release --target $(WASM_TARGET)

test:
	cargo test

plugin: build
	@mkdir -p dist
	cp $(WASM_BIN) dist/plugin.wasm
	shasum -a 256 dist/plugin.wasm | cut -d ' ' -f 1 > dist/plugin.wasm.sha256
	@SHA=$$(cat dist/plugin.wasm.sha256); \
	sed -i '' "s/sha256: .*/sha256: $$SHA/" examples/authors/sqlc.yaml; \
	sed -i '' "s/sha256: .*/sha256: \"$$SHA\"/" end2end/sqlc.yaml
	@echo "Plugin built: dist/plugin.wasm"
	@echo "SHA256: $$(cat dist/plugin.wasm.sha256)"

generate: plugin
	cd examples/authors && sqlc generate
	cd end2end && sqlc generate

generate-e2e: plugin
	cd end2end && sqlc generate
	cd end2end/gleam_test_project && gleam build

e2e: generate-e2e
	cd end2end && docker compose up -d --wait
	cd end2end/gleam_test_project && gleam test
	cd end2end && docker compose down

clean:
	cargo clean
	rm -rf dist
