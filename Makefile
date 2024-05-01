.PHONY: build
build:
	wasm-pack build --no-default-features --target no-modules --no-typescript --out-dir pkg --debug
