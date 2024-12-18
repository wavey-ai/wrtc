.PHONY: wasm
wasm:
	wasm-pack build --target web
	rm -rf public/pkg && mv pkg public
