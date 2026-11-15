.PHONY: build pack
build:
	mkdir -p bin
	go build -trimpath -o bin/crossseed-worker ./cmd/crossseed-worker

pack:
	bash scripts/pack.sh dist
