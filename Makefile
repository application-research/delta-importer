SHELL=/usr/bin/env bash
GO_BUILD_IMAGE?=golang:1.19
VERSION=$(shell git describe --always --tag --dirty)
COMMIT=$(shell git rev-parse --short HEAD)


all: build

build: 
	git submodule update --init --recursive
	make -C extern/filecoin-ffi
	go build -ldflags="-X 'main.Commit=$(COMMIT)' -X main.Version=$(VERSION)"  -o delta-importer

install:
	install -C -m 0755 delta-importer /usr/local/bin

.PHONE: clean
clean:
	rm -f delta-importer