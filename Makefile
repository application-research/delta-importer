SHELL=/usr/bin/env bash

all: build

build: 
	git submodule update --init --recursive
	make -C extern/filecoin-ffi
	go build -o delta-importer

install:
	install -C ./delta-importer /usr/local/bin/delta-importer
