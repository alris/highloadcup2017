#!/bin/bash

# With big respect to AterCattus
go get github.com/AterCattus/highloadcup_tester
CGO_ENABLED=0
go build -ldflags '-s -extldflags "-static"' -installsuffix netgo -o highloadcup_tester $GOPATH/src/github.com/AterCattus/highloadcup_tester/*.go

