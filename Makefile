SHELL   := /bin/bash
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)
BIN     := bin

.PHONY: all deps build server client test race vet fmt lint clean run-server run-client

all: build

## 首次拉取依赖(容器/离线环境请先配好 GOPROXY)
deps:
	go mod tidy

build: server client

server:
	@mkdir -p $(BIN)
	go build -trimpath -ldflags '$(LDFLAGS)' -o $(BIN)/tunnel-server ./cmd/tunnel-server

client:
	@mkdir -p $(BIN)
	go build -trimpath -ldflags '$(LDFLAGS)' -o $(BIN)/tunnel-client ./cmd/tunnel-client

## 交叉编译(边缘机常见目标)
linux-amd64:
	@mkdir -p $(BIN)
	GOOS=linux GOARCH=amd64 go build -trimpath -ldflags '$(LDFLAGS)' -o $(BIN)/tunnel-client-linux-amd64 ./cmd/tunnel-client
	GOOS=linux GOARCH=amd64 go build -trimpath -ldflags '$(LDFLAGS)' -o $(BIN)/tunnel-server-linux-amd64 ./cmd/tunnel-server

linux-arm64:
	@mkdir -p $(BIN)
	GOOS=linux GOARCH=arm64 go build -trimpath -ldflags '$(LDFLAGS)' -o $(BIN)/tunnel-client-linux-arm64 ./cmd/tunnel-client
	GOOS=linux GOARCH=arm64 go build -trimpath -ldflags '$(LDFLAGS)' -o $(BIN)/tunnel-server-linux-arm64 ./cmd/tunnel-server

test:
	go test ./... -count=1

race:
	go test ./... -race -count=1 -timeout 480s

vet:
	go vet ./...

fmt:
	gofmt -w .

lint: fmt vet

clean:
	rm -rf $(BIN)

run-server: server
	$(BIN)/tunnel-server -config config.yaml -log-level debug

run-client: client
	$(BIN)/tunnel-client --url ws://127.0.0.1:8443/ws --key xx1 -log-level debug
