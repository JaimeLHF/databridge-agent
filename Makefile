BINARY_NAME=databridge-agent
VERSION?=1.5.0
LDFLAGS=-ldflags "-X main.Version=$(VERSION)"

.PHONY: build build-linux build-windows build-all clean

build:
	go build $(LDFLAGS) -o $(BINARY_NAME) ./cmd/databridge-agent

build-linux:
	GOOS=linux GOARCH=amd64 go build $(LDFLAGS) -o $(BINARY_NAME)-linux-amd64 ./cmd/databridge-agent

build-windows:
	GOOS=windows GOARCH=amd64 go build $(LDFLAGS) -o $(BINARY_NAME)-windows-amd64.exe ./cmd/databridge-agent

build-all: build-linux build-windows

clean:
	rm -f $(BINARY_NAME) $(BINARY_NAME)-linux-amd64 $(BINARY_NAME)-windows-amd64.exe
