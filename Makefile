.PHONY: run
run:
	@go run cmd/cli/main.go

.PHONY: build
build:
	@go build -o bin/lsmt cmd/cli/main.go

.PHONY: test
test:
	go test -race ./...
