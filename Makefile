.PHONY: run
run:
	@go run cmd/cli/main.go

.PHONY: build
build:
	@go build -o bin/lsmt cmd/cli/main.go

.PHONY: test
test:
	go test -race ./...
coverage:
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	open coverage.html