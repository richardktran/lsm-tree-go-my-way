.PHONY: run
run:
	@go run cmd/cli/main.go

.PHONY: test
test:
	go test -race ./...
