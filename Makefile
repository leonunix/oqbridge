.PHONY: build build-proxy build-migrate clean test run run-migrate lint

BUILD_DIR=bin

build: build-proxy build-migrate

build-proxy:
	@mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/oqbridge ./cmd/oqbridge

build-migrate:
	@mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/oqbridge-migrate ./cmd/oqbridge-migrate

run: build-proxy
	./$(BUILD_DIR)/oqbridge

run-migrate: build-migrate
	./$(BUILD_DIR)/oqbridge-migrate --once

test:
	go test ./...

lint:
	golangci-lint run ./...

clean:
	rm -rf $(BUILD_DIR)
