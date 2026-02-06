.PHONY: build build-proxy build-migrate clean test run run-migrate lint docker-build \
       build-linux build-linux-proxy build-linux-migrate package

BUILD_DIR=bin
VERSION?=$(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS=-s -w -X main.version=$(VERSION)

# === Native build (current OS/arch) ===

build: build-proxy build-migrate

build-proxy:
	@mkdir -p $(BUILD_DIR)
	go build -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/oqbridge ./cmd/oqbridge

build-migrate:
	@mkdir -p $(BUILD_DIR)
	go build -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/oqbridge-migrate ./cmd/oqbridge-migrate

# === Cross-compile for Linux amd64 ===

build-linux: build-linux-proxy build-linux-migrate

build-linux-proxy:
	@mkdir -p $(BUILD_DIR)/linux-amd64
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" \
		-o $(BUILD_DIR)/linux-amd64/oqbridge ./cmd/oqbridge

build-linux-migrate:
	@mkdir -p $(BUILD_DIR)/linux-amd64
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" \
		-o $(BUILD_DIR)/linux-amd64/oqbridge-migrate ./cmd/oqbridge-migrate

# === Package into tar.gz ===

package: build-linux
	@mkdir -p $(BUILD_DIR)/release
	@rm -rf $(BUILD_DIR)/staging
	@mkdir -p $(BUILD_DIR)/staging/oqbridge-$(VERSION)
	cp $(BUILD_DIR)/linux-amd64/oqbridge $(BUILD_DIR)/staging/oqbridge-$(VERSION)/
	cp $(BUILD_DIR)/linux-amd64/oqbridge-migrate $(BUILD_DIR)/staging/oqbridge-$(VERSION)/
	cp configs/oqbridge.yaml $(BUILD_DIR)/staging/oqbridge-$(VERSION)/
	tar -czf $(BUILD_DIR)/release/oqbridge-$(VERSION)-linux-amd64.tar.gz \
		-C $(BUILD_DIR)/staging oqbridge-$(VERSION)
	@rm -rf $(BUILD_DIR)/staging
	@echo "Package: $(BUILD_DIR)/release/oqbridge-$(VERSION)-linux-amd64.tar.gz"

# === Run ===

run: build-proxy
	./$(BUILD_DIR)/oqbridge

run-migrate: build-migrate
	./$(BUILD_DIR)/oqbridge-migrate --once

# === Test & Lint ===

test:
	go test ./...

lint:
	golangci-lint run ./...

# === Docker ===

docker-build:
	bash container/build.sh -t oqbridge:latest

# === Clean ===

clean:
	rm -rf $(BUILD_DIR)
