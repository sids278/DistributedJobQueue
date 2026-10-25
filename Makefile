# Go 1.21's internal linker omits LC_UUID, which recent macOS refuses to load.
# The external linker works around it; drop this once Go is upgraded.
LDFLAGS ?= -linkmode=external
REDIS_ADDR ?= localhost:6379

.PHONY: build test test-redis redis-up redis-down

build:
	go build -ldflags=$(LDFLAGS) -o bin/jobqueue ./cmd/jobqueue

test:
	go test -ldflags=$(LDFLAGS) -race ./...

test-redis:
	REDIS_ADDR=$(REDIS_ADDR) go test -ldflags=$(LDFLAGS) -race -count=1 ./...

redis-up:
	docker compose up -d redis

redis-down:
	docker compose down
