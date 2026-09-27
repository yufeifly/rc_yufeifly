GO := env -u GOROOT go

.PHONY: test race vet build demo recovery-demo benchmark verify

test:
	$(GO) test ./...

race:
	$(GO) test -race ./...

vet:
	$(GO) vet ./...

build:
	$(GO) build ./cmd/...

demo:
	bash scripts/demo.sh

recovery-demo:
	bash scripts/recovery-demo.sh

benchmark:
	bash scripts/benchmark.sh

verify: test race vet build demo recovery-demo benchmark
