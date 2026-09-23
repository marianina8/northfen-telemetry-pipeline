.PHONY: help build test test-race vet fmt demo dashboard mcp compare clean sam-validate sam-build sam-deploy sam-outputs

BIN := bin
DATA ?= .northfen
LAMBDA_BUILD = GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -tags lambda.norpc -trimpath -ldflags="-s -w" -o $(ARTIFACTS_DIR)/bootstrap

help:
	@echo "make build       build bin/northfen and bin/dashboard"
	@echo "make test        run all unit tests (no network, no AWS)"
	@echo "make demo        reset local data and run every scenario through the pipeline"
	@echo "make dashboard   live console on http://127.0.0.1:8080 (offline mock explainer)"
	@echo "make mcp         MCP server on stdio (read-only)"
	@echo "make compare     naive fixed limits vs adaptive detection (BEDROCK=1 adds the live model)"
	@echo "make sam-build / sam-deploy / sam-outputs   AWS (see infra/README.md)"

build:
	go build -o $(BIN)/northfen ./cmd/northfen
	go build -o $(BIN)/dashboard ./cmd/dashboard

test:
	go test ./...

test-race:
	go test -race ./...

vet:
	go vet ./...

fmt:
	gofmt -w .

demo: build
	./demo/run-demo.sh

dashboard: build
	$(BIN)/dashboard -data $(DATA)

mcp: build
	$(BIN)/northfen -data $(DATA) mcp

compare:
	go run ./compare -out compare/RESULTS.md $(if $(BEDROCK),-bedrock -profile demos-admin,)

clean:
	rm -rf $(BIN) $(DATA) infra/.aws-sam

# --- AWS SAM (phase 5) -------------------------------------------------------
sam-validate:
	cd infra && sam validate --lint

sam-build:
	cd infra && sam build

sam-deploy: sam-build
	cd infra && sam deploy

sam-outputs:
	aws cloudformation describe-stacks --stack-name northfen-telemetry-pipeline --profile demos-admin --region us-west-2 \
		--query "Stacks[0].Outputs[].[OutputKey,OutputValue]" --output table

# Called by `sam build` (BuildMethod: makefile, CodeUri: repo root). Config and
# demo data are embedded in the binaries, so nothing else is copied.
build-ConsumerFunction:
	$(LAMBDA_BUILD) ./cmd/lambda/consumer
build-ExplainFunction:
	$(LAMBDA_BUILD) ./cmd/lambda/explainer
build-SimulatorFunction:
	$(LAMBDA_BUILD) ./cmd/lambda/simulator
build-WebhookFunction:
	$(LAMBDA_BUILD) ./cmd/lambda/webhook
build-DashboardFunction:
	$(LAMBDA_BUILD) ./cmd/lambda/dashboard
