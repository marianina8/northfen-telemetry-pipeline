# infra/: deploying to AWS with SAM (phase 5)

`template.yaml` is the AWS version of the pipeline. Each local stand-in from phases 1–4 maps to a
real service:

| Local (phases 1–4) | AWS (phase 5) | Code |
|---|---|---|
| `pipeline.StreamLocal` / `awsapp.RunLocal` (Kinesis stand-in) | `TelemetryStream` (Kinesis, 1 shard) | `awsapp.KinesisProducer` |
| local runner / `northfen simulate` | `SimulatorFunction` (async invoke, paces a run into Kinesis) | `cmd/lambda/simulator`, `awsapp.Simulator` |
| `awsapp.Consumer` fed by the stand-in | `ConsumerFunction` (Kinesis event source mapping, batch ≤100, partial-batch failures) | `cmd/lambda/consumer` |
| in-memory SQS stand-in / `Sweep` | `ExplainQueue` (SQS, settle delay) + `ExplainDLQ` → `ExplainFunction` | `cmd/lambda/explainer`, `awsapp.Explainer` |
| `explain.Mock` | Bedrock Converse (Claude Haiku 4.5), IAM scoped to one model | `explain.Bedrock` |
| `.northfen/store.json` (`store.File`) | `ReadingsTable`, `StateTable`, `AppTable` (DynamoDB, on-demand, TTL) | `store.Dynamo` |
| `.northfen/outbox.jsonl` | `nf_action` CloudWatch log lines + audit trail; pages → `PageTopic` (SNS) | `dispatch.LogSink`, `dispatch.PagerSink` |
| `bin/dashboard` on 127.0.0.1 | `DashboardApi` + `DashboardFunction` at `/demos/northfen/` | `cmd/lambda/dashboard` |
| n/a | `WebhookApi` + `WebhookFunction`: `POST /simulate`, `POST /readings` | `cmd/lambda/webhook`, `awsapp.Webhook` |

Timestream isn't used: LiveAnalytics has been closed to new customers since 2025-06-20, so the
time series lives in DynamoDB. See `NORTHFEN-ARCHITECTURE.md` → Storage.

## Before the first deploy (one time)

- AWS CLI v2, SAM CLI and **Go 1.24+** on your Mac (`sam build` compiles the Lambdas with your
  local Go through the repo `Makefile`). Docker is only needed for `sam local`.
- `aws sso login --profile demos-admin` (samconfig.toml uses that profile and `us-west-2`).
- Bedrock: the same Claude Haiku 4.5 inference profile Rivergate/Amberlight use. Check it:
  `aws bedrock-runtime converse --model-id us.anthropic.claude-haiku-4-5-20251001-v1:0 --messages '[{"role":"user","content":[{"text":"ping"}]}]' --profile demos-admin --region us-west-2`
- The two SSM secrets:

```sh
aws ssm put-parameter --name /northfen-telemetry/webhook-shared-secret --type SecureString \
  --value "$(openssl rand -hex 32)" --profile demos-admin --region us-west-2
aws ssm put-parameter --name /northfen-telemetry/dashboard-password --type SecureString \
  --value 'choose-a-demo-password' --profile demos-admin --region us-west-2
```

  The dashboard password is what you share with prospects. It's separate from the Rivergate and
  Amberlight passwords. To change it later, re-run with `--overwrite`; it takes effect on the next
  cold start, and everyone is signed out.

## Deploy

From the repo root:

```sh
go mod tidy        # this build's session couldn't reach proxy.golang.org for a test-only dep; tidy once locally
make test          # everything passes locally first
make sam-validate  # sam validate --lint
make sam-build     # cross-compiles the five Lambdas for linux/arm64 (~3-4 min the first time)
make sam-deploy    # shows the changeset and asks before applying (confirm_changeset = true)
make sam-outputs   # DashboardOrigin, DashboardUrl, WebhookUrl, table names, ...
```

`samconfig.toml` already carries the stack name, region, profile, parameters and the
`app=northfen-telemetry data=synthetic` tags, so plain `sam deploy` works; `--guided` isn't needed.
To deploy the plumbing without any Bedrock calls first, add
`--parameter-overrides ExplainProvider=mock`.

## Put it on marian.online

```sh
python3 infra/site/add-northfen-to-marian-online.py ~/Code/github.com/marianina8/marian.online <DashboardOrigin>
```

This adds three `/demos/northfen` rewrites to `vercel.json` (before the SPA fallback, same shape as
Rivergate's and Amberlight's) and the Northfen card to `src/pages/DemosPage.tsx`, directly below
Amberlight, with the same LIVE DEMO badge, sections, stack chips, launch button, password request
and GitHub link. It's idempotent. Review the diff, commit and push; Vercel deploys it.

## Try it against the deployed stack

```sh
export AWS_PROFILE=demos-admin
OUT=$(aws cloudformation describe-stacks --stack-name northfen-telemetry-pipeline --region us-west-2 --query "Stacks[0].Outputs" --output json)
get() { echo "$OUT" | python3 -c "import sys,json;print({o['OutputKey']:o['OutputValue'] for o in json.load(sys.stdin)}['$1'])"; }
export NF_READINGS_TABLE=$(get ReadingsTableName) NF_STATE_TABLE=$(get StateTableName) NF_APP_TABLE=$(get AppTableName)
WEBHOOK=$(get WebhookUrl)
TOKEN=$(aws ssm get-parameter --name /northfen-telemetry/webhook-shared-secret --with-decryption --region us-west-2 --query Parameter.Value --output text)

# 1. start a streamed scenario (webhook -> simulator Lambda -> Kinesis -> consumer -> SQS -> explain)
curl -sS -X POST "$WEBHOOK/simulate" -H "X-Northfen-Token: $TOKEN" -H 'Content-Type: application/json' -d '{"scenario":"10"}'
sleep 60
./bin/northfen -store dynamo feed -session webhook
./bin/northfen -store dynamo status NF-XXXXXXXX

# 2. or push raw readings straight onto the stream
./demo/webhook-example.sh "$WEBHOOK" "$TOKEN"

# 3. the hosted console
open "$(get DashboardUrl)"      # or https://marian.online/demos/northfen/ once the rewrite is live
```

Logs: `cd infra && sam logs -n ConsumerFunction --stack-name northfen-telemetry-pipeline --tail`
(also `ExplainFunction`, `SimulatorFunction`). Look for `nf_batch`, `nf_resolved` and `nf_action`.

## Local Lambda testing (optional, needs Docker)

```sh
cp infra/local-env.example.json infra/local-env.json   # fill in the stack outputs
cd infra && sam build
sam local invoke ConsumerFunction -e events/kinesis-batch.json
sam local invoke ExplainFunction  -e events/sqs-explain-job.json    # put a real alert id in it first
sam local invoke WebhookFunction  -e events/webhook-simulate.json   # put the SSM secret in the header first
```

These run the real handlers in a Lambda container against the deployed tables, stream and queue.
The same handlers are unit-tested with fake AWS clients in `internal/awsapp`, and driven end to end
by the local stand-ins with `northfen simulate <n> -via-kinesis`.

## Cost and cleanup

- **Kinesis:** one provisioned shard is about $11/month whether or not anyone runs the demo. That's
  the only always-on cost. (On-demand streams bill a higher per-stream-hour, so provisioned is
  cheaper here.)
- Everything else is pay-per-request: DynamoDB on-demand, Lambda, SQS, SNS and HTTP API cost cents at
  demo volume. Each flagged alert is one short Haiku call; quiet scenarios make none. The
  explainer's `MaximumConcurrency: 2`, 20 model calls and 12 runs per sandbox, and one streaming run
  per sandbox bound the spend.
- Tear down: `cd infra && sam delete` (no buckets to empty).

## Design notes

- **Webhook auth:** callers send the shared secret in `X-Northfen-Token`. The Lambda reads it from
  SSM, caches it once read, and rejects every request if it can't (fails closed; a transient SSM
  error isn't cached). `POST /readings` can't write into visitor sandbox sessions.
- **Ordering and replay:** partition key `session#equipment` plus `ParallelizationFactor: 1` means
  one tool's readings are scored in order by one invocation at a time. A failed batch is reported
  from its first record and retried; the consumer is idempotent (see the architecture doc).
- **Settle delay:** a new or changed alert is queued with `DelaySeconds` = settle time. The explain
  worker re-queues a job that isn't due yet (up to 6 times, 2 s apart), so correlated sensors are
  in the one model call.
- **Concurrency:** no reserved concurrency anywhere (it fails on accounts with a low Lambda
  concurrency quota). The SQS `MaximumConcurrency` and the per-sandbox limits bound the load.
- **Actions:** tickets are stubs (`TKT-…` in the audit trail and CloudWatch). Pages go to SNS
  `PageTopic` (set `OnCallEmail` to subscribe an address). Pages from visitor sandboxes are always
  recorded as simulated.
