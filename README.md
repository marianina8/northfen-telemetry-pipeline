# northfen-telemetry-pipeline

A working AI workflow demo in Go on AWS: **streaming equipment-anomaly diagnosis for a
semiconductor fab.** All data is synthetic.

**Live demo:** [marian.online/demos](https://marian.online/demos) (password on request) ·
**Architecture:** [NORTHFEN-ARCHITECTURE.md](NORTHFEN-ARCHITECTURE.md)

## The prospect and the problem

**Northfen Semiconductor** is a fictional ~310-person contract wafer fab partway through a
capacity expansion: new production lines, new tooling and new operators. That's exactly when process
drift and equipment degradation are hardest to catch and most expensive to miss (think of the
GlobalFoundries Fab 8 ramp). Contract fabs run at high margins, they already invest heavily in
automation, and the incumbents in this space (PTC, Siemens, GE Digital) are slow-moving.

A fab has hundreds of sensors per tool. **Spotting that a reading is statistically anomalous is a
solved, cheap, deterministic problem**, and using an LLM for it would be a mistake. The
bottleneck is *diagnosis*. When three sensors on a tool drift together, a senior process engineer
can often tell you what's probably happening from experience. The on-call engineer at 2am
usually can't, and has to escalate or guess. That judgment call is what this demo automates.

> **AI does diagnostic synthesis over already-detected anomalies. It never decides whether
> something is anomalous.** Detection is plain windowed statistics in Go, unit-tested without
> Bedrock.

```
Kinesis stream → Go consumer Lambda: rolling EWMA / z-score per sensor (state in DynamoDB)
              → window crosses a threshold → one Bedrock "explain" call (likely causes,
                what to check first, severity, confidence)
              → fixed table: log / ticket / page on-call (SNS); uncertain → a human
              → live console: acknowledge, escalate, dismiss (full audit trail)
```

## What's different from the Rivergate and Amberlight demos

Those two are the same **Ingest → Classify → Route** pipeline: one discrete item, one model call
that classifies it, and a router. This one is a different architecture:

| | Rivergate / Amberlight (ICR) | Northfen |
|---|---|---|
| Input | one ticket / handoff at a time | a continuous multi-sensor stream (Kinesis) |
| Who decides "is this a problem?" | the model classifies every item | **statistics**. The model is never asked |
| When the model runs | on every item | only on windows the detector flagged. Quiet tools cost nothing |
| Model's job | label one item | synthesize a cause across *correlated* sensors plus the tool's maintenance history |
| State | per item | rolling per-sensor detector state, externalized in DynamoDB between stateless Lambda invocations |
| Hard parts | confidence thresholds, routing | windowing, replay-safe stream processing, grouping correlated flags into one incident, settle time before explaining |
| Demo feel | submit one item, see the result | pick a scenario and watch ~50 s of telemetry stream, get scored and flagged live |

What carries over on purpose: synthetic-only data, the phased local-first build, SAM, the
deterministic action table after the model, low confidence going to a human, the full audit
trail, and the MCP read/write split.

## Try it locally (no AWS needed)

```bash
make test                       # every scenario's detection outcome, the z = 3.0 boundary, replay safety, ...
make build
bin/northfen scenarios          # 10 synthetic scenarios and what each should do
bin/northfen simulate 10        # correlated drift on a CMP polisher → one alert → explained → page
bin/northfen simulate 06        # noisy but stable → nothing flagged, no model call
bin/northfen simulate 07 -via-kinesis   # through the local Kinesis/SQS stand-ins into the Lambda handlers
bin/northfen simulate 05 -live  # paced tick by tick
bin/northfen feed
bin/northfen explain W-…        # refuses windows the detector didn't flag
bin/northfen ack NF-… -escalate -note "operator reports bearing noise"
make dashboard                  # live console on http://127.0.0.1:8080
```

Add `-bedrock` (profile `demos-admin`) to any CLI or dashboard command to use the real model
instead of the offline mock explainer.

### Scenarios (`demo/scenarios/`, outcomes in `demo/expected.json`)

| # | Tool | Scenario | Expected |
|---|---|---|---|
| 01–03 | etch, CVD, implanter | normal operation | no flag |
| 04 | Etch Tool 12 | chamber pressure creeps up, never reaches the spec limit | drift anomaly, lower severity |
| 05 | CVD Chamber 4 | particle burst | spike anomaly, higher severity |
| 06 | CMP Polisher 7 | 3× the usual vibration noise, but stable | no flag (the harder false-positive test) |
| 07 | Ion Implanter 2 | vacuum gauge frozen at a normal-looking value | sensor fault → ticket, no model call |
| 08 | Etch Tool 12 | temperature channel goes silent for 8 readings | sensor fault → ticket, no model call |
| 09 | CVD Chamber 4 | noise-free fixture: z = 3.000 ×3 (flags) vs 3.000 ×2 then 2.997 ×3 (doesn't) | exactly one flag |
| 10 | CMP Polisher 7 | vibration ×2 and pad temperature drift together | ONE alert, three sensors; explanation must reference the correlation |

### Naive vs. adaptive (`compare/`)

`go run ./compare` runs every scenario through fixed spec-sheet limits and through the adaptive
detector. **Naive: 5/10 correct. Adaptive: 10/10.** The fixed limit misses the gradual drift,
false-alarms on the noisy-but-healthy tool, can't see a stuck or silent sensor, and sees the
three correlated sensors 17 and 51 ticks apart. See [compare/RESULTS.md](compare/RESULTS.md).
With `-bedrock` it also checks the live model's severity against `demo/expected.json`.

### MCP

```bash
bin/northfen mcp                                  # read-only: feed, status, get_sensor_history, list_scenarios
bin/northfen mcp -allow-write ack,escalate        # write tools are enabled one by one
```
Example client config: [demo/mcp-client-config.example.json](demo/mcp-client-config.example.json).

## Deploy

`infra/template.yaml` (SAM): Kinesis, five Go Lambdas (arm64), three DynamoDB tables, SQS, SNS
and two HTTP APIs, in stack `northfen-telemetry-pipeline`, us-west-2, profile `demos-admin`,
tagged `app=northfen-telemetry data=synthetic`. Runbook: [infra/README.md](infra/README.md).
