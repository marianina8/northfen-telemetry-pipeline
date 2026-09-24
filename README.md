# northfen-telemetry-pipeline

A working AI workflow demo in Go on AWS: **streaming anomaly diagnosis for an animation/VFX
studio's render farm.** All data is synthetic.

**Live demo:** [marian.online/demos](https://marian.online/demos) (password on request) ·
**Architecture:** [NORTHFEN-ARCHITECTURE.md](NORTHFEN-ARCHITECTURE.md)

## The prospect and the problem

**Northfen Studios** is a fictional ~250-person animation and VFX studio in the middle of a
season-2 capacity push: new render nodes, a new compositing pool, and new driver and node images
rolling out. Every night the render farm has to turn the day's work into frames in time for
morning dailies. Studios already pay for serious render infrastructure, and a night of lost
frames costs a day of a whole team's schedule.

Every render node, and the shared storage, license server and render manager behind it, streams
metrics: frame time, GPU/CPU temperature, memory, storage latency, failed frames, license waits.
**Spotting that a metric is statistically unusual is a solved, cheap, deterministic problem**, and
using an LLM for it would be a mistake. The bottleneck is *diagnosis*. When frame times climb on
several nodes at once, a senior render wrangler can usually tell whether it's one bad node, a
driver roll-out, a heavy asset publish, or shared storage choking. The on-call wrangler at 2am
often can't, and escalates or guesses. That judgment call is what this demo automates.

> **AI does operational diagnosis over already-detected anomalies. It never decides whether
> something is anomalous, and it never judges the work.** Detection is plain windowed statistics
> in Go, unit-tested without Bedrock. The model diagnoses infrastructure; it is told never to
> comment on the creative content or quality of any shot.

```
Kinesis stream → Go consumer Lambda: rolling EWMA / z-score per metric (state in DynamoDB)
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
| Input | one ticket / handoff at a time | a continuous multi-metric stream from the render farm (Kinesis) |
| Who decides "is this a problem?" | the model classifies every item | **statistics**. The model is never asked |
| When the model runs | on every item | only on windows the detector flagged. A quiet night costs nothing |
| Model's job | label one item | synthesize a cause across *correlated* metrics plus the pool's recent changes and incidents |
| State | per item | rolling per-metric detector state, externalized in DynamoDB between stateless Lambda invocations |
| Hard parts | confidence thresholds, routing | windowing, replay-safe stream processing, grouping correlated flags into one incident, settle time before explaining |
| Demo feel | submit one item, see the result | pick a scenario and watch ~50 s of farm telemetry stream, get scored and flagged live |

What carries over on purpose: synthetic-only data, the phased local-first build, SAM, the
deterministic action table after the model, low confidence going to a human, the full audit
trail, and the MCP read/write split.

## Try it locally (no AWS needed)

```bash
make test                       # every scenario's detection outcome, the z = 3.0 boundary, replay safety, ...
make build
bin/northfen scenarios          # 10 synthetic scenarios and what each should do
bin/northfen simulate 10        # whole lighting pool slows with NAS latency → one alert → "storage" → page
bin/northfen simulate 06        # bursty but healthy comp pool → nothing flagged, no model call
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

| # | Render pool | Scenario | Expected |
|---|---|---|---|
| 01–03 | lighting, FX, shared services | normal night | no flag |
| 04 | Lighting pool | node12's frame time creeps up after a GPU driver update, never reaches the alert limit | drift anomaly, lower severity |
| 05 | FX pool | burst of failed frames after an asset publish | spike anomaly, higher severity |
| 06 | Compositing pool | 3× the usual swing on light vs heavy shots, but stable | no flag (the harder false-positive test) |
| 07 | Shared services | license monitor frozen at a normal-looking seat count | monitoring fault → ticket, no model call |
| 08 | Lighting pool | node07 goes silent for 8 readings | monitoring fault (both of its metrics, one alert) → ticket, no model call |
| 09 | FX pool | noise-free fixture: z = 3.000 ×3 (flags) vs 3.000 ×2 then 2.997 ×3 (doesn't) | exactly one flag |
| 10 | Lighting pool | NAS latency and both nodes' frame times rise together, GPU temp flat | ONE alert, three metrics; the explanation must point at shared storage |

### Naive vs. adaptive (`compare/`)

`go run ./compare` runs every scenario through fixed alert limits and through the adaptive
detector. **Naive: 5/10 correct. Adaptive: 10/10.** The fixed limits miss the driver drift,
false-alarm on the bursty-but-healthy compositing pool, and can't see a frozen or silent metric.
In the storage slowdown they page about two slow nodes first and only see the storage latency ~40
ticks later; the adaptive detector flags storage first and groups all three into one incident.
See [compare/RESULTS.md](compare/RESULTS.md). With `-bedrock` it also checks the live model's
severity against `demo/expected.json`.

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
