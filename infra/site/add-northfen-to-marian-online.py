#!/usr/bin/env python3
"""Add the Northfen demo to marian.online (edits files in place, idempotent).

Usage (from anywhere):
  python3 add-northfen-to-marian-online.py <path-to-marian.online> <DashboardOrigin>

<DashboardOrigin> is the `DashboardOrigin` output of the northfen-telemetry-pipeline
stack, e.g. https://abc123.execute-api.us-west-2.amazonaws.com

What it changes:
  - vercel.json: three /demos/northfen rewrites (same shape as Rivergate's and
    Amberlight's), placed before the SPA fallback.
  - src/pages/DemosPage.tsx: the Northfen entry directly below Amberlight, with
    the same layout, badge, sections, stack chips and GitHub link. (Assumes the
    Amberlight script already ran, so `repoHref` exists.)
"""
import json
import sys
from pathlib import Path

if len(sys.argv) != 3 or not sys.argv[2].startswith("https://"):
    sys.exit(__doc__)
site, origin = Path(sys.argv[1]), sys.argv[2].rstrip("/")

# ---- vercel.json -------------------------------------------------------------
vp = site / "vercel.json"
v = json.loads(vp.read_text())
rewrites = [r for r in v["rewrites"] if not r["source"].startswith("/demos/northfen")]
fallback = next(i for i, r in enumerate(rewrites) if r["source"] == "/(.*)")
rewrites[fallback:fallback] = [
    {"source": "/demos/northfen", "destination": f"{origin}/demos/northfen/"},
    {"source": "/demos/northfen/", "destination": f"{origin}/demos/northfen/"},
    {"source": "/demos/northfen/:path+", "destination": f"{origin}/demos/northfen/:path+"},
]
v["rewrites"] = rewrites
vp.write_text(json.dumps(v, indent=2) + "\n")
print("vercel.json: northfen rewrites ->", origin)

# ---- DemosPage.tsx -----------------------------------------------------------
dp = site / "src" / "pages" / "DemosPage.tsx"
s = dp.read_text()
if "repoHref?: string" not in s:
    sys.exit("DemosPage.tsx: run add-amberlight-to-marian-online.py first (it adds repoHref and the GitHub link)")

NORTHFEN = """  {
    id: 'northfen',
    status: 'Live',
    title: 'Equipment anomaly diagnosis: Stream → Detect → Explain → Dispatch',
    forWho:
      "Semiconductor fabs and other high-margin manufacturers with hundreds of sensors per tool, where statistics already spot the anomaly but working out why still needs a senior engineer, and the 2am on-call engineer usually isn't one.",
    scenario:
      'Northfen Semiconductor is a fictional 310-person contract wafer fab partway through a capacity expansion: new lines, new tools and new operators, which is exactly when drift is hardest to catch. Etch, CVD, CMP and ion-implant tools stream temperature, vibration, particle, RF-power and pressure readings. Every tool, sensor and reading in the demo is synthetic.',
    steps: [
      {
        label: 'Stream',
        detail:
          'Sensor readings stream through Amazon Kinesis tick by tick. In the demo you pick a scenario and watch about 50 seconds of telemetry arrive live.',
      },
      {
        label: 'Detect',
        detail:
          "Plain statistics, no model: each sensor learns its own tool's normal, then rolling EWMA / z-score rules flag spikes, sustained excursions and slow drift. A frozen or silent sensor is flagged as a sensor fault. Every window is scored and stored, and correlated sensors on one tool are grouped into one incident.",
      },
      {
        label: 'Explain',
        detail:
          "Only after something is flagged, one bounded model call (Claude on Amazon Bedrock) reads every sensor on that tool plus its recent maintenance history, and returns ranked likely causes, what to check first, a severity and a confidence score. The model never decides whether something is anomalous.",
      },
      {
        label: 'Dispatch',
        detail:
          "A fixed table, not the model, turns severity and confidence into log, ticket or page. Explanations the model isn't sure about go to a human instead of being dismissed. On-call can acknowledge, escalate or dismiss with a note.",
      },
    ],
    capabilities: [
      'Detection is 100% deterministic and unit-tested: the model is called only on already-flagged windows, so quiet tools cost nothing and false alarms can\\'t be talked into existence',
      'Full audit trail for every alert: the detector rule and z-score, exactly what the model was shown, its answer, which dispatch rule fired, and who acknowledged it',
      'A different architecture from the two triage demos: a continuous stream with windowed state kept in DynamoDB between Lambda invocations, not one item classified at a time',
      'MCP tools let an AI agent work the anomaly feed. Read access is on by default and write access is opt-in per tool; no tool can change a threshold or pick the action',
    ],
    tryIt: [
      'Pick a scenario: normal operation, a slow drift, a sudden spike, a stuck sensor, or three sensors drifting together',
      'Watch the readings stream in and the windows get scored live. Normal scenarios should stay quiet',
      'When something flags, read the diagnosis, then acknowledge, escalate or dismiss it',
    ],
    stack: ['Go', 'Amazon Kinesis', 'AWS Lambda', 'DynamoDB', 'SQS', 'SNS', 'API Gateway', 'Amazon Bedrock', 'AWS SAM', 'MCP'],
    launchHref: '/demos/northfen/',
    repoHref: 'https://github.com/marianina8/northfen-telemetry-pipeline',
  },
"""
if "id: 'northfen'" not in s:
    anchor = "    repoHref: 'https://github.com/marianina8/amberlight-icr-pipeline',\n  },\n"
    if anchor not in s:
        sys.exit("DemosPage.tsx: Amberlight entry not found (run the Amberlight script first)")
    s = s.replace(anchor, anchor + NORTHFEN, 1)
dp.write_text(s)
print("DemosPage.tsx: Northfen entry below Amberlight")
