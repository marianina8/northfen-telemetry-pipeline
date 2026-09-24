#!/usr/bin/env python3
"""Add the Northfen demo to marian.online (edits files in place, idempotent).

Usage (from anywhere):
  python3 add-northfen-to-marian-online.py <path-to-marian.online> <DashboardOrigin>

<DashboardOrigin> is the `DashboardOrigin` output of the northfen-telemetry-pipeline
stack, e.g. https://abc123.execute-api.us-west-2.amazonaws.com

What it changes:
  - vercel.json: three /demos/northfen rewrites (same shape as Rivergate's and
    Amberlight's), placed before the SPA fallback.
  - src/pages/DemosPage.tsx: the Northfen (render farm) entry. If a Northfen
    entry already exists (e.g. the earlier fab version) it is replaced in place;
    otherwise it goes directly below Amberlight. Same layout, badge, sections,
    stack chips and GitHub link. (Assumes the Amberlight script already ran, so
    `repoHref` exists.)
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
want = [
    {"source": "/demos/northfen", "destination": f"{origin}/demos/northfen/"},
    {"source": "/demos/northfen/", "destination": f"{origin}/demos/northfen/"},
    {"source": "/demos/northfen/:path+", "destination": f"{origin}/demos/northfen/:path+"},
]
have = [r for r in v["rewrites"] if r["source"].startswith("/demos/northfen")]
if have == want:
    print("vercel.json: northfen rewrites already point at", origin, "(unchanged)")
else:
    rewrites = [r for r in v["rewrites"] if not r["source"].startswith("/demos/northfen")]
    fallback = next(i for i, r in enumerate(rewrites) if r["source"] == "/(.*)")
    rewrites[fallback:fallback] = want
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
    title: 'Render farm anomaly diagnosis: Stream → Detect → Explain → Dispatch',
    forWho:
      "Animation and VFX studios whose render farm has to turn every day's work into frames by morning, where monitoring already spots that something is off but working out why still needs a senior render wrangler, and the 2am on-call wrangler usually isn't one.",
    scenario:
      'Northfen Studios is a fictional 250-person animation and VFX studio in the middle of a season-2 capacity push: new render nodes, a new compositing pool, and new driver images rolling out. Lighting, FX and compositing pools stream frame times, GPU and CPU temperature, memory, storage latency, failed frames and license waits. Every pool, node and metric in the demo is synthetic.',
    steps: [
      {
        label: 'Stream',
        detail:
          'Render-farm metrics stream through Amazon Kinesis tick by tick. In the demo you pick a scenario and watch about 50 seconds of a night on the farm arrive live.',
      },
      {
        label: 'Detect',
        detail:
          "Plain statistics, no model: each metric learns its own normal, then rolling EWMA / z-score rules flag spikes, sustained excursions and slow drift. A frozen or silent metric is flagged as a monitoring fault. Every window is scored and stored, and metrics that move together on one render pool are grouped into one incident.",
      },
      {
        label: 'Explain',
        detail:
          "Only after something is flagged, one bounded model call (Claude on Amazon Bedrock) reads every metric on that pool plus its recent changes and incidents, and returns ranked likely causes (a bad node, a driver roll-out, a heavy asset publish, shared storage), what to check first, a severity and a confidence score. It diagnoses infrastructure only: it never decides whether something is anomalous, and never judges the work.",
      },
      {
        label: 'Dispatch',
        detail:
          "A fixed table, not the model, turns severity and confidence into log, ticket or page. Explanations the model isn't sure about go to a human instead of being dismissed. On-call can acknowledge, escalate or dismiss with a note.",
      },
    ],
    capabilities: [
      'Detection is 100% deterministic and unit-tested: the model is called only on already-flagged windows, so a quiet night costs nothing and false alarms can\\'t be talked into existence',
      'Operations, not art: the model reads infrastructure metrics and is instructed never to comment on the creative content or quality of any shot',
      'Full audit trail for every alert: the detector rule and z-score, exactly what the model was shown, its answer, which dispatch rule fired, and who acknowledged it',
      'MCP tools let an AI agent work the anomaly feed. Read access is on by default and write access is opt-in per tool; no tool can change a threshold or pick the action',
    ],
    tryIt: [
      'Pick a scenario: a normal night, a node slowing after a driver update, a burst of failed frames, a frozen license monitor, or the whole pool slowing down at once',
      'Watch the metrics stream in and the windows get scored live. Normal scenarios should stay quiet',
      'When something flags, read the diagnosis, then acknowledge, escalate or dismiss it',
    ],
    stack: ['Go', 'Amazon Kinesis', 'AWS Lambda', 'DynamoDB', 'SQS', 'SNS', 'API Gateway', 'Amazon Bedrock', 'AWS SAM', 'MCP'],
    launchHref: '/demos/northfen/',
    repoHref: 'https://github.com/marianina8/northfen-telemetry-pipeline',
  },
"""
END = "    repoHref: 'https://github.com/marianina8/northfen-telemetry-pipeline',\n  },\n"
if "id: 'northfen'" in s:
    # Replace the existing entry in place (it was first added as the fab version).
    a = s.rindex("  {\n", 0, s.index("id: 'northfen'"))
    b = s.index(END, a) + len(END)
    s = s[:a] + NORTHFEN + s[b:]
else:
    anchor = "    repoHref: 'https://github.com/marianina8/amberlight-icr-pipeline',\n  },\n"
    if anchor not in s:
        sys.exit("DemosPage.tsx: Amberlight entry not found (run the Amberlight script first)")
    s = s.replace(anchor, anchor + NORTHFEN, 1)
dp.write_text(s)
print("DemosPage.tsx: Northfen (render farm) entry in place")
