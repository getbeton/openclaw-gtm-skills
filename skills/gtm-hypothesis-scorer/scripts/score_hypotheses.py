#!/usr/bin/env python3
"""
score_hypotheses.py — RICE score all GTM hypotheses from Supabase.

Score = Reach × Impact × Confidence  (Effort=1, always drops out)
Reach = addressable_companies × ACV

Deduplicates by segment × angle, keeping best per combo.
Outputs: terminal table + CSV to beton/gtm-outbound/csv/hypothesis_scores_latest.csv
"""
import csv, json, os, re, sys, urllib.request
from collections import defaultdict

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
WORKSPACE = os.path.abspath(os.path.join(SCRIPT_DIR, "../../../../"))
CSV_OUT = os.path.join(WORKSPACE, "beton/gtm-outbound/csv/hypothesis_scores_latest.csv")

SUPA_URL = "https://amygtwoqujluepibcnfs.supabase.co"
SUPA_KEY = "YOUR_SUPABASE_SERVICE_ROLE_KEY"
H = {"apikey": SUPA_KEY, "Authorization": f"Bearer {SUPA_KEY}"}

def get(path):
    req = urllib.request.Request(f"{SUPA_URL}/rest/v1/{path}", headers=H)
    return json.loads(urllib.request.urlopen(req).read())

def count_req(path):
    req = urllib.request.Request(
        f"{SUPA_URL}/rest/v1/{path}", headers={**H, "Prefer": "count=exact"}
    )
    resp = urllib.request.urlopen(req)
    cr = resp.headers.get("Content-Range", "0/0")
    return int(cr.split("/")[-1]) if "/" in cr else 0

# ── Scoring constants ─────────────────────────────────────────────────────────
ACV          = {"nano": 1500, "small": 6000, "mid": 20000, "large": 50000}
ADDR_PCT     = {"hiring_org_gap": 0.25, "pricing_model_gap": 0.15,
                "gtm_motion_gap": 0.20, "other": 0.15}
IMPACT_BASE  = {"hiring_org_gap": 9, "pricing_model_gap": 7,
                "gtm_motion_gap": 7, "other": 6}
GENERIC_VERTS = {"other", "enterprise_saas", ""}

def detect_angle(text):
    t = text.lower()
    if any(w in t for w in ["usage-based","freemium","tiered pric","transactional pric",
                              "billing","upgrade trigger","free tier","free-to-paid"]):
        return "pricing_model_gap"
    if any(w in t for w in ["hiring","ae ","sdr","ramp time","quota","headcount",
                              "new hire","new rep","new ae","sales rep"]):
        return "hiring_org_gap"
    if any(w in t for w in ["plg","hybrid gtm","self-serve","product-led","handoff",
                              "go-to-market","inbound","gtm motion","land and expand"]):
        return "gtm_motion_gap"
    return "other"

def count_domains(evidence):
    if not evidence:
        return 0
    return min(len(set(re.findall(r'\b\w+\.\w{2,4}\b', evidence))), 15)

def score_one(text, evidence, slug, seg_n):
    parts   = slug.replace("seed_series_a--", "").split("--")
    vertical = parts[0] if parts else "other"
    hc       = parts[2] if len(parts) > 2 else "nano"
    angle    = detect_angle(text)

    acv_val  = ACV.get(hc, 1500)
    ap       = ADDR_PCT[angle]
    if vertical in GENERIC_VERTS:
        ap *= 0.40
    addr    = int(seg_n * ap)
    reach   = addr * acv_val

    imp = IMPACT_BASE[angle]
    if vertical not in GENERIC_VERTS: imp = min(imp + 1, 10)
    if hc in ("mid", "large"):        imp = min(imp + 1, 10)

    conf = 8 if vertical not in GENERIC_VERTS else 5
    ev_n = count_domains(evidence)
    if ev_n >= 6:   conf = min(conf + 2, 10)
    elif ev_n >= 3: conf = min(conf + 1, 10)
    if angle == "hiring_org_gap" and vertical == "devtools_infra":
        conf = min(conf + 1, 10)

    score = reach * imp * conf
    return score, reach, addr, imp, conf, angle, vertical, hc, acv_val

# ── Fetch data ────────────────────────────────────────────────────────────────
print("Fetching hypotheses...", flush=True)
# Paginate — there can be >1000 hypotheses
hyps = []
offset = 0
while True:
    page = get(f"hypotheses?hypothesis_type=eq.data_grounded&select=id,hypothesis_text,evidence_base,personalization_hook,segment_id&limit=1000&offset={offset}")
    if not page:
        break
    hyps.extend(page)
    if len(page) < 1000:
        break
    offset += 1000
print(f"  {len(hyps)} hypotheses", flush=True)

seg_ids = list(set(h["segment_id"] for h in hyps if h.get("segment_id")))
print("Fetching segment slugs...", flush=True)
segs = {}
for i in range(0, len(seg_ids), 200):
    chunk = "(" + ",".join(seg_ids[i:i+200]) + ")"
    for s in get(f"segments?id=in.{chunk}&select=id,slug"):
        segs[s["id"]] = s["slug"]

print("Fetching segment company counts...", flush=True)
seg_counts = {}
for sid in seg_ids:
    seg_counts[sid] = count_req(f"company_segments?segment_id=eq.{sid}&select=company_id")

# ── Score & deduplicate ───────────────────────────────────────────────────────
best = {}  # key = (segment_id, angle) → best row
for h in hyps:
    sid = h.get("segment_id")
    if not sid: continue
    slug  = segs.get(sid, "")
    seg_n = seg_counts.get(sid, 0)
    if seg_n == 0: continue
    sc, reach, addr, imp, conf, angle, vertical, hc, acv_val = score_one(
        h.get("hypothesis_text",""), h.get("evidence_base",""), slug, seg_n
    )
    key = (sid, angle)
    if key not in best or sc > best[key]["score"]:
        best[key] = {
            "score": sc, "reach": reach, "addr": addr, "imp": imp, "conf": conf,
            "angle": angle, "vertical": vertical, "hc": hc, "acv": acv_val,
            "seg_n": seg_n, "slug": slug.replace("seed_series_a--",""),
            "text": h.get("hypothesis_text",""),
            "evidence": h.get("evidence_base",""),
            "hook": h.get("personalization_hook",""),
        }

ranked = sorted(best.values(), key=lambda x: -x["score"])

# ── Terminal output ───────────────────────────────────────────────────────────
print(f"\n{len(ranked)} unique segment×angle combinations scored\n")
hdr = f"{'#':<4} {'Score($M)':>10}  {'Reach($K)':>10}  {'Addr':>5}  {'I':>3}  {'C':>3}  {'ACV':>6}  {'Angle':<22}  Segment"
print(hdr)
print("-" * 110)
for i, r in enumerate(ranked[:20], 1):
    print(f"{i:<4} ${r['score']/1e6:>9.1f}M  ${r['reach']/1000:>8.0f}K  {r['addr']:>5}  {r['imp']:>3}  {r['conf']:>3}  ${r['acv']:>5}  {r['angle']:<22}  {r['slug'][:50]}")

# ── CSV output ────────────────────────────────────────────────────────────────
os.makedirs(os.path.dirname(CSV_OUT), exist_ok=True)
fields = ["rank","score","reach_usd","addressable_companies","acv","impact","confidence",
          "angle","segment_slug","hypothesis_text","evidence_base","personalization_hook"]
with open(CSV_OUT, "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=fields)
    w.writeheader()
    for i, r in enumerate(ranked, 1):
        w.writerow({
            "rank": i, "score": round(r["score"]), "reach_usd": r["reach"],
            "addressable_companies": r["addr"], "acv": r["acv"],
            "impact": r["imp"], "confidence": r["conf"],
            "angle": r["angle"], "segment_slug": r["slug"],
            "hypothesis_text": r["text"].replace("\n"," "),
            "evidence_base": r["evidence"].replace("\n"," "),
            "personalization_hook": r["hook"].replace("\n"," "),
        })

print(f"\n✓ CSV saved to: {CSV_OUT}")
print(f"  {len(ranked)} rows")
