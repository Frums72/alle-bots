"""
API Debug Script – testet LiveScore API direkt
Einfach auf Railway ausführen: python debug_api.py
"""
import requests
import json

API_KEY    = "OHvYYqv2LTNBi8qU"
API_SECRET = "G8lerfJK8OJ8TqMH7iG6Jb8u4V6n3wiK"
LS_BASE    = "https://livescore-api.com/api-client"
AUTH       = {"key": API_KEY, "secret": API_SECRET}

print("=" * 50)
print("  LiveScore API Debug")
print("=" * 50)

# ── Test 1: Live Matches ─────────────────────────────
print("\n[Test 1] Live Matches...")
try:
    resp = requests.get(f"{LS_BASE}/matches/live.json", params=AUTH, timeout=10)
    print(f"  Status: {resp.status_code}")
    data = resp.json()
    print(f"  Rohantwort (gekürzt): {json.dumps(data)[:500]}")

    matches = data.get("data", {}).get("match", []) or []
    print(f"  → {len(matches)} Live-Spiele gefunden")

    if matches:
        m = matches[0]
        print(f"  Beispiel: {m.get('home',{}).get('name')} vs {m.get('away',{}).get('name')}")
        print(f"  Status: {m.get('status')} | Zeit: {m.get('time')}")
    else:
        # Check if data structure is different
        print(f"  Data keys: {list(data.keys())}")
        if "data" in data:
            print(f"  data keys: {list(data['data'].keys()) if isinstance(data['data'], dict) else type(data['data'])}")
        if "error" in data:
            print(f"  ❌ API Fehler: {data['error']}")
        if "message" in data:
            print(f"  ❌ Nachricht: {data['message']}")
except Exception as e:
    print(f"  ❌ Exception: {e}")

# ── Test 2: Fixtures heute ───────────────────────────
from datetime import datetime, timezone, timedelta
heute = (datetime.now(timezone.utc) + timedelta(hours=2)).strftime("%Y-%m-%d")
print(f"\n[Test 2] Fixtures für heute ({heute})...")
try:
    resp = requests.get(f"{LS_BASE}/fixtures/matches.json",
                        params={**AUTH, "date": heute}, timeout=10)
    print(f"  Status: {resp.status_code}")
    data = resp.json()
    fixtures = data.get("data", {}).get("fixtures", []) or []
    print(f"  → {len(fixtures)} Fixtures heute")
    if fixtures:
        f = fixtures[0]
        print(f"  Beispiel: {f.get('home_name','?')} vs {f.get('away_name','?')} | {f.get('time','?')}")
    elif "error" in data:
        print(f"  ❌ Fehler: {data['error']}")
except Exception as e:
    print(f"  ❌ Exception: {e}")

# ── Test 3: API Quota ────────────────────────────────
print("\n[Test 3] API Quota...")
try:
    resp = requests.get(f"{LS_BASE}/account/quota.json", params=AUTH, timeout=10)
    print(f"  Status: {resp.status_code}")
    data = resp.json()
    print(f"  Quota: {json.dumps(data.get('data', data))[:300]}")
except Exception as e:
    print(f"  ❌ Exception: {e}")

print("\n" + "=" * 50)
print("Debug abgeschlossen!")
