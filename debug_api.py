"""
API Debug Script v2 – testet alle möglichen Endpunkte
"""
import requests
import json

API_KEY    = "OHvYYqv2LTNBi8qU"
API_SECRET = "G8lerfJK8OJ8TqMH7iG6Jb8u4V6n3wiK"
LS_BASE    = "https://livescore-api.com/api-client"
AUTH       = {"key": API_KEY, "secret": API_SECRET}

print("=" * 55)
print("  LiveScore API Debug v2 - Endpunkt-Test")
print("=" * 55)

# Test 1: Alle moeglichen Live-Endpunkte
print("\n[Test 1] Alle Live-Endpunkte...")
endpunkte = [
    "/matches/live.json",
    "/scores/live.json",
    "/livescores/live.json",
]
for ep in endpunkte:
    try:
        resp = requests.get(f"{LS_BASE}{ep}", params=AUTH, timeout=10)
        data = resp.json()
        ok   = data.get("success", False)
        d    = data.get("data") or {}
        cnt  = len(d.get("match", []) or []) if isinstance(d, dict) else 0
        print(f"  {ep}: HTTP={resp.status_code} | success={ok} | Spiele={cnt}")
        if not ok:
            print(f"    Fehler: {data.get('error','?')}")
        if cnt > 0:
            print(f"    SPIELE GEFUNDEN! Erste 300 Zeichen: {resp.text[:300]}")
    except Exception as e:
        print(f"  {ep}: FEHLER {e}")

# Test 2: Parameter-Varianten
print("\n[Test 2] Parameter-Varianten auf /matches/live.json...")
varianten = [
    {},
    {"page": 1},
    {"per_page": 100},
    {"page": 1, "per_page": 100},
]
for params in varianten:
    try:
        p    = {**AUTH, **params}
        resp = requests.get(f"{LS_BASE}/matches/live.json", params=p, timeout=10)
        data = resp.json()
        d    = data.get("data") or {}
        cnt  = len(d.get("match", []) or []) if isinstance(d, dict) else 0
        total = d.get("total", "?") if isinstance(d, dict) else "?"
        print(f"  {params}: Spiele={cnt} | total={total} | success={data.get('success')}")
    except Exception as e:
        print(f"  {params}: FEHLER {e}")

# Test 3: Komplette Rohantwort
print("\n[Test 3] Komplette Rohantwort (erste 1500 Zeichen)...")
try:
    resp = requests.get(f"{LS_BASE}/matches/live.json", params=AUTH, timeout=10)
    print(resp.text[:1500])
except Exception as e:
    print(f"FEHLER: {e}")

# Test 4: Competitions
print("\n[Test 4] Verfuegbare Competitions...")
try:
    resp  = requests.get(f"{LS_BASE}/competitions/list.json",
                         params={**AUTH, "active": "true"}, timeout=10)
    data  = resp.json()
    comps = (data.get("data") or {})
    if isinstance(comps, dict):
        comps = comps.get("competition", []) or []
    elif isinstance(comps, list):
        pass
    else:
        comps = []
    print(f"  {len(comps)} Competitions gefunden")
    for c in comps[:15]:
        print(f"  - {c.get('name','?')} | ID:{c.get('id','?')} | Aktiv:{c.get('active','?')}")
except Exception as e:
    print(f"FEHLER: {e}")

print("\n" + "=" * 55)
print("Debug abgeschlossen!")
