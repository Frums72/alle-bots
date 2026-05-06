# v20 - 4 neue Bots: Druck, Comeback, Torflut, Rote Karte
import requests
import re
import time
import threading
from datetime import datetime, timezone, timedelta

# ============================================================
#  KONFIGURATION
# ============================================================
API_KEY            = "INUnk7eRsptCrMNq"
API_SECRET         = "h2wf08YErEQbSAfAn9XIgbzJB3l3P9u6"

TELEGRAM_BOT_TOKEN = "8706066107:AAFAQhT3k0jhTZ7ep-VWHPlskOKJVvsfucQ"
TELEGRAM_CHAT_ID   = "7272001004"

DISCORD_WEBHOOK_ECKEN     = "https://discord.com/api/webhooks/1501122762096377957/OqjCXNqBBnMvaQlSz5npaYYnjbWpdh3DENhPE7aJr1ZA_WgGo0PkRRG6ZFZURi9X1CK4"
DISCORD_WEBHOOK_KARTEN    = "https://discord.com/api/webhooks/1501250542788280451/BZ6r8Y2SEDPgya9skt8Gyzbsoetvq0yPY6pWG5HrUzK9moeL-RXYWAiEwWuIlEy7GBfM"
DISCORD_WEBHOOK_TORWART   = "https://discord.com/api/webhooks/1501251703025041531/QDS0RBUuG8PNRNaDFB02dAHP1miwhixrAfxUw8HhDswt6ce-hIHUootC4GhmjKP9A6b1"
DISCORD_WEBHOOK_BILANZ    = "https://discord.com/api/webhooks/1501251926564667564/fdBE4jOLislDfwpMs2cUURm_4_YzfATKWFmaOjRNEXulHCJu1DB-lUBLqLmm73l-HQ4v"
# Neue Webhooks – bitte in Discord erstellen und hier eintragen:
DISCORD_WEBHOOK_DRUCK     = "https://discord.com/api/webhooks/1501252266630316163/aBo4o0HDN_Fh3eVj-WEvRZlzo970OQJcO1g6vKk4gJJ6hfRxco98m0p5KXDEQ-NBEZr1"
DISCORD_WEBHOOK_COMEBACK  = "https://discord.com/api/webhooks/1501252266630316163/aBo4o0HDN_Fh3eVj-WEvRZlzo970OQJcO1g6vKk4gJJ6hfRxco98m0p5KXDEQ-NBEZr1"
DISCORD_WEBHOOK_TORFLUT   = "https://discord.com/api/webhooks/1501252266630316163/aBo4o0HDN_Fh3eVj-WEvRZlzo970OQJcO1g6vKk4gJJ6hfRxco98m0p5KXDEQ-NBEZr1"
DISCORD_WEBHOOK_ROTKARTE  = "https://discord.com/api/webhooks/1501252266630316163/aBo4o0HDN_Fh3eVj-WEvRZlzo970OQJcO1g6vKk4gJJ6hfRxco98m0p5KXDEQ-NBEZr1"

ODDS_API_KEY       = "866948de5d6c34ca51faf6bd77e0bb2a"
EINSATZ            = 10.0

# Bestehende Parameter
MAX_CORNERS          = 5
MIN_KARTEN           = 2
KARTEN_BIS_MINUTE    = 40
MIN_SHOTS_ON_TARGET  = 3
FUSSBALL_INTERVAL    = 3
TAGESBERICHT_UHRZEIT = 0

# Neue Parameter
MIN_DRUCK_ECKEN      = 6    # Mindest-Ecken gesamt für Druck-Signal
DRUCK_RATIO          = 2.5  # Dominantes Team muss X-mal mehr Ecken haben
COMEBACK_AB_MINUTE   = 30   # Ab welcher Minute Comeback-Signal prüfen
TORFLUT_MIN_TORE     = 3    # Mindest-Tore in HZ1 für Torflut-Signal
# ============================================================

LS_BASE = "https://livescore-api.com/api-client"
LS_AUTH = {"key": API_KEY, "secret": API_SECRET}

KARTEN_TYPEN   = {"Yellow Card", "Red Card", "Yellow Red Card"}
ROTKARTE_TYPEN = {"Red Card", "Yellow Red Card"}

# Shared Cache
_cache_matches   = []
_cache_timestamp = 0
_cache_lock      = threading.Lock()
CACHE_TTL        = 45

notified_ecken      = set()
notified_ecken_over = set()
notified_karten     = set()
notified_torwart    = set()
notified_druck      = set()
notified_comeback   = set()
notified_torflut    = set()
notified_rotkarte   = set()
beobachtete_spiele  = {}
auswertung_done     = set()

statistik = {
    "ecken":    {"gewonnen": 0, "verloren": 0, "gewinn": 0.0},
    "ecken_over": {"gewonnen": 0, "verloren": 0, "gewinn": 0.0},
    "karten":   {"gewonnen": 0, "verloren": 0, "gewinn": 0.0},
    "torwart":  {"gewonnen": 0, "verloren": 0, "gewinn": 0.0},
    "druck":    {"gewonnen": 0, "verloren": 0, "gewinn": 0.0},
    "comeback": {"gewonnen": 0, "verloren": 0, "gewinn": 0.0},
    "torflut":  {"gewonnen": 0, "verloren": 0, "gewinn": 0.0},
    "rotkarte": {"gewonnen": 0, "verloren": 0, "gewinn": 0.0},
}
wochen_statistik = {k: {"gewonnen": 0, "verloren": 0, "gewinn": 0.0} for k in statistik}
tagesbericht_gesendet = None
STATISTIK_DATEI = "statistik.json"

# ============================================================
#  STATISTIK SPEICHERN / LADEN
# ============================================================

def statistik_laden():
    global statistik, wochen_statistik, tagesbericht_gesendet
    import json, os
    if not os.path.exists(STATISTIK_DATEI):
        return
    try:
        with open(STATISTIK_DATEI, "r") as f:
            data = json.load(f)
        for k in statistik:
            if k in data.get("statistik", {}):
                statistik[k] = data["statistik"][k]
        for k in wochen_statistik:
            if k in data.get("wochen_statistik", {}):
                wochen_statistik[k] = data["wochen_statistik"][k]
        if data.get("tagesbericht_gesendet"):
            from datetime import date
            tagesbericht_gesendet = date.fromisoformat(data["tagesbericht_gesendet"])
        print(f"  [Statistik] Geladen aus {STATISTIK_DATEI}")
    except Exception as e:
        print(f"  [Statistik] Ladefehler: {e}")

def statistik_speichern():
    import json
    try:
        data = {
            "statistik": statistik,
            "wochen_statistik": wochen_statistik,
            "tagesbericht_gesendet": str(tagesbericht_gesendet) if tagesbericht_gesendet else None,
        }
        with open(STATISTIK_DATEI, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        print(f"  [Statistik] Speicherfehler: {e}")
# ============================================================
#  HILFSFUNKTIONEN
# ============================================================

def jetzt():
    return (datetime.now(timezone.utc) + timedelta(hours=2)).strftime("%H:%M")

def heute():
    return (datetime.now(timezone.utc) + timedelta(hours=2)).strftime("%d.%m.%Y")

def de_now():
    return datetime.now(timezone.utc) + timedelta(hours=2)

def parse_score(score_str):
    """Gibt (tore_heim, tore_gast) zurück, z.B. '2 - 1' → (2, 1)"""
    try:
        parts = score_str.replace(" ", "").split("-")
        return int(parts[0]), int(parts[1])
    except:
        return 0, 0

def html_zu_discord(text):
    text = text.replace("<b>", "**").replace("</b>", "**")
    text = re.sub(r"<[^>]+>", "", text)
    return text

def send_telegram(message: str):
    url     = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    resp    = requests.post(url, json=payload, timeout=10)
    if resp.status_code != 200:
        print(f"  [Telegram Fehler] {resp.text}")

def send_discord_embed(webhook_url: str, embed: dict):
    if not webhook_url or webhook_url.startswith("DISCORD"):
        return
    resp = requests.post(webhook_url, json={"embeds": [embed]}, timeout=10)
    if resp.status_code not in (200, 204):
        print(f"  [Discord Fehler] {resp.status_code}: {resp.text}")

def send_discord(webhook_url: str, message: str):
    if not webhook_url or webhook_url.startswith("DISCORD"):
        return
    discord_msg = html_zu_discord(message)
    resp = requests.post(webhook_url, json={"content": discord_msg}, timeout=10)
    if resp.status_code not in (200, 204):
        print(f"  [Discord Fehler] {resp.status_code}: {resp.text}")

def karten_emoji(typ: str) -> str:
    t = typ.lower().replace(" ", "").replace("_", "")
    if "yellowred" in t: return "🟨🟥"
    if "red"       in t: return "🟥"
    if "yellow"    in t: return "🟨"
    return "🃏"

def api_get_with_retry(url: str, params: dict, max_retries: int = 3) -> requests.Response:
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            return resp
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response else 0
            if status in (503, 502, 504) and attempt < max_retries - 1:
                wait = 2 ** attempt
                print(f"  [API] {status} Fehler – Retry {attempt+1}/{max_retries-1} in {wait}s")
                time.sleep(wait)
            else:
                raise
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            if attempt < max_retries - 1:
                wait = 2 ** attempt
                print(f"  [API] Verbindungsfehler – Retry {attempt+1}/{max_retries-1} in {wait}s: {e}")
                time.sleep(wait)
            else:
                raise
    raise RuntimeError("Alle Retry-Versuche fehlgeschlagen")

# ============================================================
#  API-FUNKTIONEN
# ============================================================

def ls_get_live_matches():
    resp    = api_get_with_retry(f"{LS_BASE}/matches/live.json", LS_AUTH)
    matches = resp.json().get("data", {}).get("match", []) or []
    for m in matches:
        if "id" in m:
            m["id"] = str(m["id"])
    return matches

def ls_get_statistiken(match_id):
    params = {**LS_AUTH, "match_id": match_id}
    resp   = api_get_with_retry(f"{LS_BASE}/statistics/matches.json", params)
    stats  = resp.json().get("data", [])
    result = {
        "corners_home": 0, "corners_away": 0,
        "shots_on_target_home": 0, "shots_on_target_away": 0,
        "saves_home": 0, "saves_away": 0,
        "possession_home": "?", "possession_away": "?",
        "dangerous_attacks_home": 0, "dangerous_attacks_away": 0,
        "free_kicks_home": 0, "free_kicks_away": 0,
    }
    for s in stats:
        val_h = int(s.get("home") or 0)
        val_a = int(s.get("away") or 0)
        typ   = s.get("type", "").lower().replace(" ", "_")
        if typ == "corners":
            result["corners_home"] = val_h
            result["corners_away"] = val_a
        elif typ in ("shots_on_target", "on_target", "shots_on_goal"):
            result["shots_on_target_home"] = val_h
            result["shots_on_target_away"] = val_a
        elif typ == "saves":
            result["saves_home"] = val_h
            result["saves_away"] = val_a
        elif typ in ("possession", "possesion", "ball_possession"):
            result["possession_home"] = str(val_h)
            result["possession_away"] = str(val_a)
        elif typ == "dangerous_attacks":
            result["dangerous_attacks_home"] = val_h
            result["dangerous_attacks_away"] = val_a
        elif typ == "free_kicks":
            result["free_kicks_home"] = val_h
            result["free_kicks_away"] = val_a
    return result

def ls_get_events(match_id):
    params = {**LS_AUTH, "id": match_id}
    resp   = api_get_with_retry(f"{LS_BASE}/matches/events.json", params)
    events = resp.json().get("data", {}).get("event", []) or []
    if events:
        print(f"  [Events-Debug] Match {match_id} | Beispiel-Event: {events[0]}")
    else:
        print(f"  [Events-Debug] Match {match_id} | Keine Events")
    return events

def ls_get_single_match(match_id):
    params = {**LS_AUTH, "id": match_id}
    resp   = api_get_with_retry(f"{LS_BASE}/matches/single.json", params)
    return resp.json().get("data", {}).get("match", {})

def get_live_matches():
    global _cache_matches, _cache_timestamp
    with _cache_lock:
        now = time.time()
        if now - _cache_timestamp > CACHE_TTL:
            _cache_matches   = ls_get_live_matches()
            _cache_timestamp = now
            print(f"  [Cache] {len(_cache_matches)} Spiele geladen")
        return list(_cache_matches)

def get_statistiken(match_id): return ls_get_statistiken(match_id)
def get_events(match_id):      return ls_get_events(match_id)

def get_quote(home, away, typ):
    if not ODDS_API_KEY:
        return None
    try:
        url    = "https://api.the-odds-api.com/v4/sports/soccer/odds/"
        params = {"apiKey": ODDS_API_KEY, "regions": "eu",
                  "markets": "totals", "oddsFormat": "decimal"}
        resp   = requests.get(url, params=params, timeout=8)
        if resp.status_code != 200:
            return None
        for game in resp.json():
            h = game.get("home_team", "").lower()
            a = game.get("away_team", "").lower()
            if home.lower()[:4] in h or away.lower()[:4] in a:
                for bookmaker in game.get("bookmakers", [])[:1]:
                    for market in bookmaker.get("markets", []):
                        if market.get("key") == "totals":
                            for outcome in market.get("outcomes", []):
                                return round(outcome.get("price", 0), 2)
        return None
    except:
        return None

# ============================================================
#  DISCORD EMBEDS
# ============================================================

FARBE_ECKEN               = 0xF4A300
FARBE_ECKEN_OVER          = 0x9B59B6
FARBE_KARTEN              = 0xE74C3C
FARBE_TORWART             = 0x1ABC9C
FARBE_DRUCK               = 0x3498DB
FARBE_COMEBACK            = 0xE67E22
FARBE_TORFLUT             = 0xFF6B6B
FARBE_ROTKARTE            = 0xC0392B
FARBE_AUSWERTUNG_GEWONNEN = 0x2ECC71
FARBE_AUSWERTUNG_VERLOREN = 0xE74C3C

def discord_ecken_tipp(home, away, comp, country, score, corners_home, corners_away, corners, grenze, quote):
    qt = f"\n💶 **Quote:** {quote}" if quote else ""
    return {
        "title": "📐 Ecken Tipp", "color": FARBE_ECKEN,
        "fields": [
            {"name": "🏆 Liga",              "value": f"{comp} ({country})", "inline": True},
            {"name": "⚽ Spiel",             "value": f"{home} vs {away}",   "inline": True},
            {"name": "📊 Halbzeitstand",     "value": f"**{score}**",        "inline": True},
            {"name": "📐 Ecken zur Halbzeit",
             "value": f"🔵 {home}: **{corners_home}**\n🔴 {away}: **{corners_away}**\n📊 Gesamt: **{corners}**", "inline": False},
            {"name": "🎯 Empfehlung", "value": f"Unter **{grenze} Ecken** (Gesamtspiel){qt}", "inline": False},
        ],
        "footer": {"text": f"Fixture • {heute()} {jetzt()}"},
    }

def discord_ecken_over_tipp(home, away, comp, country, score, minute, corners_home, corners_away, corners, quote):
    qt = f"\n💶 **Quote:** {quote}" if quote else ""
    return {
        "title": "📐 Ecken ÜBER Tipp", "color": FARBE_ECKEN_OVER,
        "fields": [
            {"name": "🏆 Liga",         "value": f"{comp} ({country})",               "inline": True},
            {"name": "⚽ Spiel",        "value": f"{home} vs {away}",                 "inline": True},
            {"name": "📊 Stand",        "value": f"**{score}** | Min. **{minute}'**", "inline": True},
            {"name": "📐 Ecken bisher",
             "value": f"🔵 {home}: **{corners_home}**\n🔴 {away}: **{corners_away}**\n📊 Gesamt: **{corners}**", "inline": False},
            {"name": "🎯 Empfehlung", "value": f"Über **14 Ecken** (Gesamtspiel){qt}", "inline": False},
        ],
        "footer": {"text": f"Fixture • {heute()} {jetzt()}"},
    }

def discord_karten_tipp(home, away, comp, country, score, minute, karten_liste, quote):
    qt = f"\n💶 **Quote:** {quote}" if quote else ""
    return {
        "title": "🃏 Karten Signal", "color": FARBE_KARTEN,
        "fields": [
            {"name": "🏆 Liga",  "value": f"{comp} ({country})",               "inline": True},
            {"name": "⚽ Spiel", "value": f"{home} vs {away}",                 "inline": True},
            {"name": "📊 Stand", "value": f"**{score}** | Min. **{minute}'**", "inline": True},
            {"name": "🃏 Karten", "value": "\n".join(karten_liste) or "–",     "inline": False},
            {"name": "🎯 Empfehlung", "value": f"Über **5 Karten** gesamt{qt}", "inline": False},
        ],
        "footer": {"text": f"Fixture • {heute()} {jetzt()}"},
    }

def discord_torwart_tipp(home, away, comp, country, shots_home, shots_away, saves_h, saves_a, poss_h, poss_a, min_text, quote):
    qt = f"\n💶 **Quote:** {quote}" if quote else ""
    return {
        "title": "🧤 Torwart Alarm", "color": FARBE_TORWART,
        "fields": [
            {"name": "🏆 Liga",             "value": f"{comp} ({country})",                                        "inline": True},
            {"name": "⚽ Spiel",            "value": f"{home} vs {away}",                                          "inline": True},
            {"name": "📊 Stand",            "value": f"**0:0** | {min_text}",                                      "inline": True},
            {"name": "🎯 Schüsse aufs Tor", "value": f"Gesamt: **{shots_home+shots_away}** ({shots_home}|{shots_away})", "inline": True},
            {"name": "🧤 Paraden",          "value": f"Gesamt: **{saves_h+saves_a}** ({saves_h}|{saves_a})",       "inline": True},
            {"name": "⚽ Ballbesitz",       "value": f"{poss_h}% | {poss_a}%",                                     "inline": True},
            {"name": "🎯 Empfehlung", "value": f"Mindestens **1 Tor** fällt noch{qt}", "inline": False},
        ],
        "footer": {"text": f"Fixture • {heute()} {jetzt()}"},
    }

def discord_druck_tipp(home, away, comp, country, score, minute, druck_team,
                        ecken_stark, ecken_schwach, fk_stark, fk_schwach, quote):
    qt = f"\n💶 **Quote:** {quote}" if quote else ""
    return {
        "title": "🔥 Druck Signal", "color": FARBE_DRUCK,
        "fields": [
            {"name": "🏆 Liga",        "value": f"{comp} ({country})",               "inline": True},
            {"name": "⚽ Spiel",       "value": f"{home} vs {away}",                 "inline": True},
            {"name": "📊 Stand",       "value": f"**{score}** | Min. **{minute}'**", "inline": True},
            {"name": "🔥 Dominantes Team", "value": f"**{druck_team}**",             "inline": False},
            {"name": "📐 Ecken",       "value": f"**{ecken_stark}** : {ecken_schwach}", "inline": True},
            {"name": "🦵 Freistöße",  "value": f"**{fk_stark}** : {fk_schwach}",    "inline": True},
            {"name": "🎯 Empfehlung", "value": f"Nächste Ecke / Tor für **{druck_team}**{qt}", "inline": False},
        ],
        "footer": {"text": f"Fixture • {heute()} {jetzt()}"},
    }

def discord_comeback_tipp(home, away, comp, country, score, minute,
                           rueckliegend, fuehrend, shots_r, shots_f, poss_r, quote):
    qt = f"\n💶 **Quote:** {quote}" if quote else ""
    return {
        "title": "🔄 Comeback Signal", "color": FARBE_COMEBACK,
        "fields": [
            {"name": "🏆 Liga",          "value": f"{comp} ({country})",               "inline": True},
            {"name": "⚽ Spiel",         "value": f"{home} vs {away}",                 "inline": True},
            {"name": "📊 Stand",         "value": f"**{score}** | Min. **{minute}'**", "inline": True},
            {"name": "📉 Rückliegendes Team", "value": f"**{rueckliegend}**",          "inline": True},
            {"name": "📈 Führendes Team",     "value": f"**{fuehrend}**",              "inline": True},
            {"name": "🎯 Schüsse (Rückliegend)", "value": f"**{shots_r}** aufs Tor",  "inline": True},
            {"name": "⚽ Ballbesitz",     "value": f"**{poss_r}%**",                   "inline": True},
            {"name": "🎯 Empfehlung", "value": f"Beide Teams treffen (Comeback){qt}", "inline": False},
        ],
        "footer": {"text": f"Fixture • {heute()} {jetzt()}"},
    }

def discord_torflut_tipp(home, away, comp, country, score_hz1, tore_hz1, grenze, quote):
    qt = f"\n💶 **Quote:** {quote}" if quote else ""
    return {
        "title": "🌊 Torflut Signal", "color": FARBE_TORFLUT,
        "fields": [
            {"name": "🏆 Liga",        "value": f"{comp} ({country})", "inline": True},
            {"name": "⚽ Spiel",       "value": f"{home} vs {away}",   "inline": True},
            {"name": "📊 Halbzeitstand", "value": f"**{score_hz1}**",  "inline": True},
            {"name": "⚽ Tore HZ1",    "value": f"**{tore_hz1}** Tore", "inline": True},
            {"name": "🎯 Empfehlung", "value": f"Über **{grenze} Tore** im Gesamtspiel{qt}", "inline": False},
        ],
        "footer": {"text": f"Fixture • {heute()} {jetzt()}"},
    }

def discord_rotkarte_tipp(home, away, comp, country, score, minute,
                           rote_karte_team, ueberzahl_team, spieler, quote):
    qt = f"\n💶 **Quote:** {quote}" if quote else ""
    return {
        "title": "🟥 Rote Karte Signal", "color": FARBE_ROTKARTE,
        "fields": [
            {"name": "🏆 Liga",           "value": f"{comp} ({country})",               "inline": True},
            {"name": "⚽ Spiel",          "value": f"{home} vs {away}",                 "inline": True},
            {"name": "📊 Stand",          "value": f"**{score}** | Min. **{minute}'**", "inline": True},
            {"name": "🟥 Rote Karte für", "value": f"**{spieler}** ({rote_karte_team})", "inline": False},
            {"name": "💪 Überzahl",       "value": f"**{ueberzahl_team}**",              "inline": True},
            {"name": "🎯 Empfehlung", "value": f"Nächstes Tor für **{ueberzahl_team}**{qt}", "inline": False},
        ],
        "footer": {"text": f"Fixture • {heute()} {jetzt()}"},
    }

def discord_auswertung(typ, home, away, gewonnen, details: dict):
    farbe = FARBE_AUSWERTUNG_GEWONNEN if gewonnen else FARBE_AUSWERTUNG_VERLOREN
    emoji = "✅ GEWONNEN" if gewonnen else "❌ VERLOREN"
    titel = {
        "ecken":    "📐 Auswertung – Ecken Unter",
        "ecken_over": "📐 Auswertung – Ecken Über",
        "karten":   "🃏 Auswertung – Karten",
        "torwart":  "🧤 Auswertung – Torwart",
        "druck":    "🔥 Auswertung – Druck",
        "comeback": "🔄 Auswertung – Comeback",
        "torflut":  "🌊 Auswertung – Torflut",
        "rotkarte": "🟥 Auswertung – Rote Karte",
    }.get(typ, "📊 Auswertung")
    felder = [{"name": "⚽ Spiel", "value": f"{home} vs {away}", "inline": False}]
    for k, v in details.items():
        felder.append({"name": k, "value": v, "inline": True})
    felder.append({"name": "Ergebnis", "value": f"**{emoji}**", "inline": False})
    return {"title": titel, "color": farbe, "fields": felder,
            "footer": {"text": f"Auswertung • {heute()} {jetzt()}"}}

# ============================================================
#  STATISTIK & BERICHTE
# ============================================================

def update_statistik(typ, gewonnen, quote):
    if gewonnen:
        gewinn = round((quote - 1) * EINSATZ, 2) if quote else round(EINSATZ * 0.7, 2)
        statistik[typ]["gewonnen"]        += 1
        statistik[typ]["gewinn"]          += gewinn
        wochen_statistik[typ]["gewonnen"] += 1
        wochen_statistik[typ]["gewinn"]   += gewinn
    else:
        statistik[typ]["verloren"]        += 1
        statistik[typ]["gewinn"]          -= EINSATZ
        wochen_statistik[typ]["verloren"] += 1
        wochen_statistik[typ]["gewinn"]   -= EINSATZ
    statistik_speichern()

def statistik_zeile(name, stat):
    gesamt = stat["gewonnen"] + stat["verloren"]
    if gesamt == 0:
        return f"{name}: Noch keine Tipps"
    pct    = round(stat["gewonnen"] / gesamt * 100)
    gewinn = round(stat["gewinn"], 2)
    emoji  = "📈" if gewinn >= 0 else "📉"
    return f"{name}: {stat['gewonnen']}/{gesamt} ({pct}%) {emoji} {'+' if gewinn >= 0 else ''}{gewinn}€"

def send_tagesbericht():
    global tagesbericht_gesendet
    gw  = sum(statistik[t]["gewonnen"] for t in statistik)
    vl  = sum(statistik[t]["verloren"] for t in statistik)
    ges = gw + vl
    gn  = round(sum(statistik[t]["gewinn"] for t in statistik), 2)
    pct = round(gw / ges * 100) if ges else 0
    ei  = "📈" if gn >= 0 else "📉"
    msg = (f"📋 <b>Tagesbericht – {heute()}</b>\n"
           f"━━━━━━━━━━━━━━━━━━━━\n"
           f"✅ Gewonnen: <b>{gw}</b>\n❌ Verloren: <b>{vl}</b>\n"
           f"🎯 Trefferquote: <b>{pct}%</b>\n"
           f"{ei} Simulation ({EINSATZ}€/Tipp): <b>{'+' if gn>=0 else ''}{gn}€</b>\n"
           f"━━━━━━━━━━━━━━━━━━━━\n📊 <b>Nach Wetttyp:</b>\n"
           f"📐 {statistik_zeile('Ecken Unter',  statistik['ecken'])}\n"
           f"📐 {statistik_zeile('Ecken Über',   statistik['ecken_over'])}\n"
           f"🃏 {statistik_zeile('Karten',        statistik['karten'])}\n"
           f"🧤 {statistik_zeile('Torwart',       statistik['torwart'])}\n"
           f"🔥 {statistik_zeile('Druck',         statistik['druck'])}\n"
           f"🔄 {statistik_zeile('Comeback',      statistik['comeback'])}\n"
           f"🌊 {statistik_zeile('Torflut',       statistik['torflut'])}\n"
           f"🟥 {statistik_zeile('Rote Karte',    statistik['rotkarte'])}\n"
           f"━━━━━━━━━━━━━━━━━━━━\n🕐 {jetzt()} Uhr")
    send_telegram(msg)
    send_discord(DISCORD_WEBHOOK_BILANZ, msg)
    for t in statistik:
        statistik[t] = {"gewonnen": 0, "verloren": 0, "gewinn": 0.0}
    print(f"  [Bericht] Tagesbericht gesendet ({heute()})")

def send_wochenbericht():
    gw  = sum(wochen_statistik[t]["gewonnen"] for t in wochen_statistik)
    vl  = sum(wochen_statistik[t]["verloren"] for t in wochen_statistik)
    ges = gw + vl
    gn  = round(sum(wochen_statistik[t]["gewinn"] for t in wochen_statistik), 2)
    pct = round(gw / ges * 100) if ges else 0
    ei  = "📈" if gn >= 0 else "📉"
    msg = (f"📅 <b>Wochenbericht</b>\n"
           f"━━━━━━━━━━━━━━━━━━━━\n"
           f"✅ Gewonnen: <b>{gw}</b>\n❌ Verloren: <b>{vl}</b>\n"
           f"🎯 Trefferquote: <b>{pct}%</b>\n"
           f"{ei} Simulation ({EINSATZ}€/Tipp): <b>{'+' if gn>=0 else ''}{gn}€</b>\n"
           f"━━━━━━━━━━━━━━━━━━━━\n📊 <b>Nach Wetttyp:</b>\n"
           f"📐 {statistik_zeile('Ecken Unter',  wochen_statistik['ecken'])}\n"
           f"📐 {statistik_zeile('Ecken Über',   wochen_statistik['ecken_over'])}\n"
           f"🃏 {statistik_zeile('Karten',        wochen_statistik['karten'])}\n"
           f"🧤 {statistik_zeile('Torwart',       wochen_statistik['torwart'])}\n"
           f"🔥 {statistik_zeile('Druck',         wochen_statistik['druck'])}\n"
           f"🔄 {statistik_zeile('Comeback',      wochen_statistik['comeback'])}\n"
           f"🌊 {statistik_zeile('Torflut',       wochen_statistik['torflut'])}\n"
           f"🟥 {statistik_zeile('Rote Karte',    wochen_statistik['rotkarte'])}\n"
           f"━━━━━━━━━━━━━━━━━━━━\n🕐 {jetzt()} Uhr")
    send_telegram(msg)
    send_discord(DISCORD_WEBHOOK_BILANZ, msg)
    for t in wochen_statistik:
        wochen_statistik[t] = {"gewonnen": 0, "verloren": 0, "gewinn": 0.0}
    print(f"  [Bericht] Wochenbericht gesendet")

# ============================================================
#  AUSWERTUNG
# ============================================================

def auswertung_ecken(spiel):
    match_id  = spiel["match_id"]
    hz1_ecken = spiel["hz1_ecken"]
    grenze    = hz1_ecken * 2 + 3
    home, away, quote = spiel["home"], spiel["away"], spiel.get("quote")
    try:
        stats       = get_statistiken(match_id)
        total_ecken = stats["corners_home"] + stats["corners_away"]
        gewonnen    = total_ecken < grenze
        update_statistik("ecken", gewonnen, quote)
        emoji = "✅ GEWONNEN" if gewonnen else "❌ VERLOREN"
        ql = f"💶 Quote: <b>{quote}</b> → Gewinn: <b>+{round((quote-1)*EINSATZ,2)}€</b>\n" if quote and gewonnen else ""
        return (f"📊 <b>Auswertung – Ecken Unter</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                f"📌 {home} vs {away}\n📐 Ecken HZ1: <b>{hz1_ecken}</b>\n"
                f"🎯 Tipp: Unter <b>{grenze}</b> Ecken\n📈 Tatsächlich: <b>{total_ecken}</b>{ql}\n"
                f"━━━━━━━━━━━━━━━━━━━━\n{emoji}\n🕐 {jetzt()} Uhr")
    except Exception as e:
        print(f"  [Auswertung] Ecken Fehler: {e}")
        return None

def auswertung_ecken_over(spiel):
    match_id  = spiel["match_id"]
    hz1_ecken = spiel["hz1_ecken"]
    GRENZE    = 14
    home, away, quote = spiel["home"], spiel["away"], spiel.get("quote")
    try:
        stats       = get_statistiken(match_id)
        total_ecken = stats["corners_home"] + stats["corners_away"]
        gewonnen    = total_ecken > GRENZE
        update_statistik("ecken_over", gewonnen, quote)
        emoji = "✅ GEWONNEN" if gewonnen else "❌ VERLOREN"
        ql = f"💶 Quote: <b>{quote}</b> → Gewinn: <b>+{round((quote-1)*EINSATZ,2)}€</b>\n" if quote and gewonnen else ""
        return (f"📊 <b>Auswertung – Ecken Über</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                f"📌 {home} vs {away}\n📐 Ecken bei Signal: <b>{hz1_ecken}</b>\n"
                f"🎯 Tipp: Über <b>{GRENZE}</b> Ecken\n📈 Tatsächlich: <b>{total_ecken}</b>\n{ql}"
                f"━━━━━━━━━━━━━━━━━━━━\n{emoji}\n🕐 {jetzt()} Uhr")
    except Exception as e:
        print(f"  [Auswertung] Ecken-Über Fehler: {e}")
        return None

def auswertung_karten(spiel):
    match_id   = spiel["match_id"]
    karten_hz1 = spiel["karten_anzahl"]
    GRENZE     = 5
    home, away, quote = spiel["home"], spiel["away"], spiel.get("quote")
    try:
        events   = get_events(match_id)
        anzahl   = len([e for e in events if e.get("event") in KARTEN_TYPEN])
        gewonnen = anzahl > GRENZE
        update_statistik("karten", gewonnen, quote)
        emoji = "✅ GEWONNEN" if gewonnen else "❌ VERLOREN"
        ql = f"💶 Quote: <b>{quote}</b> → Gewinn: <b>+{round((quote-1)*EINSATZ,2)}€</b>\n" if quote and gewonnen else ""
        return (f"📊 <b>Auswertung – Karten</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                f"📌 {home} vs {away}\n🃏 Karten bei Signal: <b>{karten_hz1}</b>\n"
                f"🎯 Tipp: Über <b>{GRENZE}</b> Karten\n📈 Tatsächlich: <b>{anzahl}</b>\n{ql}"
                f"━━━━━━━━━━━━━━━━━━━━\n{emoji}\n🕐 {jetzt()} Uhr")
    except Exception as e:
        print(f"  [Auswertung] Karten Fehler: {e}")
        return None

def auswertung_torwart(spiel):
    match_id = spiel["match_id"]
    home, away, quote = spiel["home"], spiel["away"], spiel.get("quote")
    try:
        match    = ls_get_single_match(match_id)
        score    = match.get("scores", {}).get("score", "0 - 0")
        h, a     = parse_score(score)
        tore     = h + a
        gewonnen = tore >= 1
        update_statistik("torwart", gewonnen, quote)
        emoji = "✅ GEWONNEN" if gewonnen else "❌ VERLOREN"
        ql = f"💶 Quote: <b>{quote}</b> → Gewinn: <b>+{round((quote-1)*EINSATZ,2)}€</b>\n" if quote and gewonnen else ""
        return (f"📊 <b>Auswertung – Torwart</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                f"📌 {home} vs {away}\n🎯 Tipp: Mind. 1 Tor\n"
                f"📈 Endstand: <b>{score}</b> ({tore} Tore)\n{ql}"
                f"━━━━━━━━━━━━━━━━━━━━\n{emoji}\n🕐 {jetzt()} Uhr")
    except Exception as e:
        print(f"  [Auswertung] Torwart Fehler: {e}")
        return None

def auswertung_druck(spiel):
    match_id   = spiel["match_id"]
    druck_team = spiel["druck_team"]
    home, away, quote = spiel["home"], spiel["away"], spiel.get("quote")
    try:
        match = ls_get_single_match(match_id)
        score = match.get("scores", {}).get("score", "0 - 0")
        h, a  = parse_score(score)
        # Druck-Team hat gewonnen wenn es mehr Tore als Gegner hat
        if druck_team == home:
            gewonnen = h > a
        else:
            gewonnen = a > h
        update_statistik("druck", gewonnen, quote)
        emoji = "✅ GEWONNEN" if gewonnen else "❌ VERLOREN"
        ql = f"💶 Quote: <b>{quote}</b> → Gewinn: <b>+{round((quote-1)*EINSATZ,2)}€</b>\n" if quote and gewonnen else ""
        return (f"📊 <b>Auswertung – Druck</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                f"📌 {home} vs {away}\n🔥 Druck-Team: <b>{druck_team}</b>\n"
                f"📈 Endstand: <b>{score}</b>\n{ql}"
                f"━━━━━━━━━━━━━━━━━━━━\n{emoji}\n🕐 {jetzt()} Uhr")
    except Exception as e:
        print(f"  [Auswertung] Druck Fehler: {e}")
        return None

def auswertung_comeback(spiel):
    match_id     = spiel["match_id"]
    rueckliegend = spiel["rueckliegend"]
    home, away, quote = spiel["home"], spiel["away"], spiel.get("quote")
    try:
        match = ls_get_single_match(match_id)
        score = match.get("scores", {}).get("score", "0 - 0")
        h, a  = parse_score(score)
        # Gewonnen wenn beide Teams getroffen haben (mind. 1 Tor jedes Team)
        gewonnen = h >= 1 and a >= 1
        update_statistik("comeback", gewonnen, quote)
        emoji = "✅ GEWONNEN" if gewonnen else "❌ VERLOREN"
        ql = f"💶 Quote: <b>{quote}</b> → Gewinn: <b>+{round((quote-1)*EINSATZ,2)}€</b>\n" if quote and gewonnen else ""
        return (f"📊 <b>Auswertung – Comeback</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                f"📌 {home} vs {away}\n🔄 Rückliegend: <b>{rueckliegend}</b>\n"
                f"🎯 Tipp: Beide Teams treffen\n"
                f"📈 Endstand: <b>{score}</b>\n{ql}"
                f"━━━━━━━━━━━━━━━━━━━━\n{emoji}\n🕐 {jetzt()} Uhr")
    except Exception as e:
        print(f"  [Auswertung] Comeback Fehler: {e}")
        return None

def auswertung_torflut(spiel):
    match_id  = spiel["match_id"]
    grenze    = spiel["grenze"]
    hz1_tore  = spiel["hz1_tore"]
    home, away, quote = spiel["home"], spiel["away"], spiel.get("quote")
    try:
        match = ls_get_single_match(match_id)
        score = match.get("scores", {}).get("score", "0 - 0")
        h, a  = parse_score(score)
        tore  = h + a
        gewonnen = tore > grenze
        update_statistik("torflut", gewonnen, quote)
        emoji = "✅ GEWONNEN" if gewonnen else "❌ VERLOREN"
        ql = f"💶 Quote: <b>{quote}</b> → Gewinn: <b>+{round((quote-1)*EINSATZ,2)}€</b>\n" if quote and gewonnen else ""
        return (f"📊 <b>Auswertung – Torflut</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                f"📌 {home} vs {away}\n⚽ Tore HZ1: <b>{hz1_tore}</b>\n"
                f"🎯 Tipp: Über <b>{grenze}</b> Tore gesamt\n"
                f"📈 Endstand: <b>{score}</b> ({tore} Tore)\n{ql}"
                f"━━━━━━━━━━━━━━━━━━━━\n{emoji}\n🕐 {jetzt()} Uhr")
    except Exception as e:
        print(f"  [Auswertung] Torflut Fehler: {e}")
        return None

def auswertung_rotkarte(spiel):
    match_id      = spiel["match_id"]
    ueberzahl_team = spiel["ueberzahl_team"]
    score_signal  = spiel["score_signal"]
    home, away, quote = spiel["home"], spiel["away"], spiel.get("quote")
    try:
        match = ls_get_single_match(match_id)
        score = match.get("scores", {}).get("score", "0 - 0")
        h_end, a_end = parse_score(score)
        h_sig, a_sig = parse_score(score_signal)
        # Gewonnen wenn Überzahl-Team nach Signal noch getroffen hat
        if ueberzahl_team == home:
            gewonnen = h_end > h_sig
        else:
            gewonnen = a_end > a_sig
        update_statistik("rotkarte", gewonnen, quote)
        emoji = "✅ GEWONNEN" if gewonnen else "❌ VERLOREN"
        ql = f"💶 Quote: <b>{quote}</b> → Gewinn: <b>+{round((quote-1)*EINSATZ,2)}€</b>\n" if quote and gewonnen else ""
        return (f"📊 <b>Auswertung – Rote Karte</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                f"📌 {home} vs {away}\n💪 Überzahl-Team: <b>{ueberzahl_team}</b>\n"
                f"📊 Stand bei Signal: <b>{score_signal}</b>\n"
                f"📈 Endstand: <b>{score}</b>\n{ql}"
                f"━━━━━━━━━━━━━━━━━━━━\n{emoji}\n🕐 {jetzt()} Uhr")
    except Exception as e:
        print(f"  [Auswertung] Rote Karte Fehler: {e}")
        return None

# ============================================================
#  AUSWERTUNGS- & BERICHT-THREAD
# ============================================================

def bot_auswertung_und_berichte():
    global tagesbericht_gesendet
    print("[Auswertung-Bot] Gestartet")
    letzter_wochenbericht = de_now().isocalendar()[1]
    spiel_zuletzt_live    = {}

    while True:
        try:
            now = de_now()
            if now.hour == TAGESBERICHT_UHRZEIT and tagesbericht_gesendet != now.date():
                send_tagesbericht()
                tagesbericht_gesendet = now.date()
            aktuelle_woche = now.isocalendar()[1]
            if now.weekday() == 0 and aktuelle_woche != letzter_wochenbericht:
                send_wochenbericht()
                letzter_wochenbericht = aktuelle_woche

            if beobachtete_spiele:
                try:
                    alle        = get_live_matches()
                    live_ids    = {m.get("id") for m in alle}
                    live_status = {m.get("id"): m.get("status", "") for m in alle}
                except Exception as e:
                    print(f"  [Auswertung-Bot] API Fehler: {e}")
                    time.sleep(60)
                    continue

                aktuell = time.time()
                for match_id, spiel in list(beobachtete_spiele.items()):
                    if match_id in auswertung_done:
                        continue
                    mid_str = str(match_id)
                    if mid_str in live_ids:
                        spiel_zuletzt_live[mid_str] = aktuell
                        continue
                    zuletzt        = spiel_zuletzt_live.get(mid_str, 0)
                    minuten_weg    = (aktuell - zuletzt) / 60 if zuletzt > 0 else 999
                    status         = live_status.get(mid_str, "")
                    beendet_status = status in ("FT", "FINISHED", "AET", "Finished", "finished")
                    print(f"  [Auswertung] {spiel['home']} vs {spiel['away']} | "
                          f"Status: '{status}' | Weg: {minuten_weg:.1f} Min | Beendet: {beendet_status}")
                    if beendet_status or minuten_weg >= 3:
                        print(f"  [Auswertung] Beendet: {spiel['home']} vs {spiel['away']}")
                        time.sleep(20)
                        typ     = spiel["typ"]
                        webhook = spiel["webhook"]
                        auswertung_fn = {
                            "ecken":    auswertung_ecken,
                            "ecken_over": auswertung_ecken_over,
                            "karten":   auswertung_karten,
                            "torwart":  auswertung_torwart,
                            "druck":    auswertung_druck,
                            "comeback": auswertung_comeback,
                            "torflut":  auswertung_torflut,
                            "rotkarte": auswertung_rotkarte,
                        }.get(typ)
                        msg = auswertung_fn(spiel) if auswertung_fn else None
                        if msg:
                            send_telegram(msg)
                            gewonnen = "GEWONNEN" in msg
                            details  = {"📊 Typ": f"**{typ.upper()}**"}
                            embed    = discord_auswertung(typ, spiel["home"], spiel["away"], gewonnen, details)
                            send_discord_embed(webhook, embed)
                            print(f"  [Auswertung] Gesendet: {spiel['home']} vs {spiel['away']} ({typ})")
                        auswertung_done.add(match_id)
        except Exception as e:
            print(f"  [Auswertung-Bot] Fehler: {e}")
        time.sleep(60)

# ============================================================
#  FUSSBALL BOTS (Bestehend)
# ============================================================

def bot_ecken():
    print(f"[Ecken-Bot] Gestartet | max. {MAX_CORNERS} Ecken in HZ1")
    while True:
        try:
            matches  = get_live_matches()
            halftime = [m for m in matches if m.get("status") == "HALF TIME BREAK"]
            print(f"[{jetzt()}] [Ecken-Bot] {len(halftime)} Halbzeit-Spiele")
            for game in halftime:
                match_id = str(game.get("id"))
                if match_id in notified_ecken:
                    continue
                stats        = get_statistiken(match_id)
                corners_home = stats["corners_home"]
                corners_away = stats["corners_away"]
                corners      = corners_home + corners_away
                home    = game.get("home", {}).get("name", "?")
                away    = game.get("away", {}).get("name", "?")
                comp    = game.get("competition", {}).get("name", "?")
                country = (game.get("country") or {}).get("name", "International")
                score   = game.get("scores", {}).get("score", "?")
                grenze  = corners * 2 + 3
                if corners == 0:
                    continue
                if corners <= MAX_CORNERS:
                    quote = get_quote(home, away, "ecken")
                    ql    = f"\n💶 Quote: <b>{quote}</b>" if quote else ""
                    msg   = (f"📐 <b>Ecken Tipp!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                             f"🏆 {comp} ({country})\n📌 {home} vs {away}\n"
                             f"📊 Stand: <b>{score}</b>\n"
                             f"🔵 {home}: <b>{corners_home}</b> | 🔴 {away}: <b>{corners_away}</b>\n"
                             f"📊 Gesamt: <b>{corners}</b>\n"
                             f"🎯 Tipp: Unter <b>{grenze}</b> Ecken gesamt{ql}\n"
                             f"━━━━━━━━━━━━━━━━━━━━\n🕐 {jetzt()} Uhr")
                    send_telegram(msg)
                    send_discord_embed(DISCORD_WEBHOOK_ECKEN,
                        discord_ecken_tipp(home, away, comp, country, score,
                                           corners_home, corners_away, corners, grenze, quote))
                    notified_ecken.add(match_id)
                    beobachtete_spiele[match_id] = {
                        "typ": "ecken", "match_id": match_id,
                        "home": home, "away": away, "hz1_ecken": corners,
                        "quote": quote, "webhook": DISCORD_WEBHOOK_ECKEN
                    }
                    print(f"  [Ecken-Bot] OK: {home} vs {away} ({corners} Ecken)")
                time.sleep(0.5)
        except Exception as e:
            print(f"  [Ecken-Bot] Fehler: {e}")
        time.sleep(FUSSBALL_INTERVAL * 60)

def bot_ecken_over():
    print(f"[Ecken-Über-Bot] Gestartet | Signal sobald 7 Ecken in laufender HZ1")
    while True:
        try:
            matches = get_live_matches()
            hz1     = [m for m in matches if m.get("status") == "IN PLAY"
                       and 1 <= _safe_int(m.get("time", 0)) <= 45]
            print(f"[{jetzt()}] [Ecken-Über-Bot] {len(hz1)} laufende HZ1-Spiele")
            for game in hz1:
                match_id = str(game.get("id"))
                if match_id in notified_ecken_over:
                    continue
                stats        = get_statistiken(match_id)
                corners_home = stats["corners_home"]
                corners_away = stats["corners_away"]
                corners      = corners_home + corners_away
                if corners < 7:
                    continue
                home    = game.get("home", {}).get("name", "?")
                away    = game.get("away", {}).get("name", "?")
                comp    = game.get("competition", {}).get("name", "?")
                country = (game.get("country") or {}).get("name", "International")
                score   = game.get("scores", {}).get("score", "?")
                minute  = game.get("time", "?")
                quote   = get_quote(home, away, "ecken_over")
                ql      = f"\n💶 Quote: <b>{quote}</b>" if quote else ""
                msg     = (f"📐 <b>Ecken ÜBER Tipp!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                           f"🏆 {comp} ({country})\n📌 {home} vs {away}\n"
                           f"📊 Stand: <b>{score}</b> | Minute: <b>{minute}'</b>\n"
                           f"🔵 {home}: <b>{corners_home}</b> | 🔴 {away}: <b>{corners_away}</b>\n"
                           f"📊 Gesamt: <b>{corners}</b>\n"
                           f"🎯 Tipp: Über <b>14</b> Ecken gesamt{ql}\n"
                           f"━━━━━━━━━━━━━━━━━━━━\n🕐 {jetzt()} Uhr")
                send_telegram(msg)
                send_discord_embed(DISCORD_WEBHOOK_ECKEN,
                    discord_ecken_over_tipp(home, away, comp, country, score, minute,
                                            corners_home, corners_away, corners, quote))
                notified_ecken_over.add(match_id)
                beobachtete_spiele[match_id] = {
                    "typ": "ecken_over", "match_id": match_id,
                    "home": home, "away": away, "hz1_ecken": corners,
                    "quote": quote, "webhook": DISCORD_WEBHOOK_ECKEN
                }
                print(f"  [Ecken-Über-Bot] OK: {home} vs {away} ({corners} Ecken in Min. {minute})")
                time.sleep(0.5)
        except Exception as e:
            print(f"  [Ecken-Über-Bot] Fehler: {e}")
        time.sleep(FUSSBALL_INTERVAL * 60)

def bot_karten():
    print(f"[Karten-Bot] Gestartet | mind. {MIN_KARTEN} Karten bis Minute {KARTEN_BIS_MINUTE}")
    while True:
        try:
            matches = get_live_matches()
            laufend = [m for m in matches if m.get("status") in ("IN PLAY", "ADDED TIME")]
            print(f"[{jetzt()}] [Karten-Bot] {len(laufend)} laufende Spiele")
            for game in laufend:
                match_id = str(game.get("id"))
                if match_id in notified_karten:
                    continue
                minute = _safe_int(game.get("time", 0))
                if minute > KARTEN_BIS_MINUTE + 5:
                    continue
                events = get_events(match_id)
                karten = [e for e in events
                          if e.get("event") in KARTEN_TYPEN
                          and _safe_int(e.get("time") or 999) <= KARTEN_BIS_MINUTE]
                home    = game.get("home", {}).get("name", "?")
                away    = game.get("away", {}).get("name", "?")
                comp    = game.get("competition", {}).get("name", "?")
                country = (game.get("country") or {}).get("name", "International")
                score   = game.get("scores", {}).get("score", "?")
                if len(karten) >= MIN_KARTEN:
                    quote  = get_quote(home, away, "karten")
                    ql     = f"\n💶 Quote: <b>{quote}</b>" if quote else ""
                    zeilen = []
                    karten_discord = []
                    for k in karten:
                        spieler = (k.get("player") or {}).get("name", "?")
                        team    = k.get("home_away", "?")
                        min_k   = k.get("time", "?")
                        emj     = karten_emoji(k.get("event", "Yellow Card"))
                        zeilen.append(f"  {emj} {min_k}' {spieler} ({team})")
                        karten_discord.append(f"{emj} {min_k}' {spieler} ({team})")
                    msg = (f"🃏 <b>Karten-Alarm!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                           f"🏆 {comp} ({country})\n📌 {home} vs {away}\n"
                           f"📊 Stand: <b>{score}</b> | Minute: <b>{minute}'</b>\n"
                           f"━━━━━━━━━━━━━━━━━━━━\n<b>{len(karten)} Karten bis Min. {KARTEN_BIS_MINUTE}:</b>\n"
                           f"{chr(10).join(zeilen)}\n"
                           f"🎯 Tipp: Über <b>5</b> Karten gesamt{ql}\n"
                           f"━━━━━━━━━━━━━━━━━━━━\n🕐 {jetzt()} Uhr")
                    send_telegram(msg)
                    send_discord_embed(DISCORD_WEBHOOK_KARTEN,
                        discord_karten_tipp(home, away, comp, country, score, minute, karten_discord, quote))
                    notified_karten.add(match_id)
                    beobachtete_spiele[match_id] = {
                        "typ": "karten", "match_id": match_id,
                        "home": home, "away": away, "karten_anzahl": len(karten),
                        "quote": quote, "webhook": DISCORD_WEBHOOK_KARTEN
                    }
                    print(f"  [Karten-Bot] OK: {home} vs {away} ({len(karten)} Karten)")
                time.sleep(0.5)
        except Exception as e:
            print(f"  [Karten-Bot] Fehler: {e}")
        time.sleep(FUSSBALL_INTERVAL * 60)

def bot_torwart():
    print(f"[Torwart-Bot] Gestartet | 0:0 + mind. {MIN_SHOTS_ON_TARGET} Schüsse")
    while True:
        try:
            matches = get_live_matches()
            aktiv   = [m for m in matches if m.get("status") in ("IN PLAY", "ADDED TIME", "HALF TIME BREAK")]
            print(f"[{jetzt()}] [Torwart-Bot] {len(aktiv)} aktive Spiele")
            for game in aktiv:
                match_id = str(game.get("id"))
                if match_id in notified_torwart:
                    continue
                score = game.get("scores", {}).get("score", "")
                if "0 - 0" not in score and "0-0" not in score:
                    continue
                stats      = get_statistiken(match_id)
                shots_home = stats["shots_on_target_home"]
                shots_away = stats["shots_on_target_away"]
                shots_ges  = shots_home + shots_away
                if shots_ges < MIN_SHOTS_ON_TARGET:
                    continue
                saves_home = stats["saves_home"]
                saves_away = stats["saves_away"]
                poss_home  = stats["possession_home"]
                poss_away  = stats["possession_away"]
                home    = game.get("home", {}).get("name", "?")
                away    = game.get("away", {}).get("name", "?")
                comp    = game.get("competition", {}).get("name", "?")
                country = (game.get("country") or {}).get("name", "International")
                status  = game.get("status", "")
                minute  = game.get("time", "?")
                min_text = "Halbzeit" if status == "HALF TIME BREAK" else f"{minute}'"
                quote   = get_quote(home, away, "torwart")
                ql      = f"\n💶 Quote: <b>{quote}</b>" if quote else ""
                msg     = (f"🧤 <b>Torwart-Alarm!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                           f"🏆 {comp} ({country})\n📌 {home} vs {away}\n"
                           f"📊 Stand: <b>0:0</b> | {min_text}\n━━━━━━━━━━━━━━━━━━━━\n"
                           f"🎯 Schüsse: <b>{shots_ges}</b> ({shots_home}|{shots_away})\n"
                           f"🧤 Paraden: <b>{saves_home+saves_away}</b> ({saves_home}|{saves_away})\n"
                           f"⚽ Ballbesitz: {poss_home}%|{poss_away}%\n"
                           f"🎯 Tipp: Mind. <b>1 Tor</b> fällt noch{ql}\n"
                           f"━━━━━━━━━━━━━━━━━━━━\n🕐 {jetzt()} Uhr")
                send_telegram(msg)
                send_discord_embed(DISCORD_WEBHOOK_TORWART,
                    discord_torwart_tipp(home, away, comp, country,
                                         shots_home, shots_away, saves_home, saves_away,
                                         poss_home, poss_away, min_text, quote))
                notified_torwart.add(match_id)
                beobachtete_spiele[match_id] = {
                    "typ": "torwart", "match_id": match_id,
                    "home": home, "away": away,
                    "quote": quote, "webhook": DISCORD_WEBHOOK_TORWART
                }
                print(f"  [Torwart-Bot] OK: {home} vs {away} | {shots_ges} Schüsse")
                time.sleep(0.5)
        except Exception as e:
            print(f"  [Torwart-Bot] Fehler: {e}")
        time.sleep(FUSSBALL_INTERVAL * 60)

# ============================================================
#  NEUE BOTS
# ============================================================

def _safe_int(val, default=0):
    try:
        return int(val)
    except (ValueError, TypeError):
        return default

def bot_druck():
    """Signal wenn ein Team deutlich mehr Ecken+Freistöße hat als der Gegner."""
    print(f"[Druck-Bot] Gestartet | Ratio {DRUCK_RATIO}:1 bei mind. {MIN_DRUCK_ECKEN} Ecken")
    while True:
        try:
            matches = get_live_matches()
            laufend = [m for m in matches if m.get("status") in ("IN PLAY", "ADDED TIME")
                       and 20 <= _safe_int(m.get("time", 0)) <= 85]
            print(f"[{jetzt()}] [Druck-Bot] {len(laufend)} Spiele geprüft")
            for game in laufend:
                match_id = str(game.get("id"))
                if match_id in notified_druck:
                    continue
                stats  = get_statistiken(match_id)
                c_home = stats["corners_home"]
                c_away = stats["corners_away"]
                f_home = stats["free_kicks_home"]
                f_away = stats["free_kicks_away"]
                gesamt_ecken = c_home + c_away
                if gesamt_ecken < MIN_DRUCK_ECKEN:
                    continue
                # Dominantes Team bestimmen
                druck_team = None
                if c_away > 0 and c_home / c_away >= DRUCK_RATIO:
                    druck_team   = game.get("home", {}).get("name", "?")
                    ecken_stark  = c_home
                    ecken_schwach = c_away
                    fk_stark     = f_home
                    fk_schwach   = f_away
                elif c_home > 0 and c_away / c_home >= DRUCK_RATIO:
                    druck_team   = game.get("away", {}).get("name", "?")
                    ecken_stark  = c_away
                    ecken_schwach = c_home
                    fk_stark     = f_away
                    fk_schwach   = f_home
                if not druck_team:
                    continue
                home    = game.get("home", {}).get("name", "?")
                away    = game.get("away", {}).get("name", "?")
                comp    = game.get("competition", {}).get("name", "?")
                country = (game.get("country") or {}).get("name", "International")
                score   = game.get("scores", {}).get("score", "?")
                minute  = game.get("time", "?")
                quote   = get_quote(home, away, "druck")
                ql      = f"\n💶 Quote: <b>{quote}</b>" if quote else ""
                msg     = (f"🔥 <b>Druck Signal!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                           f"🏆 {comp} ({country})\n📌 {home} vs {away}\n"
                           f"📊 Stand: <b>{score}</b> | Minute: <b>{minute}'</b>\n"
                           f"🔥 Dominantes Team: <b>{druck_team}</b>\n"
                           f"📐 Ecken: <b>{ecken_stark}</b> : {ecken_schwach}\n"
                           f"🦵 Freistöße: <b>{fk_stark}</b> : {fk_schwach}\n"
                           f"🎯 Tipp: Nächste Ecke / Tor für <b>{druck_team}</b>{ql}\n"
                           f"━━━━━━━━━━━━━━━━━━━━\n🕐 {jetzt()} Uhr")
                send_telegram(msg)
                send_discord_embed(DISCORD_WEBHOOK_DRUCK,
                    discord_druck_tipp(home, away, comp, country, score, minute, druck_team,
                                       ecken_stark, ecken_schwach, fk_stark, fk_schwach, quote))
                notified_druck.add(match_id)
                beobachtete_spiele[match_id] = {
                    "typ": "druck", "match_id": match_id,
                    "home": home, "away": away, "druck_team": druck_team,
                    "quote": quote, "webhook": DISCORD_WEBHOOK_DRUCK
                }
                print(f"  [Druck-Bot] OK: {home} vs {away} | {druck_team} dominiert ({ecken_stark}:{ecken_schwach} Ecken)")
                time.sleep(0.5)
        except Exception as e:
            print(f"  [Druck-Bot] Fehler: {e}")
        time.sleep(FUSSBALL_INTERVAL * 60)

def bot_comeback():
    """Signal wenn rückliegendes Team mehr Schüsse & Ballbesitz hat als das führende."""
    print(f"[Comeback-Bot] Gestartet | ab Minute {COMEBACK_AB_MINUTE}")
    while True:
        try:
            matches = get_live_matches()
            laufend = [m for m in matches if m.get("status") in ("IN PLAY", "ADDED TIME")
                       and _safe_int(m.get("time", 0)) >= COMEBACK_AB_MINUTE]
            print(f"[{jetzt()}] [Comeback-Bot] {len(laufend)} Spiele geprüft")
            for game in laufend:
                match_id = str(game.get("id"))
                if match_id in notified_comeback:
                    continue
                score_str = game.get("scores", {}).get("score", "")
                h_tore, a_tore = parse_score(score_str)
                # Nur Spiele mit genau 1 Tor Unterschied
                if abs(h_tore - a_tore) != 1:
                    continue
                home    = game.get("home", {}).get("name", "?")
                away    = game.get("away", {}).get("name", "?")
                # Wer liegt zurück?
                rueckliegend = away if h_tore > a_tore else home
                fuehrend     = home if h_tore > a_tore else away
                stats     = get_statistiken(match_id)
                shots_h   = stats["shots_on_target_home"]
                shots_a   = stats["shots_on_target_away"]
                poss_h    = _safe_int(stats["possession_home"])
                poss_a    = _safe_int(stats["possession_away"])
                # Rückliegendes Team muss mehr Schüsse UND Ballbesitz haben
                if rueckliegend == home:
                    shots_r, shots_f = shots_h, shots_a
                    poss_r           = poss_h
                else:
                    shots_r, shots_f = shots_a, shots_h
                    poss_r           = poss_a
                if shots_r <= shots_f or poss_r <= 50:
                    continue
                comp    = game.get("competition", {}).get("name", "?")
                country = (game.get("country") or {}).get("name", "International")
                minute  = game.get("time", "?")
                quote   = get_quote(home, away, "comeback")
                ql      = f"\n💶 Quote: <b>{quote}</b>" if quote else ""
                msg     = (f"🔄 <b>Comeback Signal!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                           f"🏆 {comp} ({country})\n📌 {home} vs {away}\n"
                           f"📊 Stand: <b>{score_str}</b> | Minute: <b>{minute}'</b>\n"
                           f"📉 Rückliegend: <b>{rueckliegend}</b>\n"
                           f"🎯 Schüsse aufs Tor: <b>{shots_r}</b> (Gegner: {shots_f})\n"
                           f"⚽ Ballbesitz: <b>{poss_r}%</b>\n"
                           f"🎯 Tipp: Beide Teams treffen{ql}\n"
                           f"━━━━━━━━━━━━━━━━━━━━\n🕐 {jetzt()} Uhr")
                send_telegram(msg)
                send_discord_embed(DISCORD_WEBHOOK_COMEBACK,
                    discord_comeback_tipp(home, away, comp, country, score_str, minute,
                                          rueckliegend, fuehrend, shots_r, shots_f, poss_r, quote))
                notified_comeback.add(match_id)
                beobachtete_spiele[match_id] = {
                    "typ": "comeback", "match_id": match_id,
                    "home": home, "away": away, "rueckliegend": rueckliegend,
                    "quote": quote, "webhook": DISCORD_WEBHOOK_COMEBACK
                }
                print(f"  [Comeback-Bot] OK: {home} vs {away} | {rueckliegend} liegt zurück aber dominiert")
                time.sleep(0.5)
        except Exception as e:
            print(f"  [Comeback-Bot] Fehler: {e}")
        time.sleep(FUSSBALL_INTERVAL * 60)

def bot_torflut():
    """Signal wenn HZ1 bereits 3+ Tore hatte."""
    print(f"[Torflut-Bot] Gestartet | mind. {TORFLUT_MIN_TORE} Tore in HZ1")
    while True:
        try:
            matches  = get_live_matches()
            halftime = [m for m in matches if m.get("status") == "HALF TIME BREAK"]
            print(f"[{jetzt()}] [Torflut-Bot] {len(halftime)} Halbzeit-Spiele")
            for game in halftime:
                match_id = str(game.get("id"))
                if match_id in notified_torflut:
                    continue
                score_str = game.get("scores", {}).get("score", "0 - 0")
                h, a      = parse_score(score_str)
                tore_hz1  = h + a
                if tore_hz1 < TORFLUT_MIN_TORE:
                    continue
                home    = game.get("home", {}).get("name", "?")
                away    = game.get("away", {}).get("name", "?")
                comp    = game.get("competition", {}).get("name", "?")
                country = (game.get("country") or {}).get("name", "International")
                grenze  = tore_hz1 + 1  # z.B. 3 Tore HZ1 → Tipp Über 4 gesamt
                quote   = get_quote(home, away, "torflut")
                ql      = f"\n💶 Quote: <b>{quote}</b>" if quote else ""
                msg     = (f"🌊 <b>Torflut Signal!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                           f"🏆 {comp} ({country})\n📌 {home} vs {away}\n"
                           f"📊 Halbzeitstand: <b>{score_str}</b>\n"
                           f"⚽ Tore HZ1: <b>{tore_hz1}</b>\n"
                           f"🎯 Tipp: Über <b>{grenze}</b> Tore im Gesamtspiel{ql}\n"
                           f"━━━━━━━━━━━━━━━━━━━━\n🕐 {jetzt()} Uhr")
                send_telegram(msg)
                send_discord_embed(DISCORD_WEBHOOK_TORFLUT,
                    discord_torflut_tipp(home, away, comp, country, score_str, tore_hz1, grenze, quote))
                notified_torflut.add(match_id)
                beobachtete_spiele[match_id] = {
                    "typ": "torflut", "match_id": match_id,
                    "home": home, "away": away, "hz1_tore": tore_hz1,
                    "grenze": grenze, "quote": quote, "webhook": DISCORD_WEBHOOK_TORFLUT
                }
                print(f"  [Torflut-Bot] OK: {home} vs {away} | {tore_hz1} Tore in HZ1")
                time.sleep(0.5)
        except Exception as e:
            print(f"  [Torflut-Bot] Fehler: {e}")
        time.sleep(FUSSBALL_INTERVAL * 60)

def bot_rotkarte():
    """Signal nach einer Roten Karte – Tipp auf Tor des Überzahl-Teams."""
    print(f"[Rotkarte-Bot] Gestartet | Signal nach Roter Karte")
    while True:
        try:
            matches = get_live_matches()
            laufend = [m for m in matches if m.get("status") in ("IN PLAY", "ADDED TIME")]
            print(f"[{jetzt()}] [Rotkarte-Bot] {len(laufend)} laufende Spiele")
            for game in laufend:
                match_id = str(game.get("id"))
                if match_id in notified_rotkarte:
                    continue
                minute = _safe_int(game.get("time", 0))
                if minute > 80:  # Zu spät für sinnvolles Signal
                    continue
                events = get_events(match_id)
                # Rote Karten finden
                rote_karten = [e for e in events if e.get("event") in ROTKARTE_TYPEN]
                if not rote_karten:
                    continue
                # Letzte Rote Karte nehmen
                letzte_karte = rote_karten[-1]
                karte_min    = _safe_int(letzte_karte.get("time") or 0)
                # Nur wenn Karte in den letzten 10 Minuten kam
                if minute - karte_min > 10:
                    continue
                home    = game.get("home", {}).get("name", "?")
                away    = game.get("away", {}).get("name", "?")
                comp    = game.get("competition", {}).get("name", "?")
                country = (game.get("country") or {}).get("name", "International")
                score   = game.get("scores", {}).get("score", "?")
                spieler = (letzte_karte.get("player") or {}).get("name", "?")
                # home_away: "home" = Heimteam hat Karte bekommen
                karte_fuer    = letzte_karte.get("home_away", "")
                rote_karte_team = home if karte_fuer == "home" else away
                ueberzahl_team  = away if karte_fuer == "home" else home
                quote = get_quote(home, away, "rotkarte")
                ql    = f"\n💶 Quote: <b>{quote}</b>" if quote else ""
                msg   = (f"🟥 <b>Rote Karte Signal!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                         f"🏆 {comp} ({country})\n📌 {home} vs {away}\n"
                         f"📊 Stand: <b>{score}</b> | Minute: <b>{minute}'</b>\n"
                         f"🟥 Rote Karte: <b>{spieler}</b> ({rote_karte_team}) in Min. {karte_min}'\n"
                         f"💪 Überzahl: <b>{ueberzahl_team}</b>\n"
                         f"🎯 Tipp: Nächstes Tor für <b>{ueberzahl_team}</b>{ql}\n"
                         f"━━━━━━━━━━━━━━━━━━━━\n🕐 {jetzt()} Uhr")
                send_telegram(msg)
                send_discord_embed(DISCORD_WEBHOOK_ROTKARTE,
                    discord_rotkarte_tipp(home, away, comp, country, score, minute,
                                          rote_karte_team, ueberzahl_team, spieler, quote))
                notified_rotkarte.add(match_id)
                beobachtete_spiele[match_id] = {
                    "typ": "rotkarte", "match_id": match_id,
                    "home": home, "away": away,
                    "ueberzahl_team": ueberzahl_team,
                    "score_signal": score,
                    "quote": quote, "webhook": DISCORD_WEBHOOK_ROTKARTE
                }
                print(f"  [Rotkarte-Bot] OK: {home} vs {away} | {ueberzahl_team} in Überzahl (Min. {karte_min})")
                time.sleep(0.5)
        except Exception as e:
            print(f"  [Rotkarte-Bot] Fehler: {e}")
        time.sleep(FUSSBALL_INTERVAL * 60)

# ============================================================
#  START
# ============================================================

if __name__ == "__main__":
    print("=" * 50)
    print("  ⚽ FUSSBALL BOTS v20")
    print("  8 Bots: Ecken · Ecken+ · Karten · Torwart")
    print("          Druck · Comeback · Torflut · Rotkarte")
    print("  Powered by livescore-api")
    print("=" * 50 + "\n")

    statistik_laden()

    threads = [
        threading.Thread(target=bot_ecken,                   daemon=True, name="Ecken-Bot"),
        threading.Thread(target=bot_ecken_over,              daemon=True, name="Ecken-Über-Bot"),
        threading.Thread(target=bot_karten,                  daemon=True, name="Karten-Bot"),
        threading.Thread(target=bot_torwart,                 daemon=True, name="Torwart-Bot"),
        threading.Thread(target=bot_druck,                   daemon=True, name="Druck-Bot"),
        threading.Thread(target=bot_comeback,                daemon=True, name="Comeback-Bot"),
        threading.Thread(target=bot_torflut,                 daemon=True, name="Torflut-Bot"),
        threading.Thread(target=bot_rotkarte,                daemon=True, name="Rotkarte-Bot"),
        threading.Thread(target=bot_auswertung_und_berichte, daemon=True, name="Auswertung-Bot"),
    ]

    for t in threads:
        t.start()
        time.sleep(2)

    print("Alle Bots laufen!\n")
    while True:
        time.sleep(60)
