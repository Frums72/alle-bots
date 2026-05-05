# v19 - Auswertungs-Fix: ID-Normalisierung, Cache-TTL 45s, besseres Logging
import requests
import re
import time
import threading
from datetime import datetime, timezone, timedelta

# ============================================================
#  KONFIGURATION – hier deine Daten eintragen
# ============================================================
API_KEY            = "INUnk7eRsptCrMNq"
API_SECRET         = "h2wf08YErEQbSAfAn9XIgbzJB3l3P9u6"

TELEGRAM_BOT_TOKEN = "8706066107:AAFAQhT3k0jhTZ7ep-VWHPlskOKJVvsfucQ"
TELEGRAM_CHAT_ID   = "7272001004"

DISCORD_WEBHOOK_ECKEN    = "https://discord.com/api/webhooks/1501122762096377957/OqjCXNqBBnMvaQlSz5npaYYnjbWpdh3DENhPE7aJr1ZA_WgGo0PkRRG6ZFZURi9X1CK4"
DISCORD_WEBHOOK_KARTEN   = "https://discord.com/api/webhooks/1501250542788280451/BZ6r8Y2SEDPgya9skt8Gyzbsoetvq0yPY6pWG5HrUzK9moeL-RXYWAiEwWuIlEy7GBfM"
DISCORD_WEBHOOK_TORWART  = "https://discord.com/api/webhooks/1501251703025041531/QDS0RBUuG8PNRNaDFB02dAHP1miwhixrAfxUw8HhDswt6ce-hIHUootC4GhmjKP9A6b1"
DISCORD_WEBHOOK_BILANZ   = "https://discord.com/api/webhooks/1501251926564667564/fdBE4jOLislDfwpMs2cUURm_4_YzfATKWFmaOjRNEXulHCJu1DB-lUBLqLmm73l-HQ4v"

ODDS_API_KEY       = "866948de5d6c34ca51faf6bd77e0bb2a"
EINSATZ            = 10.0

MAX_CORNERS         = 5
MIN_KARTEN          = 2
KARTEN_BIS_MINUTE   = 40
MIN_SHOTS_ON_TARGET = 3
FUSSBALL_INTERVAL   = 3
TAGESBERICHT_UHRZEIT = 22
# ============================================================

LS_BASE = "https://livescore-api.com/api-client"
LS_AUTH = {"key": API_KEY, "secret": API_SECRET}

# ============================================================
# FIX #1: Korrekte Event-Typen der LiveScore-API
# Die API liefert "Yellow Card", "Red Card", "Yellow Red Card"
# (mit Leerzeichen und Groß/Kleinschreibung, NICHT Unterstriche!)
# ============================================================
KARTEN_TYPEN = {"Yellow Card", "Red Card", "Yellow Red Card"}

# Shared Cache
_cache_matches   = []
_cache_timestamp = 0
_cache_lock      = threading.Lock()
CACHE_TTL        = 45  # FIX: Kürzerer Cache damit beendete Spiele schneller erkannt werden

notified_ecken       = set()
notified_ecken_over  = set()
notified_karten      = set()
notified_torwart     = set()
beobachtete_spiele   = {}
auswertung_done      = set()

statistik = {
    "ecken":       {"gewonnen": 0, "verloren": 0, "gewinn": 0.0},
    "ecken_over":  {"gewonnen": 0, "verloren": 0, "gewinn": 0.0},
    "karten":      {"gewonnen": 0, "verloren": 0, "gewinn": 0.0},
    "torwart":     {"gewonnen": 0, "verloren": 0, "gewinn": 0.0},
}
wochen_statistik = {
    "ecken":       {"gewonnen": 0, "verloren": 0, "gewinn": 0.0},
    "ecken_over":  {"gewonnen": 0, "verloren": 0, "gewinn": 0.0},
    "karten":      {"gewonnen": 0, "verloren": 0, "gewinn": 0.0},
    "torwart":     {"gewonnen": 0, "verloren": 0, "gewinn": 0.0},
}
tagesbericht_gesendet = None

# ============================================================
#  HILFSFUNKTIONEN
# ============================================================

def jetzt():
    return (datetime.now(timezone.utc) + timedelta(hours=2)).strftime("%H:%M")

def heute():
    return (datetime.now(timezone.utc) + timedelta(hours=2)).strftime("%d.%m.%Y")

def de_now():
    return datetime.now(timezone.utc) + timedelta(hours=2)

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

# ============================================================
# FIX #2: Korrekte Karten-Emoji-Funktion
# Verwendet jetzt die richtigen LiveScore Event-Typen
# ============================================================
def karten_emoji(typ: str) -> str:
    t = typ.lower().replace(" ", "").replace("_", "")
    if "yellowred" in t: return "🟨🟥"
    if "red"       in t: return "🟥"
    if "yellow"    in t: return "🟨"
    return "🃏"

# ============================================================
# FIX #3: Retry-Logik bei API-Fehlern (503, Timeouts etc.)
# ============================================================
def api_get_with_retry(url: str, params: dict, max_retries: int = 3) -> requests.Response:
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            return resp
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response else 0
            if status in (503, 502, 504) and attempt < max_retries - 1:
                wait = 2 ** attempt  # 1s, 2s, 4s
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
#  API-FUNKTIONEN (mit Retry)
# ============================================================

def ls_get_live_matches():
    resp    = api_get_with_retry(f"{LS_BASE}/matches/live.json", LS_AUTH)
    matches = resp.json().get("data", {}).get("match", []) or []
    # FIX: IDs immer als String normalisieren – API liefert manchmal int, manchmal str
    for m in matches:
        if "id" in m:
            m["id"] = str(m["id"])
    return matches

def ls_get_statistiken(match_id):
    params = {**LS_AUTH, "match_id": match_id}
    resp   = api_get_with_retry(f"{LS_BASE}/statistics/matches.json", params)
    stats  = resp.json().get("data", [])
    result = {"corners_home": 0, "corners_away": 0,
              "shots_on_target_home": 0, "shots_on_target_away": 0,
              "saves_home": 0, "saves_away": 0,
              "possession_home": "?", "possession_away": "?"}

    # DEBUG: Alle Stat-Typen loggen um falsche Feldnamen zu erkennen
    alle_typen = [s.get("type", "") for s in stats]
    print(f"  [Stats-Debug] Match {match_id} → Typen: {alle_typen}")

    for s in stats:
        val_h = int(s.get("home") or 0)
        val_a = int(s.get("away") or 0)
        # Feldname normalisieren: Kleinschreibung, Leerzeichen→Unterstrich
        typ_raw  = s.get("type", "")
        typ      = typ_raw.lower().replace(" ", "_")

        if typ == "corners":
            result["corners_home"] = val_h
            result["corners_away"] = val_a
        elif typ in ("shots_on_target", "on_target", "shots_on_goal"):
            result["shots_on_target_home"] = val_h
            result["shots_on_target_away"] = val_a
            print(f"  [Stats-Debug] Schüsse aufs Tor gefunden als '{typ_raw}': {val_h}|{val_a}")
        elif typ == "saves":
            result["saves_home"] = val_h
            result["saves_away"] = val_a
        elif typ in ("possession", "possesion", "ball_possession"):
            result["possession_home"] = str(val_h)
            result["possession_away"] = str(val_a)

    return result

def ls_get_events(match_id):
    params = {**LS_AUTH, "id": match_id}
    resp   = api_get_with_retry(f"{LS_BASE}/matches/events.json", params)
    return resp.json().get("data", {}).get("event", []) or []

def get_live_matches():
    global _cache_matches, _cache_timestamp
    with _cache_lock:
        now = time.time()
        if now - _cache_timestamp > CACHE_TTL:
            _cache_matches   = ls_get_live_matches()
            _cache_timestamp = now
            print(f"  [Cache] {len(_cache_matches)} Spiele geladen")
        return list(_cache_matches)

def get_statistiken(match_id):
    return ls_get_statistiken(match_id)

def get_events(match_id):
    return ls_get_events(match_id)

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

FARBE_ECKEN              = 0xF4A300
FARBE_ECKEN_OVER         = 0x9B59B6
FARBE_KARTEN             = 0xE74C3C
FARBE_TORWART            = 0x1ABC9C
FARBE_AUSWERTUNG_GEWONNEN= 0x2ECC71
FARBE_AUSWERTUNG_VERLOREN= 0xE74C3C
FARBE_BERICHT            = 0x3498DB

def discord_ecken_tipp(home, away, comp, country, score, corners_home, corners_away, corners, grenze, quote):
    quote_text = f"\n💶 **Quote:** {quote}" if quote else ""
    return {
        "title": "📐 Ecken Tipp",
        "color": FARBE_ECKEN,
        "fields": [
            {"name": "🏆 Liga",             "value": f"{comp} ({country})", "inline": True},
            {"name": "⚽ Spiel",            "value": f"{home} vs {away}",   "inline": True},
            {"name": "📊 Halbzeitstand",    "value": f"**{score}**",        "inline": True},
            {"name": "📐 Ecken zur Halbzeit",
             "value": f"🔵 {home}: **{corners_home}**\n🔴 {away}: **{corners_away}**\n📊 Gesamt: **{corners}**",
             "inline": False},
            {"name": "🎯 Empfehlung",
             "value": f"Unter **{grenze} Ecken** (Gesamtspiel){quote_text}", "inline": False},
        ],
        "footer": {"text": f"Fixture • {heute()} {jetzt()}"},
    }

def discord_ecken_over_tipp(home, away, comp, country, score, minute, corners_home, corners_away, corners, quote):
    quote_text = f"\n💶 **Quote:** {quote}" if quote else ""
    return {
        "title": "📐 Ecken ÜBER Tipp",
        "color": FARBE_ECKEN_OVER,
        "fields": [
            {"name": "🏆 Liga",          "value": f"{comp} ({country})",              "inline": True},
            {"name": "⚽ Spiel",         "value": f"{home} vs {away}",                "inline": True},
            {"name": "📊 Stand",         "value": f"**{score}** | Min. **{minute}'**","inline": True},
            {"name": "📐 Ecken bisher",
             "value": f"🔵 {home}: **{corners_home}**\n🔴 {away}: **{corners_away}**\n📊 Gesamt: **{corners}**",
             "inline": False},
            {"name": "🎯 Empfehlung",
             "value": f"Über **14 Ecken** (Gesamtspiel){quote_text}", "inline": False},
        ],
        "footer": {"text": f"Fixture • {heute()} {jetzt()}"},
    }

def discord_karten_tipp(home, away, comp, country, score, minute, karten_liste, quote):
    quote_text  = f"\n💶 **Quote:** {quote}" if quote else ""
    karten_text = "\n".join(karten_liste) if karten_liste else "–"
    return {
        "title": "🃏 Karten Signal",
        "color": FARBE_KARTEN,
        "fields": [
            {"name": "🏆 Liga",  "value": f"{comp} ({country})",              "inline": True},
            {"name": "⚽ Spiel", "value": f"{home} vs {away}",                "inline": True},
            {"name": "📊 Stand", "value": f"**{score}** | Min. **{minute}'**","inline": True},
            {"name": "🃏 Karten bis Minute 30", "value": karten_text,         "inline": False},
            {"name": "🎯 Empfehlung",
             "value": f"Über **5 Karten** (Gesamtspiel){quote_text}", "inline": False},
        ],
        "footer": {"text": f"Fixture • {heute()} {jetzt()}"},
    }

def discord_torwart_tipp(home, away, comp, country, shots_home, shots_away,
                          saves_h, saves_a, poss_h, poss_a, min_text, quote):
    quote_text = f"\n💶 **Quote:** {quote}" if quote else ""
    return {
        "title": "🧤 Torwart Alarm",
        "color": FARBE_TORWART,
        "fields": [
            {"name": "🏆 Liga",              "value": f"{comp} ({country})",                        "inline": True},
            {"name": "⚽ Spiel",             "value": f"{home} vs {away}",                          "inline": True},
            {"name": "📊 Stand",             "value": f"**0:0** | {min_text}",                      "inline": True},
            {"name": "🎯 Schüsse aufs Tor",  "value": f"Gesamt: **{shots_home+shots_away}** ({shots_home}|{shots_away})", "inline": True},
            {"name": "🧤 Paraden",           "value": f"Gesamt: **{saves_h+saves_a}** ({saves_h}|{saves_a})",            "inline": True},
            {"name": "⚽ Ballbesitz",        "value": f"{poss_h}% | {poss_a}%",                     "inline": True},
            {"name": "🎯 Empfehlung",
             "value": f"Mindestens **1 Tor** fällt noch{quote_text}", "inline": False},
        ],
        "footer": {"text": f"Fixture • {heute()} {jetzt()}"},
    }

def discord_auswertung(typ, home, away, gewonnen, details: dict):
    farbe = FARBE_AUSWERTUNG_GEWONNEN if gewonnen else FARBE_AUSWERTUNG_VERLOREN
    emoji = "✅ GEWONNEN" if gewonnen else "❌ VERLOREN"
    titel = {"ecken": "📐 Auswertung – Ecken Unter",
             "ecken_over": "📐 Auswertung – Ecken Über",
             "karten": "🃏 Auswertung – Karten",
             "torwart": "🧤 Auswertung – Torwart"}.get(typ, "📊 Auswertung")
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
           f"✅ Gewonnen: <b>{gw}</b>\n"
           f"❌ Verloren: <b>{vl}</b>\n"
           f"🎯 Trefferquote: <b>{pct}%</b>\n"
           f"{ei} Simulation ({EINSATZ}€/Tipp): <b>{'+' if gn >= 0 else ''}{gn}€</b>\n"
           f"━━━━━━━━━━━━━━━━━━━━\n"
           f"📊 <b>Nach Wetttyp:</b>\n"
           f"⚽ {statistik_zeile('Ecken Unter', statistik['ecken'])}\n"
           f"📐 {statistik_zeile('Ecken Über', statistik['ecken_over'])}\n"
           f"🃏 {statistik_zeile('Karten', statistik['karten'])}\n"
           f"🧤 {statistik_zeile('Torwart', statistik['torwart'])}\n"
           f"━━━━━━━━━━━━━━━━━━━━\n"
           f"🕐 {jetzt()} Uhr")
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
           f"✅ Gewonnen: <b>{gw}</b>\n"
           f"❌ Verloren: <b>{vl}</b>\n"
           f"🎯 Trefferquote: <b>{pct}%</b>\n"
           f"{ei} Simulation ({EINSATZ}€/Tipp): <b>{'+' if gn >= 0 else ''}{gn}€</b>\n"
           f"━━━━━━━━━━━━━━━━━━━━\n"
           f"📊 <b>Nach Wetttyp:</b>\n"
           f"⚽ {statistik_zeile('Ecken Unter', wochen_statistik['ecken'])}\n"
           f"📐 {statistik_zeile('Ecken Über', wochen_statistik['ecken_over'])}\n"
           f"🃏 {statistik_zeile('Karten', wochen_statistik['karten'])}\n"
           f"🧤 {statistik_zeile('Torwart', wochen_statistik['torwart'])}\n"
           f"━━━━━━━━━━━━━━━━━━━━\n"
           f"🕐 {jetzt()} Uhr")
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
    grenze    = hz1_ecken * 2 + 2
    home      = spiel["home"]
    away      = spiel["away"]
    quote     = spiel.get("quote")
    try:
        stats       = get_statistiken(match_id)
        total_ecken = stats["corners_home"] + stats["corners_away"]
        gewonnen    = total_ecken < grenze
        emoji       = "✅ GEWONNEN" if gewonnen else "❌ VERLOREN"
        update_statistik("ecken", gewonnen, quote)
        ql = f"💶 Quote: <b>{quote}</b> → Gewinn: <b>+{round((quote-1)*EINSATZ,2)}€</b>\n" if quote and gewonnen else ""
        msg = (f"📊 <b>Auswertung – Ecken Unter</b>\n━━━━━━━━━━━━━━━━━━━━\n"
               f"📌 {home} vs {away}\n"
               f"📐 Ecken HZ1: <b>{hz1_ecken}</b>\n"
               f"🎯 Tipp: Unter <b>{grenze}</b> Ecken gesamt\n"
               f"📈 Tatsächlich: <b>{total_ecken}</b> Ecken\n{ql}"
               f"━━━━━━━━━━━━━━━━━━━━\n{emoji}\n🕐 {jetzt()} Uhr")
        return msg
    except Exception as e:
        print(f"  [Auswertung] Ecken Fehler: {e}")
        return None

def auswertung_ecken_over(spiel):
    match_id  = spiel["match_id"]
    hz1_ecken = spiel["hz1_ecken"]
    home      = spiel["home"]
    away      = spiel["away"]
    quote     = spiel.get("quote")
    GRENZE    = 14
    try:
        stats       = get_statistiken(match_id)
        total_ecken = stats["corners_home"] + stats["corners_away"]
        gewonnen    = total_ecken > GRENZE
        emoji       = "✅ GEWONNEN" if gewonnen else "❌ VERLOREN"
        update_statistik("ecken_over", gewonnen, quote)
        ql = f"💶 Quote: <b>{quote}</b> → Gewinn: <b>+{round((quote-1)*EINSATZ,2)}€</b>\n" if quote and gewonnen else ""
        msg = (f"📊 <b>Auswertung – Ecken Über</b>\n━━━━━━━━━━━━━━━━━━━━\n"
               f"📌 {home} vs {away}\n"
               f"📐 Ecken bei Signal: <b>{hz1_ecken}</b>\n"
               f"🎯 Tipp: Über <b>{GRENZE}</b> Ecken gesamt\n"
               f"📈 Tatsächlich: <b>{total_ecken}</b> Ecken\n{ql}"
               f"━━━━━━━━━━━━━━━━━━━━\n{emoji}\n🕐 {jetzt()} Uhr")
        return msg
    except Exception as e:
        print(f"  [Auswertung] Ecken-Über Fehler: {e}")
        return None

def auswertung_karten(spiel):
    match_id   = spiel["match_id"]
    home       = spiel["home"]
    away       = spiel["away"]
    karten_hz1 = spiel["karten_anzahl"]
    quote      = spiel.get("quote")
    GRENZE     = 5
    try:
        events = get_events(match_id)
        # FIX: Korrekte Event-Typen der LiveScore-API verwenden
        anzahl = len([e for e in events if e.get("event") in KARTEN_TYPEN])
        gewonnen = anzahl > GRENZE
        emoji    = "✅ GEWONNEN" if gewonnen else "❌ VERLOREN"
        update_statistik("karten", gewonnen, quote)
        ql = f"💶 Quote: <b>{quote}</b> → Gewinn: <b>+{round((quote-1)*EINSATZ,2)}€</b>\n" if quote and gewonnen else ""
        msg = (f"📊 <b>Auswertung – Karten Tipp</b>\n━━━━━━━━━━━━━━━━━━━━\n"
               f"📌 {home} vs {away}\n"
               f"🃏 Karten nach 30 Min.: <b>{karten_hz1}</b>\n"
               f"🎯 Tipp: Über <b>{GRENZE}</b> Karten gesamt\n"
               f"📈 Tatsächlich: <b>{anzahl}</b> Karten\n{ql}"
               f"━━━━━━━━━━━━━━━━━━━━\n{emoji}\n🕐 {jetzt()} Uhr")
        return msg
    except Exception as e:
        print(f"  [Auswertung] Karten Fehler: {e}")
        return None

def auswertung_torwart(spiel):
    match_id = spiel["match_id"]
    home     = spiel["home"]
    away     = spiel["away"]
    quote    = spiel.get("quote")
    try:
        params = {**LS_AUTH, "id": match_id}
        resp   = api_get_with_retry(f"{LS_BASE}/matches/single.json", params)
        match  = resp.json().get("data", {}).get("match", {})
        score  = match.get("scores", {}).get("score", "0 - 0")
        parts  = score.replace(" ", "").split("-")
        tore   = int(parts[0]) + int(parts[1]) if len(parts) == 2 else 0
        gewonnen = tore >= 1
        emoji    = "✅ GEWONNEN" if gewonnen else "❌ VERLOREN"
        update_statistik("torwart", gewonnen, quote)
        ql = f"💶 Quote: <b>{quote}</b> → Gewinn: <b>+{round((quote-1)*EINSATZ,2)}€</b>\n" if quote and gewonnen else ""
        msg = (f"📊 <b>Auswertung – Torwart Tipp</b>\n━━━━━━━━━━━━━━━━━━━━\n"
               f"📌 {home} vs {away}\n"
               f"🎯 Tipp: Mindestens 1 Tor fällt noch\n"
               f"📈 Endstand: <b>{score}</b> ({tore} Tore)\n{ql}"
               f"━━━━━━━━━━━━━━━━━━━━\n{emoji}\n🕐 {jetzt()} Uhr")
        return msg
    except Exception as e:
        print(f"  [Auswertung] Torwart Fehler: {e}")
        return None

# ============================================================
#  AUSWERTUNGS & BERICHT THREAD
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
                    # FIX: ID immer als str vergleichen
                    mid_str = str(match_id)
                    if mid_str in live_ids:
                        spiel_zuletzt_live[mid_str] = aktuell
                        continue
                    zuletzt        = spiel_zuletzt_live.get(mid_str, 0)
                    minuten_weg    = (aktuell - zuletzt) / 60 if zuletzt > 0 else 999
                    status         = live_status.get(mid_str, "")
                    beendet_status = status in ("FT", "FINISHED", "AET", "Finished", "finished")
                    # FIX: Detailliertes Logging
                    print(f"  [Auswertung] {spiel['home']} vs {spiel['away']} | "
                          f"Status: '{status}' | Weg: {minuten_weg:.1f} Min | "
                          f"Beendet: {beendet_status}")
                    if beendet_status or minuten_weg >= 3:
                        print(f"  [Auswertung] Beendet: {spiel['home']} vs {spiel['away']}")
                        time.sleep(20)
                        typ     = spiel["typ"]
                        webhook = spiel["webhook"]
                        msg     = None
                        if typ == "ecken":
                            msg = auswertung_ecken(spiel)
                        elif typ == "ecken_over":
                            msg = auswertung_ecken_over(spiel)
                        elif typ == "karten":
                            msg = auswertung_karten(spiel)
                        elif typ == "torwart":
                            msg = auswertung_torwart(spiel)
                        if msg:
                            send_telegram(msg)
                            gewonnen = "GEWONNEN" in msg
                            if typ in ("ecken", "ecken_over"):
                                hz1 = spiel["hz1_ecken"]
                                grenze = hz1 * 2 + 2 if typ == "ecken" else 14
                                total = re.search(r"Tatsächlich.*?(\d+)", msg)
                                total_val = total.group(1) if total else "?"
                                details = {
                                    "📐 Ecken HZ1": f"**{hz1}**",
                                    "🎯 Tipp": f"{'Unter' if typ == 'ecken' else 'Über'} **{grenze}** Ecken",
                                    "📈 Tatsächlich": f"**{total_val}** Ecken"
                                }
                            elif typ == "karten":
                                details = {
                                    "🃏 Karten bis 30'": f"**{spiel['karten_anzahl']}**",
                                    "🎯 Tipp": "Über **5** Karten gesamt"
                                }
                            else:
                                score_match = re.search(r"Endstand.*?(\d+ - \d+)", msg)
                                details = {
                                    "🎯 Tipp": "Mindestens **1 Tor** fällt noch",
                                    "📈 Endstand": f"**{score_match.group(1) if score_match else '?'}**"
                                }
                            embed = discord_auswertung(typ, spiel["home"], spiel["away"], gewonnen, details)
                            send_discord_embed(webhook, embed)
                            auswertung_done.add(match_id)
                            print(f"  [Auswertung] Gesendet: {spiel['home']} vs {spiel['away']} ({typ})")
                        else:
                            auswertung_done.add(match_id)

        except Exception as e:
            print(f"  [Auswertung-Bot] Fehler: {e}")
        time.sleep(60)

# ============================================================
#  FUSSBALL BOTS
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
                home         = game.get("home", {}).get("name", "?")
                away         = game.get("away", {}).get("name", "?")
                comp         = game.get("competition", {}).get("name", "?")
                country      = (game.get("country") or {}).get("name", "International")
                score        = game.get("scores", {}).get("score", "?")
                grenze       = corners * 2 + 2

                if corners == 0:
                    continue
                if corners <= MAX_CORNERS:
                    quote = get_quote(home, away, "ecken")
                    ql    = f"\n💶 Quote: <b>{quote}</b>" if quote else ""
                    msg   = (f"📐 <b>Ecken Tipp!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                             f"🏆 {comp} ({country})\n📌 {home} vs {away}\n"
                             f"📊 Stand: <b>{score}</b>\n"
                             f"🔵 {home}: <b>{corners_home}</b>\n"
                             f"🔴 {away}: <b>{corners_away}</b>\n"
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
                        "home": home, "away": away,
                        "hz1_ecken": corners, "quote": quote,
                        "webhook": DISCORD_WEBHOOK_ECKEN
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
            hz1 = []
            for m in matches:
                status = m.get("status", "")
                try:
                    minute = int(m.get("time", 0))
                except:
                    continue
                if status == "IN PLAY" and 1 <= minute <= 45:
                    hz1.append(m)
            print(f"[{jetzt()}] [Ecken-Über-Bot] {len(hz1)} laufende HZ1-Spiele")
            for game in hz1:
                match_id     = str(game.get("id"))
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
                           f"🔵 {home}: <b>{corners_home}</b>\n"
                           f"🔴 {away}: <b>{corners_away}</b>\n"
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
                    "home": home, "away": away,
                    "hz1_ecken": corners, "quote": quote,
                    "webhook": DISCORD_WEBHOOK_ECKEN
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
                try:
                    minute = int(game.get("time", 0))
                except:
                    continue
                if minute > KARTEN_BIS_MINUTE + 5:
                    continue
                events  = get_events(match_id)
                # FIX: Korrekte Event-Typen + robuste Minutenprüfung (String → int)
                karten = []
                for e in events:
                    if e.get("event") not in KARTEN_TYPEN:
                        continue
                    try:
                        e_min = int(e.get("time") or 999)
                    except (ValueError, TypeError):
                        e_min = 999
                    if e_min <= KARTEN_BIS_MINUTE:
                        karten.append(e)

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
                        spieler  = (k.get("player") or {}).get("name", "?")
                        team     = k.get("home_away", "?")
                        min_k    = k.get("time", "?")
                        detail   = k.get("event", "Yellow Card")
                        emoji    = karten_emoji(detail)
                        zeilen.append(f"  {emoji} {min_k}' {spieler} ({team})")
                        karten_discord.append(f"{emoji} {min_k}' {spieler} ({team})")
                    karten_text = "\n".join(zeilen)
                    msg = (f"🃏 <b>Karten-Alarm!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                           f"🏆 {comp} ({country})\n📌 {home} vs {away}\n"
                           f"📊 Stand: <b>{score}</b> | Minute: <b>{minute}'</b>\n"
                           f"━━━━━━━━━━━━━━━━━━━━\n<b>{len(karten)} Karten bis Min. {KARTEN_BIS_MINUTE}:</b>\n"
                           f"{karten_text}\n"
                           f"🎯 Tipp: Über <b>5</b> Karten gesamt{ql}\n"
                           f"━━━━━━━━━━━━━━━━━━━━\n🕐 {jetzt()} Uhr")
                    send_telegram(msg)
                    send_discord_embed(DISCORD_WEBHOOK_KARTEN,
                        discord_karten_tipp(home, away, comp, country, score, minute,
                                            karten_discord, quote))
                    notified_karten.add(match_id)
                    beobachtete_spiele[match_id] = {
                        "typ": "karten", "match_id": match_id,
                        "home": home, "away": away,
                        "karten_anzahl": len(karten), "quote": quote,
                        "webhook": DISCORD_WEBHOOK_KARTEN
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
                saves_home = stats["saves_home"]
                saves_away = stats["saves_away"]
                poss_home  = stats["possession_home"]
                poss_away  = stats["possession_away"]
                home       = game.get("home", {}).get("name", "?")
                away       = game.get("away", {}).get("name", "?")
                comp       = game.get("competition", {}).get("name", "?")
                country    = (game.get("country") or {}).get("name", "International")
                minute     = game.get("time", "?")
                status     = game.get("status", "")
                min_text   = "Halbzeit" if status == "HALF TIME BREAK" else f"{minute}'"
                if shots_ges >= MIN_SHOTS_ON_TARGET:
                    quote = get_quote(home, away, "torwart")
                    ql    = f"\n💶 Quote: <b>{quote}</b>" if quote else ""
                    msg   = (f"🧤 <b>Torwart-Alarm!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                             f"🏆 {comp} ({country})\n📌 {home} vs {away}\n"
                             f"📊 Stand: <b>0:0</b> | {min_text}\n━━━━━━━━━━━━━━━━━━━━\n"
                             f"🎯 Schüsse: <b>{shots_ges}</b> ({shots_home}|{shots_away})\n"
                             f"🧤 Paraden: <b>{saves_home+saves_away}</b> ({saves_home}|{saves_away})\n"
                             f"⚽ Ballbesitz: {poss_home}%|{poss_away}%\n"
                             f"🎯 Tipp: Mindestens <b>1 Tor</b> fällt noch{ql}\n"
                             f"━━━━━━━━━━━━━━━━━━━━\n🕐 {jetzt()} Uhr")
                    send_telegram(msg)
                    send_discord_embed(DISCORD_WEBHOOK_TORWART,
                        discord_torwart_tipp(home, away, comp, country,
                                             shots_home, shots_away,
                                             saves_home, saves_away,
                                             poss_home, poss_away, min_text, quote))
                    notified_torwart.add(match_id)
                    beobachtete_spiele[match_id] = {
                        "typ": "torwart", "match_id": match_id,
                        "home": home, "away": away, "quote": quote,
                        "webhook": DISCORD_WEBHOOK_TORWART
                    }
                    print(f"  [Torwart-Bot] OK: {home} vs {away} | {shots_ges} Schüsse")
                time.sleep(0.5)
        except Exception as e:
            print(f"  [Torwart-Bot] Fehler: {e}")
        time.sleep(FUSSBALL_INTERVAL * 60)

# ============================================================
#  START
# ============================================================

if __name__ == "__main__":
    print("=" * 50)
    print("  ⚽ FUSSBALL BOTS v19")
    print("  Telegram + Discord (3 Webhooks)")
    print("  Ecken Unter + Ecken Über + Karten + Torwart")
    print("  Powered by livescore-api")
    print("=" * 50 + "\n")

    threads = [
        threading.Thread(target=bot_ecken,                   daemon=True, name="Ecken-Bot"),
        threading.Thread(target=bot_ecken_over,              daemon=True, name="Ecken-Über-Bot"),
        threading.Thread(target=bot_karten,                  daemon=True, name="Karten-Bot"),
        threading.Thread(target=bot_torwart,                 daemon=True, name="Torwart-Bot"),
        threading.Thread(target=bot_auswertung_und_berichte, daemon=True, name="Auswertung-Bot"),
    ]

    for t in threads:
        t.start()
        time.sleep(2)

    print("Alle Bots laufen!\n")
    while True:
        time.sleep(60)
