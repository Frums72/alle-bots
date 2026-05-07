# v27 - Telegram Befehle, Persistenz, Bankroll, Multi-Signal, API-Monitor, Comeback+
import requests
import re
import time
import threading
from datetime import datetime, timezone, timedelta

# ============================================================
#  KONFIGURATION
# ============================================================
API_KEY            = "OHvYYqv2LTNBi8qU"
API_SECRET         = "G8lerfJK8OJ8TqMH7iG6Jb8u4V6n3wiK"

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
DISCORD_WEBHOOK_HZ1TORE  = "https://discord.com/api/webhooks/1501252266630316163/aBo4o0HDN_Fh3eVj-WEvRZlzo970OQJcO1g6vKk4gJJ6hfRxco98m0p5KXDEQ-NBEZr1"
DISCORD_WEBHOOK_VZTORE   = "https://discord.com/api/webhooks/1501252266630316163/aBo4o0HDN_Fh3eVj-WEvRZlzo970OQJcO1g6vKk4gJJ6hfRxco98m0p5KXDEQ-NBEZr1"

ODDS_API_KEY       = "866948de5d6c34ca51faf6bd77e0bb2a"
ANTHROPIC_API_KEY  = "ANTHROPIC_API_KEY_HIER_EINTRAGEN"  # claude.ai → API Keys
EINSATZ            = 10.0

# Pre-Match Bot
PREMATCH_UHRZEITEN   = [10, 16, 20]  # Uhrzeiten für automatische Tipps
PREMATCH_MAX_TIPPS   = 3             # Max. Spiele pro Post
PREMATCH_LIGEN       = {
    "premier league", "bundesliga", "la liga", "serie a", "ligue 1",
    "champions league", "europa league", "eredivisie", "primeira liga",
    "uefa champions league", "uefa europa league", "uefa conference league",
    "primera division", "süper lig", "scottish premiership",
}
TELEGRAM_CHAT_PREMATCH = "-1001510152037"  # Telegram Gruppe

# Bestehende Parameter
MAX_CORNERS          = 5
MIN_KARTEN           = 2
KARTEN_BIS_MINUTE    = 40
MIN_SHOTS_ON_TARGET  = 3
FUSSBALL_INTERVAL    = 3
TAGESBERICHT_UHRZEIT = 0

# Wett-Optimierung
MIN_QUOTE            = 1.3   # Kein Signal wenn Quote unter diesem Wert
KELLY_FRACTION       = 0.25  # Vorsichtiges Kelly (25% des vollen Kelly)
KELLY_MAX_EINSATZ    = 50.0  # Maximaler Einsatz pro Tipp in €
KELLY_MIN_EINSATZ    = 2.0   # Minimaler Einsatz pro Tipp in €

# Wetter-Filter
WETTER_WIND_GRENZE   = 35    # km/h – ab hier gilt es als windig
WETTER_REGEN_GRENZE  = 2     # mm  – ab hier gilt es als regnerisch

# Liga-Filter
LIGA_MIN_TIPPS       = 10    # Mindest-Tipps bevor Liga gefiltert wird
LIGA_MIN_TREFFERQUOTE = 0.40 # Ligen unter dieser Quote werden ignoriert

# Ecken-Durchschnitt
ECKEN_HISTORY_SPIELE = 5     # Anzahl historischer Spiele für Durchschnitt
ECKEN_TOLERANZ       = 1.5   # Tipp wird gesendet wenn Grenze > Durchschnitt + Toleranz

# H2H Tore Bots
H2H_MIN_SPIELE       = 3     # Mindest-H2H-Spiele für Tipp
H2H_MAX_SPIELE       = 10    # Wie viele H2H-Spiele abgerufen werden
H2H_SIGNAL_BIS_MIN   = 15    # Nur Signale in den ersten X Minuten eines Spiels
# HZ1-Tore Schwellenwerte (Über/Unter X.5 Tore)
HZ1_UEBER_GRENZE     = 1.2   # avg HZ1-Tore > dieser Wert → Tipp Über 1.5
HZ1_UNTER_GRENZE     = 0.7   # avg HZ1-Tore < dieser Wert → Tipp Unter 0.5
# Vollzeit-Tore Schwellenwerte
VZ_UEBER_GRENZE      = 2.7   # avg VZ-Tore > dieser Wert → Tipp Über 2.5
VZ_UNTER_GRENZE      = 1.8   # avg VZ-Tore < dieser Wert → Tipp Unter 1.5

# Quoten-Validierung & Closing Line Value
MIN_BOOKMAKER_ANZAHL = 0     # 0 = deaktiviert (viele kleine Ligen haben keine Odds)
CLV_WARNSCHWELLE     = 0.05  # Wenn Schlusskurs >5% tiefer als Einstieg → CLV negativ

# Neue Parameter
MIN_DRUCK_ECKEN      = 6    # Mindest-Ecken gesamt für Druck-Signal
DRUCK_RATIO          = 2.5  # Dominantes Team muss X-mal mehr Ecken haben
COMEBACK_AB_MINUTE   = 30   # Ab welcher Minute Comeback-Signal prüfen
TORFLUT_MIN_TORE     = 3    # Mindest-Tore in HZ1 für Torflut-Signal

# ── Bankroll-Tracking ────────────────────────────────────────
BANKROLL             = 100.0  # Deine aktuelle Bankroll in €
BANKROLL_DATEI       = "bankroll.json"

# ── Multi-Signal Boost ───────────────────────────────────────
MULTI_SIGNAL_BONUS   = 2     # Konfidenz-Bonus wenn mehrere Bots dasselbe Spiel tippen

# ── API-Monitor ──────────────────────────────────────────────
API_MONITOR_DATEI    = "api_monitor.json"

# ── Persistenz ───────────────────────────────────────────────
BEOBACHTETE_DATEI    = "beobachtete_spiele.json"

# ── Telegram Bot Polling ─────────────────────────────────────
BOT_PAUSIERT         = False   # Wird durch /pause Befehl gesetzt
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
notified_hz1tore    = set()
notified_vztore     = set()
beobachtete_spiele  = {}
auswertung_done     = set()

# ── Doppel-Tipp Schutz ──────────────────────────────────────
bereits_getippt  = {}   # match_id → bot_name (verhindert doppelte Tipps)

# ── Fehler-Zähler für Telegram-Alerts ───────────────────────
fehler_zaehler   = {}   # bot_name → aufeinanderfolgende Fehler
FEHLER_ALERT_AB  = 3    # nach X Fehlern Telegram-Alert senden

# ── API Rate-Limit ───────────────────────────────────────────
_api_calls_log   = []
_api_calls_lock  = threading.Lock()
MAX_API_PER_MIN  = 50   # max. API-Calls pro Minute (Professional Plan: 50k/Tag)

# Gegentipp-Schutz
aktive_tipps     = {}   # match_id → list of {markt, richtung, bot}

# Signal-Log für Optimizer
signal_log       = []   # Liste aller Signale mit Ergebnis
SIGNAL_LOG_DATEI = "signal_log.json"

# Wetter-Cache
_wetter_cache    = {}   # country → {"wind": x, "regen": x, "ts": timestamp}
WETTER_TTL       = 1800 # 30 Minuten

# Ecken-Durchschnitt Cache
_ecken_avg_cache = {}   # team_id → {"avg": x, "ts": timestamp}
ECKEN_AVG_TTL    = 3600 # 1 Stunde

# Standings Cache
_standings_cache = {}   # league_id → {"data": [...], "ts": timestamp}
STANDINGS_TTL    = 1800 # 30 Minuten

# ── Streak-Tracker ──────────────────────────────────────────
streak_aktuell   = 0   # positiv = Gewinnserie, negativ = Verlustserie
streak_beste     = 0   # beste Gewinnserie gesamt

statistik = {
    "ecken":    {"gewonnen": 0, "verloren": 0, "gewinn": 0.0},
    "ecken_over": {"gewonnen": 0, "verloren": 0, "gewinn": 0.0},
    "karten":   {"gewonnen": 0, "verloren": 0, "gewinn": 0.0},
    "torwart":  {"gewonnen": 0, "verloren": 0, "gewinn": 0.0},
    "druck":    {"gewonnen": 0, "verloren": 0, "gewinn": 0.0},
    "comeback": {"gewonnen": 0, "verloren": 0, "gewinn": 0.0},
    "torflut":  {"gewonnen": 0, "verloren": 0, "gewinn": 0.0},
    "rotkarte": {"gewonnen": 0, "verloren": 0, "gewinn": 0.0},
    "hz1tore":  {"gewonnen": 0, "verloren": 0, "gewinn": 0.0},
    "vztore":   {"gewonnen": 0, "verloren": 0, "gewinn": 0.0},
}
wochen_statistik = {k: {"gewonnen": 0, "verloren": 0, "gewinn": 0.0} for k in statistik}

# ── Erweiterte Statistik ─────────────────────────────────────
stunden_statistik = {str(h): {"gewonnen": 0, "verloren": 0} for h in range(24)}
liga_statistik    = {}  # liga_name → {"gewonnen": 0, "verloren": 0}
tagesbericht_gesendet = None
STATISTIK_DATEI = "statistik.json"

# ============================================================
#  STATISTIK SPEICHERN / LADEN
# ============================================================

def statistik_laden():
    global statistik, wochen_statistik, tagesbericht_gesendet, stunden_statistik, liga_statistik, signal_log
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
        if data.get("stunden_statistik"):
            stunden_statistik.update(data["stunden_statistik"])
        if data.get("liga_statistik"):
            liga_statistik.update(data["liga_statistik"])
        if data.get("tagesbericht_gesendet"):
            from datetime import date
            tagesbericht_gesendet = date.fromisoformat(data["tagesbericht_gesendet"])
        print(f"  [Statistik] Geladen aus {STATISTIK_DATEI}")
    except Exception as e:
        print(f"  [Statistik] Ladefehler: {e}")
    # Signal-Log laden
    if os.path.exists(SIGNAL_LOG_DATEI):
        try:
            with open(SIGNAL_LOG_DATEI, "r") as f:
                signal_log = json.load(f)
            print(f"  [Signal-Log] {len(signal_log)} Einträge geladen")
        except Exception as e:
            print(f"  [Signal-Log] Ladefehler: {e}")

def statistik_speichern():
    import json
    try:
        data = {
            "statistik": statistik,
            "wochen_statistik": wochen_statistik,
            "stunden_statistik": stunden_statistik,
            "liga_statistik": liga_statistik,
            "tagesbericht_gesendet": str(tagesbericht_gesendet) if tagesbericht_gesendet else None,
        }
        with open(STATISTIK_DATEI, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        print(f"  [Statistik] Speicherfehler: {e}")

def signal_log_speichern():
    import json
    try:
        with open(SIGNAL_LOG_DATEI, "w") as f:
            json.dump(signal_log[-500:], f, indent=2)  # max. 500 Einträge
    except Exception as e:
        print(f"  [Signal-Log] Speicherfehler: {e}")
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
    if BOT_PAUSIERT:
        print("  [Telegram] Signal unterdrückt (Bot pausiert)")
        return
    url     = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    resp    = requests.post(url, json=payload, timeout=10)
    if resp.status_code != 200:
        print(f"  [Telegram Fehler] {resp.text}")

def send_telegram_gruppe(message: str, chat_id: str = None):
    """Sendet eine Nachricht an eine spezifische Chat-ID (für Gruppen)."""
    ziel = chat_id or TELEGRAM_CHAT_PREMATCH
    url     = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": ziel, "text": message, "parse_mode": "HTML"}
    resp    = requests.post(url, json=payload, timeout=10)
    if resp.status_code != 200:
        print(f"  [Telegram Gruppe Fehler] {resp.text}")

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

# ── Rate-Limit Schutz ────────────────────────────────────────
def rate_limit_check():
    """Wartet falls zu viele API-Calls in der letzten Minute gemacht wurden."""
    with _api_calls_lock:
        now = time.time()
        # Einträge älter als 60s entfernen
        while _api_calls_log and _api_calls_log[0] < now - 60:
            _api_calls_log.pop(0)
        if len(_api_calls_log) >= MAX_API_PER_MIN:
            wait = 60 - (now - _api_calls_log[0]) + 1
            print(f"  [Rate-Limit] {len(_api_calls_log)} Calls/Min – warte {wait:.0f}s")
            time.sleep(max(wait, 1))
        _api_calls_log.append(time.time())
        api_monitor_increment()

# ── Fehler-Alerts ────────────────────────────────────────────
def bot_fehler_melden(bot_name: str, fehler: Exception):
    """Zählt aufeinanderfolgende Fehler und sendet Telegram-Alert nach X Fehlern."""
    fehler_zaehler[bot_name] = fehler_zaehler.get(bot_name, 0) + 1
    count = fehler_zaehler[bot_name]
    print(f"  [{bot_name}] Fehler #{count}: {fehler}")
    if count == FEHLER_ALERT_AB:
        msg = (f"🚨 <b>Bot-Fehler Alert!</b>\n"
               f"Bot: <b>{bot_name}</b>\n"
               f"Fehler: {fehler}\n"
               f"Aufeinanderfolgende Fehler: <b>{count}</b>\n"
               f"🕐 {jetzt()} Uhr")
        send_telegram(msg)
        print(f"  [{bot_name}] Fehler-Alert gesendet!")

def bot_fehler_reset(bot_name: str):
    """Setzt Fehler-Zähler zurück nach erfolgreichem Durchlauf."""
    if fehler_zaehler.get(bot_name, 0) > 0:
        fehler_zaehler[bot_name] = 0

# ── Doppel-Tipp Schutz ───────────────────────────────────────
def tipp_erlaubt(match_id: str, bot_name: str) -> bool:
    """Gibt False zurück wenn dieses Spiel bereits von einem anderen Bot getippt wurde."""
    if match_id in bereits_getippt:
        erster = bereits_getippt[match_id]
        if erster != bot_name:
            print(f"  [{bot_name}] Doppel-Tipp verhindert für {match_id} (bereits von {erster})")
            return False
    bereits_getippt[match_id] = bot_name
    return True

def karten_emoji(typ: str) -> str:
    t = typ.lower().replace(" ", "").replace("_", "")
    if "yellowred" in t: return "🟨🟥"
    if "red"       in t: return "🟥"
    if "yellow"    in t: return "🟨"
    return "🃏"

def api_get_with_retry(url: str, params: dict, max_retries: int = 3) -> requests.Response:
    rate_limit_check()
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

def _safe_int(val, default=0):
    """Sicher int() – gibt default zurück wenn Konvertierung fehlschlägt."""
    try:
        return int(val)
    except (ValueError, TypeError):
        return default

def ls_get_live_matches():
    alle_matches = []
    seite = 1
    while True:
        params = {**LS_AUTH, "page": seite}
        resp   = api_get_with_retry(f"{LS_BASE}/matches/live.json", params)
        data   = resp.json().get("data", {})
        matches = data.get("match", []) or []
        if not matches:
            break
        for m in matches:
            # fixture_id als id verwenden falls kein id vorhanden
            if "id" not in m and "fixture_id" in m:
                m["id"] = str(m["fixture_id"])
            elif "id" in m:
                m["id"] = str(m["id"])
        alle_matches.extend(matches)
        # Prüfen ob es mehr Seiten gibt
        total     = data.get("total", len(matches))
        per_page  = data.get("per_page", 50)
        if len(alle_matches) >= total or len(matches) < per_page:
            break
        seite += 1
        if seite > 10:  # Sicherheit: max 10 Seiten
            break
    print(f"  [API] {len(alle_matches)} Live-Spiele geladen (Seiten: {seite})")
    return alle_matches

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
    """Holt die BESTE Quote aus allen verfügbaren Bookmakers."""
    if not ODDS_API_KEY:
        return None
    try:
        url    = "https://api.the-odds-api.com/v4/sports/soccer/odds/"
        params = {"apiKey": ODDS_API_KEY, "regions": "eu",
                  "markets": "totals", "oddsFormat": "decimal"}
        resp   = requests.get(url, params=params, timeout=8)
        if resp.status_code != 200:
            return None
        beste_quote = None
        for game in resp.json():
            h = game.get("home_team", "").lower()
            a = game.get("away_team", "").lower()
            if home.lower()[:4] in h or away.lower()[:4] in a:
                # Alle Bookmaker durchsuchen, beste Quote nehmen
                for bookmaker in game.get("bookmakers", []):
                    for market in bookmaker.get("markets", []):
                        if market.get("key") == "totals":
                            for outcome in market.get("outcomes", []):
                                q = round(outcome.get("price", 0), 2)
                                if q > 1.0 and (beste_quote is None or q > beste_quote):
                                    beste_quote = q
        return beste_quote
    except:
        return None

def kelly_einsatz(quote: float, typ: str) -> float:
    """Berechnet den optimalen Einsatz nach Kelly-Kriterium."""
    if not quote or quote <= 1.0:
        return EINSATZ
    ges = statistik[typ]["gewonnen"] + statistik[typ]["verloren"]
    if ges < 10:
        return EINSATZ  # Zu wenig Daten – Standard-Einsatz
    p = statistik[typ]["gewonnen"] / ges  # historische Trefferquote
    b = quote - 1.0                        # Nettogewinn pro €
    kelly = (b * p - (1 - p)) / b
    kelly = max(0, kelly) * KELLY_FRACTION  # Vorsichtiges Kelly
    einsatz = round(kelly * 100, 2)         # Auf 100€ Bankroll gerechnet
    return max(KELLY_MIN_EINSATZ, min(einsatz, KELLY_MAX_EINSATZ))

def get_wetter(country: str) -> dict:
    """Holt Wetterdaten für ein Land (gecacht, 30 Min TTL)."""
    now = time.time()
    if country in _wetter_cache and now - _wetter_cache[country]["ts"] < WETTER_TTL:
        return _wetter_cache[country]
    try:
        resp = requests.get(f"https://wttr.in/{country}?format=j1", timeout=6)
        if resp.status_code != 200:
            return {"wind": 0, "regen": 0, "ts": now}
        current  = resp.json()["current_condition"][0]
        wind     = int(current.get("windspeedKmph", 0))
        regen    = float(current.get("precipMM", 0))
        result   = {"wind": wind, "regen": regen, "ts": now}
        _wetter_cache[country] = result
        print(f"  [Wetter] {country}: {wind} km/h Wind, {regen} mm Regen")
        return result
    except:
        return {"wind": 0, "regen": 0, "ts": now}

def schlechtes_wetter(country: str) -> bool:
    """Gibt True zurück wenn das Wetter die Spielbedingungen beeinflusst."""
    w = get_wetter(country)
    return w["wind"] >= WETTER_WIND_GRENZE or w["regen"] >= WETTER_REGEN_GRENZE

# ── Liga-Filter ──────────────────────────────────────────────
def liga_erlaubt(liga: str) -> bool:
    """Gibt False zurück wenn eine Liga nach genug Daten eine schlechte Trefferquote hat."""
    if liga not in liga_statistik:
        return True  # Keine Daten → erlaubt
    s     = liga_statistik[liga]
    ges   = s["gewonnen"] + s["verloren"]
    if ges < LIGA_MIN_TIPPS:
        return True  # Zu wenig Daten → erlaubt
    quote = s["gewonnen"] / ges
    if quote < LIGA_MIN_TREFFERQUOTE:
        print(f"  [Liga-Filter] {liga} gesperrt: {s['gewonnen']}/{ges} ({round(quote*100)}%) < {round(LIGA_MIN_TREFFERQUOTE*100)}%")
        return False
    return True

# ── Ecken-Durchschnitt ───────────────────────────────────────
def get_team_ecken_avg(team_id: str) -> float | None:
    """Holt durchschnittliche Ecken aus letzten N Spielen eines Teams (gecacht)."""
    now = time.time()
    if team_id in _ecken_avg_cache:
        cached = _ecken_avg_cache[team_id]
        if now - cached["ts"] < ECKEN_AVG_TTL:
            return cached["avg"]
    try:
        params  = {**LS_AUTH, "team_id": team_id, "number": ECKEN_HISTORY_SPIELE}
        resp    = api_get_with_retry(f"{LS_BASE}/matches/history.json", params)
        matches = resp.json().get("data", {}).get("match", []) or []
        ecken_liste = []
        for m in matches:
            mid = str(m.get("id", ""))
            if not mid:
                continue
            try:
                stats  = ls_get_statistiken(mid)
                ecken  = stats["corners_home"] + stats["corners_away"]
                if ecken > 0:
                    ecken_liste.append(ecken)
            except:
                continue
        if len(ecken_liste) >= 3:
            avg = round(sum(ecken_liste) / len(ecken_liste), 1)
            _ecken_avg_cache[team_id] = {"avg": avg, "ts": now}
            print(f"  [Ecken-Avg] Team {team_id}: ⌀ {avg} Ecken ({len(ecken_liste)} Spiele)")
            return avg
        return None
    except Exception as e:
        print(f"  [Ecken-Avg] Fehler für Team {team_id}: {e}")
        return None

def ecken_tipp_sinnvoll(game: dict, grenze: float) -> bool:
    """
    Prüft ob der Ecken-Unter-Tipp mit dem historischen Durchschnitt übereinstimmt.
    Tipp wird nur gesendet wenn: Grenze > Durchschnitt + ECKEN_TOLERANZ
    → d.h. wir erwarten wirklich weniger Ecken als die Grenze erlaubt.
    """
    home_id = str((game.get("home") or {}).get("id", ""))
    away_id = str((game.get("away") or {}).get("id", ""))
    if not home_id or not away_id:
        return True  # Keine IDs → kein Filter
    avg_home = get_team_ecken_avg(home_id)
    avg_away = get_team_ecken_avg(away_id)
    if avg_home is None or avg_away is None:
        return True  # Keine Daten → kein Filter
    # Erwartete Ecken = Mittelwert beider Team-Durchschnitte
    erwartet = round((avg_home + avg_away) / 2, 1)
    sinnvoll = grenze > erwartet + ECKEN_TOLERANZ
    print(f"  [Ecken-Avg] Heim ⌀{avg_home} | Gast ⌀{avg_away} | Erwartet: {erwartet} | Grenze: {grenze} | Sinnvoll: {sinnvoll}")
    return sinnvoll

# ── H2H Tore ─────────────────────────────────────────────────
_h2h_cache = {}
H2H_TTL    = 3600

def get_h2h_daten(team1_id: str, team2_id: str) -> list:
    """Holt H2H-Geschichte beider Teams (gecacht, 1h)."""
    key = f"{min(team1_id, team2_id)}_{max(team1_id, team2_id)}"
    now = time.time()
    if key in _h2h_cache and now - _h2h_cache[key]["ts"] < H2H_TTL:
        return _h2h_cache[key]["data"]
    try:
        params  = {**LS_AUTH, "team1_id": team1_id, "team2_id": team2_id,
                   "number": H2H_MAX_SPIELE}
        resp    = api_get_with_retry(f"{LS_BASE}/matches/h2h.json", params)
        matches = resp.json().get("data", {}).get("match", []) or []
        _h2h_cache[key] = {"data": matches, "ts": now}
        if len(matches) > 0:
            print(f"  [H2H] {team1_id} vs {team2_id}: {len(matches)} Spiele gefunden")
        return matches
    except Exception as e:
        print(f"  [H2H] Fehler: {e}")
        return []

def analysiere_h2h_tore(matches: list) -> dict | None:
    """Analysiert H2H-Spiele und berechnet Tor-Durchschnitte für HZ1 und VZ."""
    if len(matches) < H2H_MIN_SPIELE:
        return None
    hz1_liste = []
    vz_liste  = []
    for m in matches:
        score = (m.get("scores") or {}).get("score", "")
        ht    = (m.get("scores") or {}).get("ht_score", "")
        h, a  = parse_score(score)
        if h + a == 0 and not score:
            continue
        vz_liste.append(h + a)
        if ht:
            hh, ha = parse_score(ht)
            hz1_liste.append(hh + ha)
    if len(vz_liste) < H2H_MIN_SPIELE:
        return None
    return {
        "avg_vz":     round(sum(vz_liste) / len(vz_liste), 2),
        "avg_hz1":    round(sum(hz1_liste) / len(hz1_liste), 2) if len(hz1_liste) >= H2H_MIN_SPIELE else None,
        "spiele":     len(vz_liste),
        "hz1_spiele": len(hz1_liste),
    }

def tipp_aus_avg(avg: float, ueber_grenze: float, unter_grenze: float):
    if avg > ueber_grenze:
        linie = int(avg - 0.5) + 0.5
        return ("über", linie)
    elif avg < unter_grenze:
        linie = int(avg) + 0.5
        return ("unter", linie)
    return None

# ── Konfidenz-Score (1-10) ───────────────────────────────────
def berechne_konfidenz(typ, liga, quote, h2h_spiele=0,
                        wetter_schlecht=False, bookmaker_anzahl=0,
                        form_uebereinstimmung=True):
    score = 5
    if liga in liga_statistik:
        s   = liga_statistik[liga]
        ges = s["gewonnen"] + s["verloren"]
        if ges >= 5:
            hit = s["gewonnen"] / ges
            if hit >= 0.65:   score += 2
            elif hit >= 0.55: score += 1
            elif hit <= 0.35: score -= 2
            elif hit <= 0.45: score -= 1
    if h2h_spiele >= 8:   score += 1
    elif 0 < h2h_spiele < 3: score -= 1
    if not form_uebereinstimmung: score -= 2
    if wetter_schlecht:           score -= 1
    if quote:
        if quote >= 2.0:   score += 1
        elif quote < 1.35: score -= 1
    if bookmaker_anzahl >= 4:   score += 1
    elif bookmaker_anzahl == 1: score -= 1
    return max(1, min(10, score))

def konfidenz_emoji(score):
    if score >= 8: return "🟢"
    if score >= 6: return "🟡"
    if score >= 4: return "🟠"
    return "🔴"

# ── Quoten-Validierung ───────────────────────────────────────
def get_quote_details(home, away):
    if not ODDS_API_KEY:
        return {"quote": None, "avg_quote": None, "bookmaker_anzahl": 0}
    try:
        url    = "https://api.the-odds-api.com/v4/sports/soccer/odds/"
        params = {"apiKey": ODDS_API_KEY, "regions": "eu",
                  "markets": "totals", "oddsFormat": "decimal"}
        resp   = requests.get(url, params=params, timeout=8)
        if resp.status_code != 200:
            return {"quote": None, "avg_quote": None, "bookmaker_anzahl": 0}
        alle_quotes = []
        for game in resp.json():
            h = game.get("home_team", "").lower()
            a = game.get("away_team", "").lower()
            if home.lower()[:4] in h or away.lower()[:4] in a:
                for bookmaker in game.get("bookmakers", []):
                    for market in bookmaker.get("markets", []):
                        if market.get("key") == "totals":
                            for outcome in market.get("outcomes", []):
                                q = outcome.get("price", 0)
                                if q > 1.0:
                                    alle_quotes.append(round(q, 2))
        if not alle_quotes:
            return {"quote": None, "avg_quote": None, "bookmaker_anzahl": 0}
        return {
            "quote":            max(alle_quotes),
            "avg_quote":        round(sum(alle_quotes) / len(alle_quotes), 2),
            "bookmaker_anzahl": len(alle_quotes),
        }
    except:
        return {"quote": None, "avg_quote": None, "bookmaker_anzahl": 0}



def get_odds_vergleich(home: str, away: str) -> str:
    """Gibt formatierten Odds-Vergleich der Top-Bookmaker zurück."""
    if not ODDS_API_KEY:
        return ""
    try:
        url    = "https://api.the-odds-api.com/v4/sports/soccer/odds/"
        params = {"apiKey": ODDS_API_KEY, "regions": "eu",
                  "markets": "totals", "oddsFormat": "decimal"}
        resp   = requests.get(url, params=params, timeout=8)
        if resp.status_code != 200:
            return ""
        bm_namen = {
            "bet365": "Bet365", "unibet": "Unibet", "betway": "Betway",
            "williamhill": "William Hill", "bwin": "Bwin",
            "pinnacle": "Pinnacle", "betfair": "Betfair",
            "1xbet": "1xBet", "betsson": "Betsson",
        }
        beste = {}
        for game in resp.json():
            h = game.get("home_team", "").lower()
            a = game.get("away_team", "").lower()
            if home.lower()[:4] not in h and away.lower()[:4] not in a:
                continue
            for bm in game.get("bookmakers", []):
                name    = bm.get("key", "").lower()
                anzeige = bm_namen.get(name, name.capitalize())
                for market in bm.get("markets", []):
                    if market.get("key") == "totals":
                        for outcome in market.get("outcomes", []):
                            q = round(outcome.get("price", 0), 2)
                            if q > 1.0:
                                if anzeige not in beste or q > beste[anzeige]:
                                    beste[anzeige] = q
        if not beste:
            return ""
        top4   = sorted(beste.items(), key=lambda x: x[1], reverse=True)[:4]
        zeilen = " | ".join([f"{bm}: <b>{q}</b>" for bm, q in top4])
        return f"\n📊 Quotes: {zeilen}"
    except:
        return ""

# ── Gegentipp-Schutz ────────────────────────────────────────
def gegentipp_registrieren(match_id, markt, richtung, bot):
    aktive_tipps.setdefault(match_id, [])
    aktive_tipps[match_id].append({"markt": markt, "richtung": richtung, "bot": bot})

def gegentipp_check(match_id, markt, richtung, bot):
    for tipp in aktive_tipps.get(match_id, []):
        if tipp["markt"] == markt and tipp["richtung"] != richtung:
            print(f"  [Gegentipp] Widerspruch: {bot} ({richtung}) vs {tipp['bot']} ({tipp['richtung']}) fuer Match {match_id}")
            return False
    return True

# ── H2H + Saisonform ────────────────────────────────────────
def get_team_saisonform(team_id):
    try:
        params  = {**LS_AUTH, "team_id": team_id, "number": 5}
        resp    = api_get_with_retry(f"{LS_BASE}/matches/history.json", params)
        matches = resp.json().get("data", {}).get("match", []) or []
        tore    = []
        for m in matches:
            score = (m.get("scores") or {}).get("score", "")
            h, a  = parse_score(score)
            if h + a > 0 or score:
                tore.append(h + a)
        return round(sum(tore) / len(tore), 2) if len(tore) >= 3 else None
    except:
        return None

def form_stimmt_ueberein(home_id, away_id, h2h_avg, richtung):
    form_home = get_team_saisonform(home_id)
    form_away = get_team_saisonform(away_id)
    if form_home is None or form_away is None:
        return True
    form_avg  = (form_home + form_away) / 2
    h2h_hoch  = h2h_avg > 2.5
    form_hoch = form_avg > 2.5
    if richtung == "über":
        ok = h2h_hoch and form_hoch
    else:
        ok = (not h2h_hoch) and (not form_hoch)
    if not ok:
        print(f"  [Form] H2H {h2h_avg} vs Form {form_avg:.1f} weichen ab")
    return ok

# ── Closing Line Value ───────────────────────────────────────
def clv_auswerten(spiel):
    einstieg = spiel.get("quote")
    if not einstieg:
        return ""
    try:
        details = get_quote_details(spiel["home"], spiel["away"])
        schluss = details.get("avg_quote")
        if not schluss:
            return ""
        diff = round((einstieg - schluss) / schluss, 3)
        if diff > CLV_WARNSCHWELLE:
            return f"\n📊 CLV: Guter Einstieg! {einstieg} -> Schluss {schluss} (+{round(diff*100,1)}%)"
        elif diff < -CLV_WARNSCHWELLE:
            return f"\n📊 CLV: Quote gesunken: {einstieg} -> Schluss {schluss} ({round(diff*100,1)}%)"
        return f"\n📊 CLV: Quote stabil: {einstieg} -> Schluss {schluss}"
    except:
        return ""

# ── Standings-Analyse ────────────────────────────────────────
def get_standings(league_id: str) -> list:
    """Holt Tabelle für eine Liga (gecacht 30 Min)."""
    now = time.time()
    lid = str(league_id)
    if lid in _standings_cache and now - _standings_cache[lid]["ts"] < STANDINGS_TTL:
        return _standings_cache[lid]["data"]
    try:
        params    = {**LS_AUTH, "league_id": lid}
        resp      = api_get_with_retry(f"{LS_BASE}/leagues/standings.json", params)
        standings = resp.json().get("data", {}).get("standings", []) or []
        _standings_cache[lid] = {"data": standings, "ts": now}
        return standings
    except:
        return []

def get_team_standing(league_id: str, team_id: str) -> dict | None:
    """Gibt Tabelleninfo eines Teams zurück."""
    standings = get_standings(league_id)
    for t in standings:
        if str(t.get("team_id", "")) == str(team_id):
            gf = _safe_int(t.get("overall_league_GF", 0))
            ga = _safe_int(t.get("overall_league_GA", 0))
            gp = _safe_int(t.get("overall_league_payed", t.get("overall_league_played", 0)))
            return {
                "position":  _safe_int(t.get("overall_league_position", t.get("position", 0))),
                "punkte":    _safe_int(t.get("overall_league_PTS", 0)),
                "gespielt":  gp,
                "siege":     _safe_int(t.get("overall_league_W", 0)),
                "unent":     _safe_int(t.get("overall_league_D", 0)),
                "niederl":   _safe_int(t.get("overall_league_L", 0)),
                "tore_f":    gf,
                "tore_g":    ga,
                "tore_avg":  round(gf / gp, 1) if gp > 0 else 0,
                "form":      t.get("league_team_form", ""),
            }
    return None

def form_zu_emojis(form: str) -> str:
    """Wandelt Form-String in Emojis um: W→🟢 D→🟡 L→🔴"""
    result = ""
    for c in (form or "")[-5:]:
        if c == "W":   result += "🟢"
        elif c == "D": result += "🟡"
        elif c == "L": result += "🔴"
    return result or "–"

def baue_analyse_text(home: str, away: str, home_id: str, away_id: str,
                       league_id: str, extra: dict = None) -> str:
    """Erstellt kompakten Analyse-Text für Signal-Nachricht und Claude-Review."""
    h_stand = get_team_standing(league_id, home_id)
    a_stand = get_team_standing(league_id, away_id)
    zeilen  = []
    if h_stand:
        hf = form_zu_emojis(h_stand["form"])
        zeilen.append(f"🏠 {home}: Platz {h_stand['position']} | {hf} | ⚽ {h_stand['tore_avg']}/Spiel")
    if a_stand:
        af = form_zu_emojis(a_stand["form"])
        zeilen.append(f"✈️ {away}: Platz {a_stand['position']} | {af} | ⚽ {a_stand['tore_avg']}/Spiel")
    if extra:
        for k, v in extra.items():
            zeilen.append(f"{k}: {v}")
    return "\n".join(zeilen)

# ── Claude Tipp-Reviewer ─────────────────────────────────────
def claude_tipp_review(home: str, away: str, typ: str, analyse: str) -> tuple:
    """
    Fragt Claude ob der Tipp Sinn ergibt.
    Gibt (empfohlen: bool, begruendung: str) zurück.
    """
    if not ANTHROPIC_API_KEY or ANTHROPIC_API_KEY.startswith("ANTHROPIC"):
        return True, ""
    try:
        typ_namen = {
            "ecken": "Ecken Unter", "ecken_over": "Ecken Über",
            "karten": "Karten Über 5", "torwart": "Mind. 1 Tor",
            "druck": "Druck/Ecken Dominanz", "comeback": "Beide Teams treffen",
            "torflut": "Torreich", "rotkarte": "Überzahl Tor",
            "hz1tore": "HZ1 Tore", "vztore": "Vollzeit Tore",
        }.get(typ, typ)
        prompt = (
            f"Du bist ein erfahrener Sportwetten-Analyst. "
            f"Bewerte diesen Tipp in max. 1 Satz auf Deutsch:\n\n"
            f"Spiel: {home} vs {away}\n"
            f"Tipp: {typ_namen}\n"
            f"Daten:\n{analyse}\n\n"
            f"Antworte NUR so:\n"
            f"EMPFOHLEN: [1 Satz Begründung]\n"
            f"oder\n"
            f"SKEPTISCH: [1 Satz Begründung]"
        )
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "Content-Type": "application/json",
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
            },
            json={
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 120,
                "messages": [{"role": "user", "content": prompt}]
            },
            timeout=15
        )
        if resp.status_code != 200:
            return True, ""
        text       = resp.json().get("content", [{}])[0].get("text", "").strip()
        empfohlen  = text.startswith("EMPFOHLEN")
        begruendung = text.replace("EMPFOHLEN:", "").replace("SKEPTISCH:", "").strip()
        return empfohlen, begruendung
    except Exception as e:
        print(f"  [Claude] Review Fehler: {e}")
        return True, ""

# ── Dynamisches Intervall ────────────────────────────────────
def dynamischer_sleep(matches: list = None):
    """
    Schläft kürzer wenn Spiele kurz vor Halbzeit oder Spielende sind.
    Normal: 3 Min | Kritische Minuten (43-46, 87-92): 1 Min
    """
    try:
        for m in (matches or []):
            minute = _safe_int(m.get("time", 0))
            if 43 <= minute <= 47 or 87 <= minute <= 93:
                time.sleep(60)
                return
    except:
        pass
    time.sleep(FUSSBALL_INTERVAL * 60)

def signal_eintragen(match_id, typ, home, away, liga, hz1_wert, grenze, quote, einsatz):
    """Trägt ein neues Signal im Log ein."""
    beobachtete_spiele_speichern()  # Persistenz bei jedem neuen Signal
    signal_log.append({
        "match_id": match_id, "typ": typ,
        "home": home, "away": away, "liga": liga,
        "hz1_wert": hz1_wert, "grenze": grenze,
        "quote": quote, "einsatz": einsatz,
        "zeit": de_now().strftime("%Y-%m-%d %H:%M"),
        "gewonnen": None  # wird nach Spielende gesetzt
    })
    signal_log_speichern()

def signal_auswertung_aktualisieren(match_id, gewonnen):
    """Setzt das Ergebnis für einen Signal-Log-Eintrag."""
    for s in reversed(signal_log):
        if s["match_id"] == match_id and s["gewonnen"] is None:
            s["gewonnen"] = gewonnen
            signal_log_speichern()
            break

# ── Persistenz: beobachtete_spiele ──────────────────────────
def beobachtete_spiele_speichern():
    """Speichert beobachtete_spiele damit sie einen Neustart überleben."""
    import json
    try:
        with open(BEOBACHTETE_DATEI, "w") as f:
            json.dump(beobachtete_spiele, f, indent=2)
    except Exception as e:
        print(f"  [Persistenz] Speicherfehler: {e}")

def beobachtete_spiele_laden():
    """Lädt beobachtete_spiele aus dem letzten Run."""
    import json, os
    if not os.path.exists(BEOBACHTETE_DATEI):
        return
    try:
        with open(BEOBACHTETE_DATEI, "r") as f:
            data = json.load(f)
        beobachtete_spiele.update(data)
        print(f"  [Persistenz] {len(data)} beobachtete Spiele geladen")
    except Exception as e:
        print(f"  [Persistenz] Ladefehler: {e}")

# ── Bankroll-Tracking ────────────────────────────────────────
def bankroll_laden() -> float:
    """Lädt aktuelle Bankroll aus Datei."""
    import json, os
    if not os.path.exists(BANKROLL_DATEI):
        bankroll_speichern(BANKROLL)
        return BANKROLL
    try:
        with open(BANKROLL_DATEI, "r") as f:
            return json.load(f).get("bankroll", BANKROLL)
    except:
        return BANKROLL

def bankroll_speichern(betrag: float):
    """Speichert aktuelle Bankroll."""
    import json
    try:
        with open(BANKROLL_DATEI, "w") as f:
            json.dump({"bankroll": round(betrag, 2), "aktualisiert": de_now().strftime("%Y-%m-%d %H:%M")}, f)
    except Exception as e:
        print(f"  [Bankroll] Speicherfehler: {e}")

def bankroll_aktualisieren(gewonnen: bool, einsatz: float, quote: float = None):
    """Aktualisiert Bankroll nach Tipp-Auswertung."""
    br = bankroll_laden()
    if gewonnen and quote:
        gewinn = round(einsatz * (quote - 1), 2)
        br    += gewinn
        print(f"  [Bankroll] +{gewinn}€ | Neue Bankroll: {br}€")
    else:
        br -= einsatz
        print(f"  [Bankroll] -{einsatz}€ | Neue Bankroll: {br}€")
    bankroll_speichern(br)
    return br

def kelly_einsatz_bankroll(quote: float, typ: str) -> float:
    """Kelly-Einsatz basierend auf echter Bankroll."""
    br  = bankroll_laden()
    ges = statistik[typ]["gewonnen"] + statistik[typ]["verloren"]
    if ges < 10 or not quote or quote <= 1.0:
        return min(round(br * 0.02, 2), KELLY_MAX_EINSATZ)  # 2% der Bankroll
    p      = statistik[typ]["gewonnen"] / ges
    b      = quote - 1.0
    kelly  = max(0, (b * p - (1 - p)) / b) * KELLY_FRACTION
    einsatz = round(kelly * br, 2)
    return max(KELLY_MIN_EINSATZ, min(einsatz, KELLY_MAX_EINSATZ))

# ── API-Monitor ──────────────────────────────────────────────
_api_monitor = {"heute": 0, "datum": ""}

def api_monitor_increment():
    """Zählt jeden API-Call mit."""
    heute_str = de_now().strftime("%Y-%m-%d")
    if _api_monitor["datum"] != heute_str:
        _api_monitor["heute"] = 0
        _api_monitor["datum"] = heute_str
    _api_monitor["heute"] += 1

def api_monitor_bericht() -> str:
    """Gibt API-Nutzung zurück."""
    n   = _api_monitor["heute"]
    pct = round(n / 50000 * 100, 1)
    bar = "█" * int(pct / 5) + "░" * (20 - int(pct / 5))
    return f"📡 API heute: <b>{n:,}</b>/50.000 ({pct}%)\n{bar}"

# ── Multi-Signal Boost ───────────────────────────────────────
def multi_signal_check(match_id: str, aktueller_bot: str) -> int:
    """
    Gibt Konfidenz-Bonus zurück wenn bereits ein anderer Bot
    dieses Spiel beobachtet – mehrere Bots = stärkeres Signal.
    """
    if match_id in beobachtete_spiele:
        erster_bot = beobachtete_spiele[match_id].get("bot", "")
        if erster_bot and erster_bot != aktueller_bot:
            print(f"  [Multi-Signal] {match_id}: {erster_bot} + {aktueller_bot} → +{MULTI_SIGNAL_BONUS} Konfidenz")
            return MULTI_SIGNAL_BONUS
    return 0

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
FARBE_HZ1TORE             = 0x27AE60
FARBE_VZTORE              = 0x8E44AD
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

def discord_hz1tore_tipp(home, away, comp, country, richtung, linie,
                          avg_hz1, spiele, quote):
    qt = f"\n💶 **Quote:** {quote}" if quote else ""
    pfeil = "📈" if richtung == "über" else "📉"
    return {
        "title": f"🥅 HZ1-Tore Signal ({richtung.upper()} {linie})",
        "color": FARBE_HZ1TORE,
        "fields": [
            {"name": "🏆 Liga",        "value": f"{comp} ({country})", "inline": True},
            {"name": "⚽ Spiel",       "value": f"{home} vs {away}",   "inline": True},
            {"name": "📊 H2H Basis",   "value": f"**{spiele}** Spiele", "inline": True},
            {"name": f"{pfeil} H2H Ø HZ1-Tore", "value": f"**{avg_hz1}** Tore", "inline": True},
            {"name": "🎯 Empfehlung",
             "value": f"**{richtung.capitalize()} {linie}** Tore (HZ1){qt}", "inline": False},
        ],
        "footer": {"text": f"H2H-Analyse • {heute()} {jetzt()}"},
    }

def discord_vztore_tipp(home, away, comp, country, richtung, linie,
                         avg_vz, spiele, quote):
    qt = f"\n💶 **Quote:** {quote}" if quote else ""
    pfeil = "📈" if richtung == "über" else "📉"
    return {
        "title": f"🏆 Vollzeit-Tore Signal ({richtung.upper()} {linie})",
        "color": FARBE_VZTORE,
        "fields": [
            {"name": "🏆 Liga",         "value": f"{comp} ({country})", "inline": True},
            {"name": "⚽ Spiel",        "value": f"{home} vs {away}",   "inline": True},
            {"name": "📊 H2H Basis",    "value": f"**{spiele}** Spiele", "inline": True},
            {"name": f"{pfeil} H2H Ø VZ-Tore", "value": f"**{avg_vz}** Tore", "inline": True},
            {"name": "🎯 Empfehlung",
             "value": f"**{richtung.capitalize()} {linie}** Tore (Vollzeit){qt}", "inline": False},
        ],
        "footer": {"text": f"H2H-Analyse • {heute()} {jetzt()}"},
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
        "hz1tore":  "🥅 Auswertung – HZ1 Tore",
        "vztore":   "🏆 Auswertung – Vollzeit Tore",
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

def update_statistik(typ, gewonnen, quote, liga=None, match_id=None):
    stunde = str(de_now().hour)
    emoji  = "✅" if gewonnen else "❌"
    print(f"  [Statistik] {emoji} {typ.upper()} | Quote: {quote} | Liga: {liga}")
    if gewonnen:
        gewinn = round((quote - 1) * EINSATZ, 2) if quote else round(EINSATZ * 0.7, 2)
        statistik[typ]["gewonnen"]        += 1
        statistik[typ]["gewinn"]          += gewinn
        wochen_statistik[typ]["gewonnen"] += 1
        wochen_statistik[typ]["gewinn"]   += gewinn
        stunden_statistik[stunde]["gewonnen"] += 1
        if liga:
            liga_statistik.setdefault(liga, {"gewonnen": 0, "verloren": 0})
            liga_statistik[liga]["gewonnen"] += 1
    else:
        statistik[typ]["verloren"]        += 1
        statistik[typ]["gewinn"]          -= EINSATZ
        wochen_statistik[typ]["verloren"] += 1
        wochen_statistik[typ]["gewinn"]   -= EINSATZ
        stunden_statistik[stunde]["verloren"] += 1
        if liga:
            liga_statistik.setdefault(liga, {"gewonnen": 0, "verloren": 0})
            liga_statistik[liga]["verloren"] += 1
    if match_id:
        signal_auswertung_aktualisieren(match_id, gewonnen)
    # Streak aktualisieren
    global streak_aktuell, streak_beste
    if gewonnen:
        streak_aktuell = max(1, streak_aktuell + 1) if streak_aktuell >= 0 else 1
        streak_beste   = max(streak_beste, streak_aktuell)
        if streak_aktuell >= 3:
            send_telegram(f"🔥 <b>{streak_aktuell} Tipps in Folge gewonnen!</b> Streak läuft! 💪")
    else:
        streak_aktuell = min(-1, streak_aktuell - 1) if streak_aktuell <= 0 else -1
        if streak_aktuell <= -3:
            send_telegram(f"⚠️ <b>{abs(streak_aktuell)} Tipps in Folge verloren.</b> Einsätze prüfen!")
    # Bankroll aktualisieren
    einsatz_wert = EINSATZ
    bankroll_aktualisieren(gewonnen, einsatz_wert, quote)
    statistik_speichern()

def claude_verloren_analyse(home: str, away: str, typ: str, details: str):
    """Claude analysiert automatisch warum ein Tipp verloren hat."""
    if not ANTHROPIC_API_KEY or ANTHROPIC_API_KEY.startswith("ANTHROPIC"):
        return
    try:
        typ_namen = {
            "ecken": "Ecken Unter", "ecken_over": "Ecken Über",
            "karten": "Karten Über 5", "torwart": "Mind. 1 Tor",
            "druck": "Druck Signal", "comeback": "Comeback",
            "torflut": "Torflut", "rotkarte": "Rote Karte",
            "hz1tore": "HZ1 Tore", "vztore": "Vollzeit Tore",
        }.get(typ, typ)
        prompt = (
            f"Ein Sportwetten-Tipp hat verloren. Analysiere kurz auf Deutsch warum:\n\n"
            f"Spiel: {home} vs {away}\n"
            f"Tipp-Typ: {typ_namen}\n"
            f"Details: {details}\n\n"
            f"Antworte in max. 2 Sätzen was schiefgelaufen ist und was man "
            f"beim nächsten Mal beachten sollte. Nur die Analyse, kein Kommentar."
        )
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"Content-Type": "application/json",
                     "x-api-key": ANTHROPIC_API_KEY,
                     "anthropic-version": "2023-06-01"},
            json={"model": "claude-sonnet-4-20250514", "max_tokens": 150,
                  "messages": [{"role": "user", "content": prompt}]},
            timeout=15
        )
        if resp.status_code != 200:
            return
        analyse = resp.json().get("content", [{}])[0].get("text", "").strip()
        if analyse:
            msg = (f"🔍 <b>Verlust-Analyse ({typ_namen})</b>\n"
                   f"━━━━━━━━━━━━━━━━━━━━\n"
                   f"📌 {home} vs {away}\n"
                   f"🤖 {analyse}\n"
                   f"━━━━━━━━━━━━━━━━━━━━")
            send_telegram(msg)
    except Exception as e:
        print(f"  [Verlust-Analyse] Fehler: {e}")

def bot_rangliste() -> str:
    """Erstellt Rangliste aller Bots nach Trefferquote."""
    bots = {
        "📐 Ecken Unter":  wochen_statistik["ecken"],
        "📐 Ecken Über":   wochen_statistik["ecken_over"],
        "🃏 Karten":        wochen_statistik["karten"],
        "🧤 Torwart":       wochen_statistik["torwart"],
        "🔥 Druck":         wochen_statistik["druck"],
        "🔄 Comeback":      wochen_statistik["comeback"],
        "🌊 Torflut":       wochen_statistik["torflut"],
        "🟥 Rotkarte":      wochen_statistik["rotkarte"],
        "🥅 HZ1-Tore":     wochen_statistik["hz1tore"],
        "🏆 VZ-Tore":       wochen_statistik["vztore"],
    }
    rang = []
    for name, stat in bots.items():
        ges = stat["gewonnen"] + stat["verloren"]
        if ges == 0:
            continue
        pct  = round(stat["gewonnen"] / ges * 100)
        rang.append((name, stat["gewonnen"], ges, pct))
    rang.sort(key=lambda x: x[3], reverse=True)
    zeilen = []
    for i, (name, gw, ges, pct) in enumerate(rang, 1):
        medal = ["🥇", "🥈", "🥉"][i-1] if i <= 3 else f"{i}."
        zeilen.append(f"{medal} {name}: {gw}/{ges} ({pct}%)")
    streak_emoji = "🔥" if streak_aktuell > 0 else "❄️"
    streak_text  = f"{streak_emoji} Streak: <b>{abs(streak_aktuell)}x {'Gewinn' if streak_aktuell > 0 else 'Verlust'}</b> | Beste: <b>{streak_beste}x</b>"
    return "\n".join(zeilen) + f"\n━━━━━━━━━━━━━━━━━━━━\n{streak_text}" if zeilen else "Noch keine Daten"

def statistik_zeile(name, stat):
    gesamt = stat["gewonnen"] + stat["verloren"]
    if gesamt == 0:
        return f"{name}: Noch keine Tipps"
    pct    = round(stat["gewonnen"] / gesamt * 100)
    gewinn = round(stat["gewinn"], 2)
    emoji  = "📈" if gewinn >= 0 else "📉"
    return f"{name}: {stat['gewonnen']}/{gesamt} ({pct}%) {emoji} {'+' if gewinn >= 0 else ''}{gewinn}€"

def send_monatsbericht():
    """Automatischer Monatsbericht am 1. des Monats."""
    import calendar
    letzter_monat = de_now().replace(day=1) - timedelta(days=1)
    monat_name    = letzter_monat.strftime("%B %Y")
    gw  = sum(statistik[t]["gewonnen"] for t in statistik)
    vl  = sum(statistik[t]["verloren"] for t in statistik)
    ges = gw + vl
    gn  = round(sum(statistik[t]["gewinn"] for t in statistik), 2)
    pct = round(gw / ges * 100) if ges else 0
    br  = bankroll_laden()
    ei  = "📈" if gn >= 0 else "📉"
    rang = bot_rangliste()
    streak_text = f"🔥 Beste Serie: <b>{streak_beste}x Gewinn</b>"
    msg = (f"📅 <b>Monatsbericht – {monat_name}</b>\n"
           f"━━━━━━━━━━━━━━━━━━━━\n"
           f"✅ Gewonnen: <b>{gw}</b>\n"
           f"❌ Verloren: <b>{vl}</b>\n"
           f"🎯 Trefferquote: <b>{pct}%</b>\n"
           f"{ei} Simulation: <b>{'+' if gn >= 0 else ''}{gn}€</b>\n"
           f"💰 Bankroll: <b>{br}€</b>\n"
           f"━━━━━━━━━━━━━━━━━━━━\n"
           f"🏆 <b>Bot-Rangliste:</b>\n{rang}\n"
           f"━━━━━━━━━━━━━━━━━━━━\n"
           f"{streak_text}\n"
           f"🕐 {jetzt()} Uhr")
    send_telegram(msg)
    send_discord(DISCORD_WEBHOOK_BILANZ, msg)
    print(f"  [Bericht] Monatsbericht gesendet ({monat_name})")

def send_tagesbericht():
    global tagesbericht_gesendet
    gw  = sum(statistik[t]["gewonnen"] for t in statistik)
    vl  = sum(statistik[t]["verloren"] for t in statistik)
    ges = gw + vl
    gn  = round(sum(statistik[t]["gewinn"] for t in statistik), 2)
    pct = round(gw / ges * 100) if ges else 0
    ei  = "📈" if gn >= 0 else "📉"
    # Beste Stunden ermitteln
    beste_stunden = sorted(
        [(h, s) for h, s in stunden_statistik.items() if s["gewonnen"] + s["verloren"] > 0],
        key=lambda x: x[1]["gewonnen"] / max(x[1]["gewonnen"] + x[1]["verloren"], 1),
        reverse=True
    )[:3]
    stunden_text = ", ".join([f"{h}:00 ({s['gewonnen']}/{s['gewonnen']+s['verloren']})"
                               for h, s in beste_stunden]) or "Noch keine Daten"
    # Beste Ligen ermitteln
    beste_ligen = sorted(
        [(l, s) for l, s in liga_statistik.items() if s["gewonnen"] + s["verloren"] > 0],
        key=lambda x: x[1]["gewonnen"] / max(x[1]["gewonnen"] + x[1]["verloren"], 1),
        reverse=True
    )[:3]
    ligen_text = "\n".join([f"  • {l}: {s['gewonnen']}/{s['gewonnen']+s['verloren']}"
                             for l, s in beste_ligen]) or "Noch keine Daten"

    # Liga-Verteilung: welche Ligen bekommen wie viele Tipps in %
    alle_liga_tipps = {l: s["gewonnen"] + s["verloren"] for l, s in liga_statistik.items() if s["gewonnen"] + s["verloren"] > 0}
    gesamt_liga_tipps = sum(alle_liga_tipps.values())
    liga_verteilung_text = ""
    if gesamt_liga_tipps > 0:
        top_ligen = sorted(alle_liga_tipps.items(), key=lambda x: x[1], reverse=True)[:5]
        zeilen = []
        for l, n in top_ligen:
            pct_l  = round(n / gesamt_liga_tipps * 100)
            s      = liga_statistik[l]
            hit    = round(s["gewonnen"] / n * 100) if n > 0 else 0
            gesperrt = " 🚫" if not liga_erlaubt(l) else ""
            zeilen.append(f"  • {l}: {pct_l}% ({hit}% Hit){gesperrt}")
        liga_verteilung_text = f"━━━━━━━━━━━━━━━━━━━━\n📊 <b>Liga-Verteilung (Top 5):</b>\n" + "\n".join(zeilen) + "\n"
    ecken_logs = [s for s in signal_log if s["typ"] == "ecken" and s["gewonnen"] is not None]
    optimizer_text = ""
    if len(ecken_logs) >= 5:
        for offset in [2, 3, 4]:
            gw = sum(1 for s in ecken_logs if s["hz1_wert"] * 2 + offset > s.get("final_ecken", s["grenze"]))
            optimizer_text += f"  x2+{offset}: {gw}/{len(ecken_logs)} ({round(gw/len(ecken_logs)*100)}%)\n"
        optimizer_text = f"━━━━━━━━━━━━━━━━━━━━\n📐 <b>Corner-Optimizer (Ecken Unter):</b>\n{optimizer_text}"

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
           f"🥅 {statistik_zeile('HZ1-Tore',      statistik['hz1tore'])}\n"
           f"🏆 {statistik_zeile('VZ-Tore',        statistik['vztore'])}\n"
           f"━━━━━━━━━━━━━━━━━━━━\n"
           f"🕐 <b>Beste Uhrzeiten:</b> {stunden_text}\n"
           f"🏆 <b>Beste Ligen:</b>\n{ligen_text}\n"
           f"{liga_verteilung_text}"
           f"{optimizer_text}"
           f"━━━━━━━━━━━━━━━━━━━━\n🕐 {jetzt()} Uhr")
    br   = bankroll_laden()
    diff = round(br - BANKROLL, 2)
    br_pfeil = "📈 +" if diff >= 0 else "📉 "
    msg += (f"\n━━━━━━━━━━━━━━━━━━━━\n"
            f"💰 <b>Bankroll:</b> {br}€ ({br_pfeil}{diff}€ seit Start)\n"
            f"{api_monitor_bericht()}")
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
           f"🥅 {statistik_zeile('HZ1-Tore',      wochen_statistik['hz1tore'])}\n"
           f"🏆 {statistik_zeile('VZ-Tore',        wochen_statistik['vztore'])}\n"
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
        ql  = f"💶 Quote: <b>{quote}</b> → Gewinn: <b>+{round((quote-1)*EINSATZ,2)}€</b>\n" if quote and gewonnen else ""
        clv = clv_auswerten(spiel)
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

def auswertung_hz1tore(spiel):
    match_id  = spiel["match_id"]
    richtung  = spiel["richtung"]
    linie     = spiel["linie"]
    home, away, quote = spiel["home"], spiel["away"], spiel.get("quote")
    try:
        match = ls_get_single_match(match_id)
        ht    = (match.get("scores") or {}).get("ht_score", "")
        if not ht:
            # HZ1 Score nicht in single match – versuche nochmal direkt
            import time as _t; _t.sleep(5)
            match = ls_get_single_match(match_id)
            ht    = (match.get("scores") or {}).get("ht_score", "")
        if not ht:
            score = match.get("scores", {}).get("score", "0 - 0")
            print(f"  [Auswertung] Hz1Tore: kein HZ1-Score für {home} vs {away} – übersprungen")
            return None  # Wird später nochmal versucht
        hh, ha   = parse_score(ht)
        hz1_tore = hh + ha
        if richtung == "über":
            gewonnen = hz1_tore > linie
        else:
            gewonnen = hz1_tore < linie
        update_statistik("hz1tore", gewonnen, quote, liga=spiel.get("liga"), match_id=match_id)
        emoji = "✅ GEWONNEN" if gewonnen else "❌ VERLOREN"
        ql = f"💶 Quote: <b>{quote}</b> → Gewinn: <b>+{round((quote-1)*spiel.get('einsatz', EINSATZ),2)}€</b>\n" if quote and gewonnen else ""
        clv = clv_auswerten(spiel)
        return (f"📊 <b>Auswertung – HZ1 Tore</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                f"📌 {home} vs {away}\n"
                f"🎯 Tipp: {richtung.capitalize()} <b>{linie}</b> Tore (HZ1)\n"
                f"📈 HZ1-Ergebnis: <b>{ht}</b> ({hz1_tore} Tore)\n{ql}{clv}"
                f"━━━━━━━━━━━━━━━━━━━━\n{emoji}\n🕐 {jetzt()} Uhr")
    except Exception as e:
        print(f"  [Auswertung] HZ1-Tore Fehler: {e}")
        return None

def auswertung_vztore(spiel):
    match_id  = spiel["match_id"]
    richtung  = spiel["richtung"]
    linie     = spiel["linie"]
    home, away, quote = spiel["home"], spiel["away"], spiel.get("quote")
    try:
        match    = ls_get_single_match(match_id)
        score    = match.get("scores", {}).get("score", "0 - 0")
        h, a     = parse_score(score)
        vz_tore  = h + a
        if richtung == "über":
            gewonnen = vz_tore > linie
        else:
            gewonnen = vz_tore < linie
        update_statistik("vztore", gewonnen, quote, liga=spiel.get("liga"), match_id=match_id)
        emoji = "✅ GEWONNEN" if gewonnen else "❌ VERLOREN"
        ql = f"💶 Quote: <b>{quote}</b> → Gewinn: <b>+{round((quote-1)*spiel.get('einsatz', EINSATZ),2)}€</b>\n" if quote and gewonnen else ""
        clv = clv_auswerten(spiel)
        return (f"📊 <b>Auswertung – Vollzeit Tore</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                f"📌 {home} vs {away}\n"
                f"🎯 Tipp: {richtung.capitalize()} <b>{linie}</b> Tore (VZ)\n"
                f"📈 Endstand: <b>{score}</b> ({vz_tore} Tore)\n{ql}{clv}"
                f"━━━━━━━━━━━━━━━━━━━━\n{emoji}\n🕐 {jetzt()} Uhr")
    except Exception as e:
        print(f"  [Auswertung] VZ-Tore Fehler: {e}")
        return None

# ============================================================
#  AUSWERTUNGS- & BERICHT-THREAD
# ============================================================

def bot_auswertung_und_berichte():
    """
    v26: Direkte FT-Erkennung via Single-Match Endpoint alle 2 Minuten.
    Viel schneller und zuverlässiger als auf Verschwinden aus Live-Liste zu warten.
    """
    global tagesbericht_gesendet
    print("[Auswertung-Bot] Gestartet | Direkte FT-Erkennung alle 2 Min.")
    letzter_wochenbericht = de_now().isocalendar()[1]

    AUSWERTUNG_FNS = {
        "ecken":      auswertung_ecken,
        "ecken_over": auswertung_ecken_over,
        "karten":     auswertung_karten,
        "torwart":    auswertung_torwart,
        "druck":      auswertung_druck,
        "comeback":   auswertung_comeback,
        "torflut":    auswertung_torflut,
        "rotkarte":   auswertung_rotkarte,
        "hz1tore":    auswertung_hz1tore,
        "vztore":     auswertung_vztore,
    }
    FT_STATI        = {"FT", "Finished", "FINISHED", "AET", "PEN", "finished", "aet", "pen"}
    leerer_status   = {}  # match_id → Anzahl leerer Status-Antworten
    ft_bestaetigung = {}  # match_id → Zeitstempel erste FT-Erkennung (Bestätigung nach 2 Min)

    while True:
        try:
            now = de_now()
            # Tagesbericht
            if now.hour == TAGESBERICHT_UHRZEIT and tagesbericht_gesendet != now.date():
                send_tagesbericht()
                tagesbericht_gesendet = now.date()
            # Wochenbericht (Montag)
            aktuelle_woche = now.isocalendar()[1]
            if now.weekday() == 0 and aktuelle_woche != letzter_wochenbericht:
                send_wochenbericht()
                letzter_wochenbericht = aktuelle_woche
            # Monatsbericht (1. des Monats)
            if now.day == 1 and now.hour == 9 and now.minute < 3:
                monatsbericht_key = f"{now.year}-{now.month}"
                if not hasattr(bot_auswertung_und_berichte, "_letzter_monat") or                    bot_auswertung_und_berichte._letzter_monat != monatsbericht_key:
                    bot_auswertung_und_berichte._letzter_monat = monatsbericht_key
                    send_monatsbericht()

            # Direkte FT-Erkennung: jeden beobachteten Match einzeln abfragen
            for match_id, spiel in list(beobachtete_spiele.items()):
                if match_id in auswertung_done:
                    continue
                try:
                    match  = ls_get_single_match(match_id)
                    status = match.get("status", "")
                    minute = _safe_int(match.get("time", 0))

                    # FIX: Leerer Status + 0 Minuten = Spiel aus API gefallen → beendet
                    if status == "" and minute == 0:
                        leerer_status[match_id] = leerer_status.get(match_id, 0) + 1
                        print(f"  [Auswertung] {spiel['home']} vs {spiel['away']} | Kein Status ({leerer_status[match_id]}x)")
                        if leerer_status[match_id] >= 8:
                            # Doppel-Check: ist das Spiel noch in der Live-Liste?
                            alle_live = get_live_matches()
                            live_ids  = {m.get("id") for m in alle_live}
                            if match_id in live_ids:
                                print(f"  [Auswertung] {spiel['home']} vs {spiel['away']} | Noch in Live-Liste – warte weiter")
                                leerer_status[match_id] = 0  # Reset – Spiel läuft noch
                                continue
                            print(f"  [Auswertung] ⚠️ Kein Status + nicht live – werte aus")
                            status = "FT"
                        else:
                            continue

                    if status not in FT_STATI:
                        print(f"  [Auswertung] {spiel['home']} vs {spiel['away']} | {status} | {minute}'")
                        leerer_status.pop(match_id, None)
                        ft_bestaetigung.pop(match_id, None)  # Status nicht FT → Reset
                        continue

                    # FT-Bestätigung: erst beim 2. FT-Signal auswerten (verhindert API-Fehler)
                    if match_id not in ft_bestaetigung:
                        ft_bestaetigung[match_id] = time.time()
                        print(f"  [Auswertung] {spiel['home']} vs {spiel['away']} | FT erkannt – warte Bestätigung")
                        continue
                    elif time.time() - ft_bestaetigung[match_id] < 90:
                        # Noch keine 90 Sekunden vergangen – warte noch
                        continue

                    # Mindest-Wartezeit je nach Tipp-Typ
                    signal_zeit = spiel.get("signal_zeit", 0)
                    minuten_seit_signal = (time.time() - signal_zeit) / 60
                    min_warte = {
                        "ecken":      50,  # Halbzeit-Signal → 2. HZ dauert ~45 Min
                        "torflut":    50,  # Halbzeit-Signal → 2. HZ dauert ~45 Min
                        "hz1tore":    35,  # Signal Min. 1-15 → HZ1 Ende ~30-45 Min weg
                        "vztore":     75,  # Signal Min. 1-15 → ganzes Spiel abwarten
                        "karten":     75,  # Signal Min. 1-40 → ganzes Spiel abwarten
                        "torwart":    30,  # 0:0 Signal → mind. 30 Min warten
                        "comeback":   25,  # In-Game Signal → mind. 25 Min warten
                        "druck":      25,  # In-Game Signal → mind. 25 Min warten
                        "rotkarte":   20,  # In-Game Signal → mind. 20 Min warten
                        "ecken_over": 20,  # In-Game Signal → mind. 20 Min warten
                    }.get(spiel.get("typ", ""), 25)
                    if signal_zeit > 0 and minuten_seit_signal < min_warte:
                        print(f"  [Auswertung] {spiel['home']} vs {spiel['away']} | Warte noch {min_warte - minuten_seit_signal:.0f} Min. ({spiel.get('typ')})")
                        leerer_status.pop(match_id, None)
                        continue

                    # Spiel beendet!
                    print(f"  [Auswertung] ✅ FT: {spiel['home']} vs {spiel['away']} ({status})")
                    time.sleep(15)

                    typ        = spiel["typ"]
                    webhook    = spiel["webhook"]
                    auswert_fn = AUSWERTUNG_FNS.get(typ)
                    msg        = auswert_fn(spiel) if auswert_fn else None

                    if msg:
                        send_telegram(msg)
                        gewonnen = "GEWONNEN" in msg
                        details  = {"📊 Typ": f"**{typ.upper()}**"}
                        embed    = discord_auswertung(typ, spiel["home"], spiel["away"], gewonnen, details)
                        send_discord_embed(webhook, embed)
                        print(f"  [Auswertung] Gesendet: {spiel['home']} vs {spiel['away']} ({typ})")
                        # Verloren-Analyse via Claude
                        if not gewonnen:
                            threading.Thread(
                                target=claude_verloren_analyse,
                                args=(spiel["home"], spiel["away"], typ, msg),
                                daemon=True
                            ).start()
                        auswertung_done.add(match_id)  # Nur als done markieren wenn erfolgreich
                    else:
                        # Auswertung fehlgeschlagen (z.B. kein HZ1-Score) → erneut versuchen
                        ft_bestaetigung.pop(match_id, None)  # Reset damit nächster Versuch klappt
                        print(f"  [Auswertung] ⚠️ Keine Auswertung für {spiel['home']} vs {spiel['away']} – versuche später")
                    leerer_status.pop(match_id, None)
                    ft_bestaetigung.pop(match_id, None)

                except Exception as e:
                    print(f"  [Auswertung] Fehler bei {match_id}: {e}")

        except Exception as e:
            print(f"  [Auswertung-Bot] Fehler: {e}")
        time.sleep(120)  # Alle 2 Minuten prüfen

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
                    print(f"  [Ecken-Bot] {home} vs {away} | Keine Ecken-Statistik von API")
                    continue
                if corners > MAX_CORNERS:
                    print(f"  [Ecken-Bot] {home} vs {away} | Zu viele Ecken: {corners} > {MAX_CORNERS}")
                    continue
                if corners <= MAX_CORNERS:
                    if not tipp_erlaubt(match_id, "Ecken-Bot"):
                        continue
                    # Liga-Filter
                    if not liga_erlaubt(comp):
                        continue
                    # Quoten-Details (beste Quote + Bookmaker-Anzahl)
                    qd      = get_quote_details(home, away)
                    quote   = qd["quote"]
                    bm_anz  = qd["bookmaker_anzahl"]
                    # Mindest-Liquidität: mind. MIN_BOOKMAKER_ANZAHL Bookmaker
                    if bm_anz > 0 and bm_anz < MIN_BOOKMAKER_ANZAHL:
                        print(f"  [Ecken-Bot] Zu wenig Bookmaker ({bm_anz}) – übersprungen")
                        continue
                    if quote and quote < MIN_QUOTE:
                        print(f"  [Ecken-Bot] Quote zu niedrig: {quote} – übersprungen")
                        continue
                    # Kein Skip wenn keine Quote vorhanden – Signal trotzdem senden
                    # Gegentipp-Schutz
                    if not gegentipp_check(match_id, "ecken", "unter", "Ecken-Bot"):
                        continue
                    # Wetter-Anpassung
                    schlecht = schlechtes_wetter(country)
                    wetter_bonus = 1 if schlecht else 0
                    grenze = corners * 2 + 3 + wetter_bonus
                    # Ecken-Durchschnitt Check
                    if not ecken_tipp_sinnvoll(game, grenze):
                        print(f"  [Ecken-Bot] Durchschnitt passt nicht – übersprungen")
                        continue
                    # Standings-Analyse
                    league_id = str((game.get("competition") or {}).get("id", ""))
                    home_id   = str((game.get("home") or {}).get("id", ""))
                    away_id   = str((game.get("away") or {}).get("id", ""))
                    analyse   = baue_analyse_text(home, away, home_id, away_id, league_id, {
                        "📐 Ecken HZ1": f"{corners} ({corners_home}|{corners_away})",
                        "🎯 Grenze":    f"Unter {grenze} gesamt",
                    })
                    konfidenz = berechne_konfidenz("ecken", comp, quote,
                        wetter_schlecht=schlecht, bookmaker_anzahl=bm_anz)
                    # Claude Review
                    cl_ok, cl_text = claude_tipp_review(home, away, "ecken", analyse)
                    if not cl_ok:
                        konfidenz = max(1, konfidenz - 2)
                    einsatz = kelly_einsatz_bankroll(quote, "ecken") if quote else EINSATZ
                    ke      = konfidenz_emoji(konfidenz)
                    ql      = f"\n💶 Quote: <b>{quote}</b> | 💰 Einsatz: <b>{einsatz}€</b>" if quote else ""
                    cl_line    = f"\n🤖 Claude: <b>{cl_text}</b>" if cl_text else ""
                    odds_vgl   = get_odds_vergleich(home, away)
                    msg     = (f"📐 <b>Ecken Tipp!</b> {ke} Konfidenz: <b>{konfidenz}/10</b>\n"
                               f"━━━━━━━━━━━━━━━━━━━━\n"
                               f"🏆 {comp} ({country})\n📌 {home} vs {away}\n"
                               f"📊 Stand: <b>{score}</b>\n"
                               f"━━━━━━━━━━━━━━━━━━━━\n"
                               f"{analyse}\n"
                               f"━━━━━━━━━━━━━━━━━━━━\n"
                               f"📐 Ecken: 🔵 {corners_home} | 🔴 {corners_away} | Gesamt: {corners}\n"
                               f"🎯 Tipp: Unter <b>{grenze}</b> Ecken gesamt{ql}{odds_vgl}{cl_line}\n"
                               f"━━━━━━━━━━━━━━━━━━━━\n🕐 {jetzt()} Uhr")
                    send_telegram(msg)
                    send_discord_embed(DISCORD_WEBHOOK_ECKEN,
                        discord_ecken_tipp(home, away, comp, country, score,
                                           corners_home, corners_away, corners, grenze, quote))
                    notified_ecken.add(match_id)
                    multi_bonus = multi_signal_check(match_id, "Ecken-Bot")
                    konfidenz   = min(10, konfidenz + multi_bonus)
                    beobachtete_spiele[match_id] = {
                        "typ": "ecken", "match_id": match_id,
                        "home": home, "away": away, "hz1_ecken": corners,
                        "quote": quote, "einsatz": einsatz, "liga": comp,
                        "webhook": DISCORD_WEBHOOK_ECKEN,
                        "signal_zeit": time.time(), "bot": "Ecken-Bot"
                    }
                    signal_eintragen(match_id, "ecken", home, away, comp, corners, grenze, quote, einsatz)
                    gegentipp_registrieren(match_id, "ecken", "unter", "Ecken-Bot")
                    print(f"  [Ecken-Bot] OK: {home} vs {away} | K:{konfidenz}/10 | Claude:{'✅' if cl_ok else '⚠️'}")
                time.sleep(0.5)
            bot_fehler_reset("Ecken-Bot")
        except Exception as e:
            bot_fehler_melden("Ecken-Bot", e)
        try:
            dynamischer_sleep(get_live_matches())
        except:
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
                if not tipp_erlaubt(match_id, "Ecken-Über-Bot"):
                    continue
                home    = game.get("home", {}).get("name", "?")
                away    = game.get("away", {}).get("name", "?")
                comp    = game.get("competition", {}).get("name", "?")
                country = (game.get("country") or {}).get("name", "International")
                score   = game.get("scores", {}).get("score", "?")
                minute  = game.get("time", "?")
                quote   = get_quote(home, away, "ecken_over")
                if quote and quote < MIN_QUOTE:
                    continue
                einsatz = kelly_einsatz_bankroll(quote, "ecken_over") if quote else EINSATZ
                ql      = f"\n💶 Quote: <b>{quote}</b> | 💰 Einsatz: <b>{einsatz}€</b>" if quote else ""
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
                    "quote": quote, "einsatz": einsatz, "liga": comp,
                    "webhook": DISCORD_WEBHOOK_ECKEN, "signal_zeit": time.time(), "bot": "Ecken-Über-Bot"
                    }
                signal_eintragen(match_id, "ecken_over", home, away, comp, corners, 14, quote, einsatz)
                print(f"  [Ecken-Über-Bot] OK: {home} vs {away} ({corners} Ecken in Min. {minute})")
                time.sleep(0.5)
            bot_fehler_reset("Ecken-Über-Bot")
        except Exception as e:
            bot_fehler_melden("Ecken-Über-Bot", e)
        try:
            dynamischer_sleep(get_live_matches())
        except:
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
                    if not tipp_erlaubt(match_id, "Karten-Bot"):
                        continue
                    quote  = get_quote(home, away, "karten")
                    if quote and quote < MIN_QUOTE:
                        continue
                    einsatz = kelly_einsatz_bankroll(quote, "karten") if quote else EINSATZ
                    ql     = f"\n💶 Quote: <b>{quote}</b> | 💰 Einsatz: <b>{einsatz}€</b>" if quote else ""
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
                        "quote": quote, "webhook": DISCORD_WEBHOOK_KARTEN, "signal_zeit": time.time(), "bot": "Karten-Bot"
                    }
                    print(f"  [Karten-Bot] OK: {home} vs {away} ({len(karten)} Karten)")
                time.sleep(0.5)
            bot_fehler_reset("Karten-Bot")
        except Exception as e:
            bot_fehler_melden("Karten-Bot", e)
        try:
            dynamischer_sleep(get_live_matches())
        except:
            time.sleep(FUSSBALL_INTERVAL * 60)

def bot_torwart():
    print(f"[Torwart-Bot] Gestartet | 0:0 + mind. {MIN_SHOTS_ON_TARGET} Schüsse")
    while True:
        try:
            matches = get_live_matches()
            aktiv   = [m for m in matches if m.get("status") in ("IN PLAY", "ADDED TIME", "HALF TIME BREAK")
                       and (_safe_int(m.get("time", 0)) >= 20 or m.get("status") == "HALF TIME BREAK")]
            print(f"[{jetzt()}] [Torwart-Bot] {len(aktiv)} aktive Spiele (ab Min. 20)")
            for game in aktiv:
                match_id = str(game.get("id"))
                if match_id in notified_torwart:
                    continue
                score = game.get("scores", {}).get("score", "")
                if "0 - 0" not in score and "0-0" not in score:
                    continue
                # FIX: home/away vor stats definieren
                home    = game.get("home", {}).get("name", "?")
                away    = game.get("away", {}).get("name", "?")
                comp    = game.get("competition", {}).get("name", "?")
                country = (game.get("country") or {}).get("name", "International")
                stats      = get_statistiken(match_id)
                shots_home = stats["shots_on_target_home"]
                shots_away = stats["shots_on_target_away"]
                shots_ges  = shots_home + shots_away
                if shots_ges < MIN_SHOTS_ON_TARGET:
                    if shots_ges > 0:  # nur loggen wenn Daten da aber zu wenig
                        print(f"  [Torwart-Bot] {home} vs {away} | Schüsse: {shots_ges}/{MIN_SHOTS_ON_TARGET} (zu wenig)")
                    continue
                if not tipp_erlaubt(match_id, "Torwart-Bot"):
                    continue
                saves_home = stats["saves_home"]
                saves_away = stats["saves_away"]
                poss_home  = stats["possession_home"]
                poss_away  = stats["possession_away"]
                status  = game.get("status", "")
                minute  = game.get("time", "?")
                min_text = "Halbzeit" if status == "HALF TIME BREAK" else f"{minute}'"
                quote   = get_quote(home, away, "torwart")
                if quote and quote < MIN_QUOTE:
                    continue
                einsatz = kelly_einsatz_bankroll(quote, "torwart") if quote else EINSATZ
                ql      = f"\n💶 Quote: <b>{quote}</b> | 💰 Einsatz: <b>{einsatz}€</b>" if quote else ""
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
                    "quote": quote, "webhook": DISCORD_WEBHOOK_TORWART, "signal_zeit": time.time(), "bot": "Torwart-Bot"
                    }
                print(f"  [Torwart-Bot] OK: {home} vs {away} | {shots_ges} Schüsse")
                time.sleep(0.5)
            bot_fehler_reset("Torwart-Bot")
        except Exception as e:
            bot_fehler_melden("Torwart-Bot", e)
        try:
            dynamischer_sleep(get_live_matches())
        except:
            time.sleep(FUSSBALL_INTERVAL * 60)

# ============================================================
#  NEUE BOTS
# ============================================================

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
                if gesamt_ecken == 0:
                    continue  # Keine API-Daten
                if gesamt_ecken < MIN_DRUCK_ECKEN:
                    continue
                # Dominantes Team bestimmen
                druck_team = None
                if c_away > 0 and c_home / c_away >= DRUCK_RATIO:
                    if c_away < 3:  # Schwächeres Team muss mind. 3 Ecken haben
                        continue
                    druck_team   = game.get("home", {}).get("name", "?")
                    ecken_stark  = c_home
                    ecken_schwach = c_away
                    fk_stark     = f_home
                    fk_schwach   = f_away
                elif c_home > 0 and c_away / c_home >= DRUCK_RATIO:
                    if c_home < 3:  # Schwächeres Team muss mind. 3 Ecken haben
                        continue
                    druck_team   = game.get("away", {}).get("name", "?")
                    ecken_stark  = c_away
                    ecken_schwach = c_home
                    fk_stark     = f_away
                    fk_schwach   = f_home
                if not druck_team:
                    continue
                if not tipp_erlaubt(match_id, "Druck-Bot"):
                    continue
                home    = game.get("home", {}).get("name", "?")
                away    = game.get("away", {}).get("name", "?")
                comp    = game.get("competition", {}).get("name", "?")
                country = (game.get("country") or {}).get("name", "International")
                score   = game.get("scores", {}).get("score", "?")
                minute  = game.get("time", "?")
                quote   = get_quote(home, away, "druck")
                if quote and quote < MIN_QUOTE:
                    continue
                einsatz = kelly_einsatz_bankroll(quote, "druck") if quote else EINSATZ
                ql      = f"\n💶 Quote: <b>{quote}</b> | 💰 Einsatz: <b>{einsatz}€</b>" if quote else ""
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
                    "quote": quote, "webhook": DISCORD_WEBHOOK_DRUCK, "signal_zeit": time.time(), "bot": "Druck-Bot"
                    }
                print(f"  [Druck-Bot] OK: {home} vs {away} | {druck_team} dominiert ({ecken_stark}:{ecken_schwach} Ecken)")
                time.sleep(0.5)
            bot_fehler_reset("Druck-Bot")
        except Exception as e:
            bot_fehler_melden("Druck-Bot", e)
        try:
            dynamischer_sleep(get_live_matches())
        except:
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
                da_h  = stats["dangerous_attacks_home"]
                da_a  = stats["dangerous_attacks_away"]
                if shots_r == 0 and shots_f == 0 and poss_r == 0:
                    continue  # Keine API-Daten, still überspringen
                # Rückliegendes Team muss Schüsse ODER gefährliche Angriffe dominieren
                da_r  = da_h if rueckliegend == home else da_a
                da_f  = da_a if rueckliegend == home else da_h
                druck_ok = (shots_r > shots_f) or (da_r > da_f * 1.3)
                if not druck_ok or poss_r <= 45:
                    continue
                if not tipp_erlaubt(match_id, "Comeback-Bot"):
                    continue
                comp    = game.get("competition", {}).get("name", "?")
                country = (game.get("country") or {}).get("name", "International")
                minute  = game.get("time", "?")
                quote   = get_quote(home, away, "comeback")
                if quote and quote < MIN_QUOTE:
                    continue
                einsatz = kelly_einsatz_bankroll(quote, "comeback") if quote else EINSATZ
                ql      = f"\n💶 Quote: <b>{quote}</b> | 💰 Einsatz: <b>{einsatz}€</b>" if quote else ""
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
                    "quote": quote, "webhook": DISCORD_WEBHOOK_COMEBACK, "signal_zeit": time.time(), "bot": "Comeback-Bot"
                    }
                print(f"  [Comeback-Bot] OK: {home} vs {away} | {rueckliegend} liegt zurück aber dominiert")
                time.sleep(0.5)
            bot_fehler_reset("Comeback-Bot")
        except Exception as e:
            bot_fehler_melden("Comeback-Bot", e)
        try:
            dynamischer_sleep(get_live_matches())
        except:
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
                if not tipp_erlaubt(match_id, "Torflut-Bot"):
                    continue
                home    = game.get("home", {}).get("name", "?")
                away    = game.get("away", {}).get("name", "?")
                comp    = game.get("competition", {}).get("name", "?")
                country = (game.get("country") or {}).get("name", "International")
                grenze  = tore_hz1 + 1  # z.B. 3 Tore HZ1 → Tipp Über 4 gesamt
                quote   = get_quote(home, away, "torflut")
                if quote and quote < MIN_QUOTE:
                    continue
                einsatz = kelly_einsatz_bankroll(quote, "torflut") if quote else EINSATZ
                ql      = f"\n💶 Quote: <b>{quote}</b> | 💰 Einsatz: <b>{einsatz}€</b>" if quote else ""
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
                    "grenze": grenze, "quote": quote, "webhook": DISCORD_WEBHOOK_TORFLUT, "signal_zeit": time.time(), "bot": "Torflut-Bot"
                    }
                print(f"  [Torflut-Bot] OK: {home} vs {away} | {tore_hz1} Tore in HZ1")
                time.sleep(0.5)
            bot_fehler_reset("Torflut-Bot")
        except Exception as e:
            bot_fehler_melden("Torflut-Bot", e)
        try:
            dynamischer_sleep(get_live_matches())
        except:
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
                if not tipp_erlaubt(match_id, "Rotkarte-Bot"):
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
                if quote and quote < MIN_QUOTE:
                    continue
                einsatz = kelly_einsatz_bankroll(quote, "rotkarte") if quote else EINSATZ
                ql    = f"\n💶 Quote: <b>{quote}</b> | 💰 Einsatz: <b>{einsatz}€</b>" if quote else ""
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
                    "quote": quote, "webhook": DISCORD_WEBHOOK_ROTKARTE, "signal_zeit": time.time(), "bot": "Rotkarte-Bot"
                    }
                print(f"  [Rotkarte-Bot] OK: {home} vs {away} | {ueberzahl_team} in Überzahl (Min. {karte_min})")
                time.sleep(0.5)
            bot_fehler_reset("Rotkarte-Bot")
        except Exception as e:
            bot_fehler_melden("Rotkarte-Bot", e)
        try:
            dynamischer_sleep(get_live_matches())
        except:
            time.sleep(FUSSBALL_INTERVAL * 60)

def bot_tore_analyse():
    """
    Kombinierter HZ1+VZ Tore Bot – analysiert H2H einmal pro Spiel
    und prüft gleichzeitig HZ1 und VZ Tipp. Halbiert die API-Calls.
    """
    print(f"[Tore-Bot] Gestartet | HZ1+VZ H2H-Analyse bis Minute {H2H_SIGNAL_BIS_MIN}")
    while True:
        try:
            matches = get_live_matches()
            frisch  = [m for m in matches
                       if m.get("status") == "IN PLAY"
                       and 1 <= _safe_int(m.get("time", 0)) <= H2H_SIGNAL_BIS_MIN]
            print(f"[{jetzt()}] [Tore-Bot] {len(frisch)} Spiele in Min. 1-{H2H_SIGNAL_BIS_MIN}")
            for game in frisch:
                match_id = str(game.get("id"))
                home    = game.get("home", {}).get("name", "?")
                away    = game.get("away", {}).get("name", "?")
                comp    = game.get("competition", {}).get("name", "?")
                country = (game.get("country") or {}).get("name", "International")
                minute  = game.get("time", "?")
                if not liga_erlaubt(comp):
                    continue
                home_id = str((game.get("home") or {}).get("id", ""))
                away_id = str((game.get("away") or {}).get("id", ""))
                if not home_id or not away_id:
                    continue

                # H2H einmal laden – für beide Bots
                h2h = get_h2h_daten(home_id, away_id)
                ana = analysiere_h2h_tore(h2h)
                if not ana:
                    continue  # Kein H2H → still überspringen

                qd     = get_quote_details(home, away)
                quote  = qd["quote"]
                bm_anz = qd["bookmaker_anzahl"]
                if quote and quote < MIN_QUOTE:
                    continue

                # ── HZ1-Tore Tipp ──────────────────────────────
                if match_id not in notified_hz1tore and ana.get("avg_hz1") is not None:
                    tipp_hz1 = tipp_aus_avg(ana["avg_hz1"], HZ1_UEBER_GRENZE, HZ1_UNTER_GRENZE)
                    if tipp_hz1 and gegentipp_check(match_id, "hz1tore", tipp_hz1[0], "Tore-Bot"):
                        richtung, linie = tipp_hz1
                        form_ok   = form_stimmt_ueberein(home_id, away_id, ana["avg_hz1"], richtung)
                        einsatz   = kelly_einsatz_bankroll(quote, "hz1tore") if quote else EINSATZ
                        konfidenz = berechne_konfidenz("hz1tore", comp, quote,
                            h2h_spiele=ana["hz1_spiele"], bookmaker_anzahl=bm_anz,
                            form_uebereinstimmung=form_ok)
                        konfidenz = min(10, konfidenz + multi_signal_check(match_id, "Tore-Bot"))
                        analyse_hz1 = f"H2H Ø HZ1-Tore: {ana['avg_hz1']} ({ana['hz1_spiele']} Spiele)\nTipp: {richtung} {linie}"
                        cl_ok, cl_text = claude_tipp_review(home, away, "hz1tore", analyse_hz1)
                        if not cl_ok: konfidenz = max(1, konfidenz - 2)
                        cl_hz1 = f"\n🤖 Claude: <b>{cl_text}</b>" if cl_text else ""
                        ql     = f"\n💶 Quote: <b>{quote}</b> | 💰 Einsatz: <b>{einsatz}€</b>" if quote else ""
                        msg    = (f"🥅 <b>HZ1-Tore Signal!</b> {konfidenz_emoji(konfidenz)} Konfidenz: <b>{konfidenz}/10</b>\n"
                                  f"━━━━━━━━━━━━━━━━━━━━\n🏆 {comp} ({country})\n📌 {home} vs {away}\n"
                                  f"⏱️ Minute: <b>{minute}'</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                                  f"📊 H2H: <b>{ana['hz1_spiele']}</b> Spiele | Ø HZ1-Tore: <b>{ana['avg_hz1']}</b>\n"
                                  f"🎯 Tipp: {richtung.capitalize()} <b>{linie}</b> Tore (HZ1){ql}{cl_hz1}\n"
                                  f"━━━━━━━━━━━━━━━━━━━━\n🕐 {jetzt()} Uhr")
                        send_telegram(msg)
                        send_discord_embed(DISCORD_WEBHOOK_HZ1TORE,
                            discord_hz1tore_tipp(home, away, comp, country, richtung, linie,
                                                  ana["avg_hz1"], ana["hz1_spiele"], quote))
                        notified_hz1tore.add(match_id)
                        beobachtete_spiele[match_id] = {
                            "typ": "hz1tore", "match_id": match_id,
                            "home": home, "away": away, "liga": comp,
                            "richtung": richtung, "linie": linie,
                            "quote": quote, "einsatz": einsatz,
                            "webhook": DISCORD_WEBHOOK_HZ1TORE,
                            "signal_zeit": time.time()
                        }
                        signal_eintragen(match_id, "hz1tore", home, away, comp, ana["avg_hz1"], linie, quote, einsatz)
                        gegentipp_registrieren(match_id, "hz1tore", richtung, "Tore-Bot")
                        print(f"  [Tore-Bot] HZ1 OK: {home} vs {away} | {richtung} {linie} (Ø {ana['avg_hz1']})")

                # ── VZ-Tore Tipp ────────────────────────────────
                if match_id not in notified_vztore:
                    tipp_vz = tipp_aus_avg(ana["avg_vz"], VZ_UEBER_GRENZE, VZ_UNTER_GRENZE)
                    if tipp_vz and gegentipp_check(match_id, "vztore", tipp_vz[0], "Tore-Bot"):
                        richtung, linie = tipp_vz
                        form_ok   = form_stimmt_ueberein(home_id, away_id, ana["avg_vz"], richtung)
                        einsatz   = kelly_einsatz_bankroll(quote, "vztore") if quote else EINSATZ
                        konfidenz = berechne_konfidenz("vztore", comp, quote,
                            h2h_spiele=ana["spiele"], bookmaker_anzahl=bm_anz,
                            form_uebereinstimmung=form_ok)
                        analyse_vz = f"H2H Ø VZ-Tore: {ana['avg_vz']} ({ana['spiele']} Spiele)\nTipp: {richtung} {linie}"
                        cl_ok, cl_text = claude_tipp_review(home, away, "vztore", analyse_vz)
                        if not cl_ok: konfidenz = max(1, konfidenz - 2)
                        cl_vz  = f"\n🤖 Claude: <b>{cl_text}</b>" if cl_text else ""
                        ql     = f"\n💶 Quote: <b>{quote}</b> | 💰 Einsatz: <b>{einsatz}€</b>" if quote else ""
                        msg    = (f"🏆 <b>Vollzeit-Tore Signal!</b> {konfidenz_emoji(konfidenz)} Konfidenz: <b>{konfidenz}/10</b>\n"
                                  f"━━━━━━━━━━━━━━━━━━━━\n🏆 {comp} ({country})\n📌 {home} vs {away}\n"
                                  f"⏱️ Minute: <b>{minute}'</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                                  f"📊 H2H: <b>{ana['spiele']}</b> Spiele | Ø VZ-Tore: <b>{ana['avg_vz']}</b>\n"
                                  f"🎯 Tipp: {richtung.capitalize()} <b>{linie}</b> Tore (VZ){ql}{cl_vz}\n"
                                  f"━━━━━━━━━━━━━━━━━━━━\n🕐 {jetzt()} Uhr")
                        send_telegram(msg)
                        send_discord_embed(DISCORD_WEBHOOK_VZTORE,
                            discord_vztore_tipp(home, away, comp, country, richtung, linie,
                                                ana["avg_vz"], ana["spiele"], quote))
                        notified_vztore.add(match_id)
                        beobachtete_spiele[match_id] = {
                            "typ": "vztore", "match_id": match_id,
                            "home": home, "away": away, "liga": comp,
                            "richtung": richtung, "linie": linie,
                            "quote": quote, "einsatz": einsatz,
                            "webhook": DISCORD_WEBHOOK_VZTORE,
                            "signal_zeit": time.time()
                        }
                        signal_eintragen(match_id, "vztore", home, away, comp, ana["avg_vz"], linie, quote, einsatz)
                        gegentipp_registrieren(match_id, "vztore", richtung, "Tore-Bot")
                        print(f"  [Tore-Bot] VZ OK: {home} vs {away} | {richtung} {linie} (Ø {ana['avg_vz']})")

                time.sleep(0.5)
            bot_fehler_reset("Tore-Bot")
        except Exception as e:
            bot_fehler_melden("Tore-Bot", e)
        try:
            dynamischer_sleep(get_live_matches())
        except:
            time.sleep(FUSSBALL_INTERVAL * 60)

# ============================================================
#  TELEGRAM BEFEHLE (/status /pause /statistik /bankroll)
# ============================================================

def bot_telegram_befehle():
    """
    Lauscht auf Telegram-Befehle und reagiert darauf.
    /status    – zeigt ob alle Bots laufen
    /pause     – pausiert alle Signale
    /start     – setzt Signale fort
    /statistik – zeigt aktuelle Tagesstatistik
    /bankroll  – zeigt aktuelle Bankroll
    /api       – zeigt API-Nutzung heute
    """
    global BOT_PAUSIERT
    print("[Telegram-Befehle] Gestartet | Lausche auf /status /pause /start /statistik")
    letzter_update_id = 0

    while True:
        try:
            url  = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
            resp = requests.get(url, params={"offset": letzter_update_id + 1, "timeout": 30}, timeout=35)
            if resp.status_code != 200:
                time.sleep(5)
                continue

            updates = resp.json().get("result", [])
            for update in updates:
                letzter_update_id = update["update_id"]
                msg_obj  = update.get("message", {})
                chat_id  = str(msg_obj.get("chat", {}).get("id", ""))
                text     = msg_obj.get("text", "").strip().lower()

                # Nur von erlaubten Chats
                if chat_id not in (TELEGRAM_CHAT_ID, TELEGRAM_CHAT_PREMATCH.lstrip("-")):
                    # Check if it's from the owner
                    if chat_id != TELEGRAM_CHAT_ID:
                        continue

                if text == "/status":
                    aktive = {t.name for t in threading.enumerate()}
                    bots   = ["Ecken-Bot", "Ecken-Über-Bot", "Karten-Bot", "Torwart-Bot",
                              "Druck-Bot", "Comeback-Bot", "Torflut-Bot", "Rotkarte-Bot",
                              "Tore-Bot", "PreMatch-Bot", "Auswertung-Bot"]
                    zeilen = "\n".join([f"{'✅' if b in aktive else '❌'} {b}" for b in bots])
                    pause  = "⏸ PAUSIERT" if BOT_PAUSIERT else "▶️ AKTIV"
                    antwort = (f"🤖 <b>Bot Status</b> – {pause}\n"
                               f"━━━━━━━━━━━━━━━━━━━━\n{zeilen}\n"
                               f"━━━━━━━━━━━━━━━━━━━━\n"
                               f"📡 {api_monitor_bericht()}\n"
                               f"🕐 {jetzt()} Uhr")
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                  json={"chat_id": chat_id, "text": antwort, "parse_mode": "HTML"}, timeout=10)

                elif text == "/pause":
                    BOT_PAUSIERT = True
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                  json={"chat_id": chat_id, "text": "⏸ <b>Alle Signale pausiert.</b>\nMit /start wieder aktivieren.", "parse_mode": "HTML"}, timeout=10)
                    print("  [Telegram] Bot pausiert via /pause")

                elif text == "/start":
                    BOT_PAUSIERT = False
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                  json={"chat_id": chat_id, "text": "▶️ <b>Signale wieder aktiv!</b>", "parse_mode": "HTML"}, timeout=10)
                    print("  [Telegram] Bot fortgesetzt via /start")

                elif text == "/statistik":
                    gw  = sum(statistik[t]["gewonnen"] for t in statistik)
                    vl  = sum(statistik[t]["verloren"] for t in statistik)
                    ges = gw + vl
                    pct = round(gw / ges * 100) if ges else 0
                    gn  = round(sum(statistik[t]["gewinn"] for t in statistik), 2)
                    streak_sym = "🔥" if streak_aktuell > 0 else "❄️"
                    antwort = (f"📊 <b>Statistik heute</b>\n"
                               f"━━━━━━━━━━━━━━━━━━━━\n"
                               f"✅ Gewonnen: <b>{gw}</b>\n"
                               f"❌ Verloren: <b>{vl}</b>\n"
                               f"🎯 Trefferquote: <b>{pct}%</b>\n"
                               f"{'📈' if gn >= 0 else '📉'} Simulation: <b>{'+' if gn >= 0 else ''}{gn}€</b>\n"
                               f"{streak_sym} Streak: <b>{abs(streak_aktuell)}x {'Gewinn' if streak_aktuell > 0 else 'Verlust'}</b>\n"
                               f"━━━━━━━━━━━━━━━━━━━━\n🕐 {jetzt()} Uhr")
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                  json={"chat_id": chat_id, "text": antwort, "parse_mode": "HTML"}, timeout=10)

                elif text == "/bankroll":
                    br     = bankroll_laden()
                    start  = BANKROLL
                    diff   = round(br - start, 2)
                    emoji  = "📈" if diff >= 0 else "📉"
                    antwort = (f"💰 <b>Bankroll</b>\n"
                               f"━━━━━━━━━━━━━━━━━━━━\n"
                               f"Start: <b>{start}€</b>\n"
                               f"Aktuell: <b>{br}€</b>\n"
                               f"{emoji} Differenz: <b>{'+' if diff >= 0 else ''}{diff}€</b>\n"
                               f"━━━━━━━━━━━━━━━━━━━━\n🕐 {jetzt()} Uhr")
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                  json={"chat_id": chat_id, "text": antwort, "parse_mode": "HTML"}, timeout=10)

                elif text == "/live":
                    try:
                        matches = get_live_matches()
                        if not matches:
                            antwort = "⚽ Gerade keine Live-Spiele."
                        else:
                            zeilen = [f"⚽ <b>Live Spiele</b> ({len(matches)} gesamt)\n━━━━━━━━━━━━━━━━━━━━"]
                            for m in matches[:15]:
                                home   = m.get("home", {}).get("name", "?")
                                away   = m.get("away", {}).get("name", "?")
                                score  = m.get("scores", {}).get("score", "? - ?")
                                minute = m.get("time", "?")
                                status = m.get("status", "")
                                min_str = "HZ" if status == "HALF TIME BREAK" else f"{minute}'"
                                zeilen.append(f"🔴 {home} <b>{score}</b> {away} | {min_str}")
                            antwort = "\n".join(zeilen)
                    except Exception as e:
                        antwort = f"❌ Fehler: {e}"
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                  json={"chat_id": chat_id, "text": antwort, "parse_mode": "HTML"}, timeout=10)

                elif text == "/rangliste":
                    rang = bot_rangliste()
                    antwort = (f"🏆 <b>Bot-Rangliste (diese Woche)</b>\n"
                               f"━━━━━━━━━━━━━━━━━━━━\n{rang}\n"
                               f"━━━━━━━━━━━━━━━━━━━━\n🕐 {jetzt()} Uhr")
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                  json={"chat_id": chat_id, "text": antwort, "parse_mode": "HTML"}, timeout=10)

                elif text == "/api":
                    antwort = f"📡 <b>API Monitor</b>\n━━━━━━━━━━━━━━━━━━━━\n{api_monitor_bericht()}\n━━━━━━━━━━━━━━━━━━━━\n🕐 {jetzt()} Uhr"
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                  json={"chat_id": chat_id, "text": antwort, "parse_mode": "HTML"}, timeout=10)

                elif text.startswith("/instagram"):
                    # Halbautomatisch: /instagram <link> → postet in Discord
                    teile = msg_obj.get("text", "").strip().split(" ", 1)
                    if len(teile) < 2 or not teile[1].startswith("http"):
                        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                      json={"chat_id": chat_id,
                                            "text": "⚠️ Benutzung: /instagram https://www.instagram.com/p/xyz",
                                            "parse_mode": "HTML"}, timeout=10)
                    else:
                        ig_link = teile[1].strip()
                        # Discord Embed senden
                        embed = {
                            "title": "📸 Neuer Instagram Post!",
                            "description": f"Schau dir unseren neuesten Post an 👇",
                            "url": ig_link,
                            "color": 0xE1306C,
                            "fields": [
                                {"name": "🔗 Zum Post", "value": ig_link, "inline": False},
                                {"name": "👤 Account", "value": "@bettingxlabs", "inline": True},
                                {"name": "📅 Gepostet", "value": f"{heute()} {jetzt()} Uhr", "inline": True},
                            ],
                            "footer": {"text": "BettingXLabs • Instagram → Discord"}
                        }
                        # An alle Discord Webhooks senden
                        for wh in ["https://discord.com/api/webhooks/1501883766883225621/JjYCVLEInIWfSiTARlJfkoxvEig59ac06822r5ijbiSy9fDcqJgtWE2PaGt0zd9CE6rv"]:
                            send_discord_embed(wh, embed)
                        # Bestätigung an Telegram
                        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                      json={"chat_id": chat_id,
                                            "text": f"✅ Instagram Post wurde in Discord gepostet!\n🔗 {ig_link}",
                                            "parse_mode": "HTML"}, timeout=10)
                        print(f"  [Instagram] Post in Discord geteilt: {ig_link}")

        except Exception as e:
            print(f"  [Telegram-Befehle] Fehler: {e}")
        time.sleep(2)



def ls_get_fixtures(date_str: str) -> list:
    """Holt Fixtures für ein bestimmtes Datum von der LiveScore API."""
    try:
        params  = {**LS_AUTH, "date": date_str}
        resp    = api_get_with_retry(f"{LS_BASE}/fixtures/matches.json", params)
        matches = resp.json().get("data", {}).get("fixtures", []) or []
        print(f"  [PreMatch] {len(matches)} Fixtures für {date_str} geladen")
        return matches
    except Exception as e:
        print(f"  [PreMatch] Fixtures Fehler: {e}")
        return []

def filtere_top_spiele(fixtures: list) -> list:
    """Filtert nur Spiele aus Top-Ligen."""
    top = []
    for f in fixtures:
        liga = f.get("competition", {}).get("name", "").lower()
        if any(l in liga for l in PREMATCH_LIGEN):
            top.append(f)
    return top

def claude_prematch_analyse(home: str, away: str, liga: str, anstoß: str) -> dict | None:
    """
    Fragt Claude nach Pre-Match Analyse.
    Gibt {"tipp": str, "analyse": str} zurück.
    """
    if not ANTHROPIC_API_KEY or ANTHROPIC_API_KEY.startswith("ANTHROPIC"):
        return {"tipp": "Beide Teams treffen", "analyse": "Starke Offensive auf beiden Seiten."}
    try:
        prompt = (
            f"Du bist ein erfahrener Sportwetten-Analyst. Analysiere dieses Spiel auf Deutsch:\n\n"
            f"Spiel: {home} vs {away}\n"
            f"Liga: {liga}\n"
            f"Anstoß: {anstoß} Uhr\n\n"
            f"Wähle NUR EINEN dieser Tipp-Typen:\n"
            f"- Über 2.5 Tore\n"
            f"- Unter 2.5 Tore\n"
            f"- Beide Teams treffen\n"
            f"- Heimsieg\n"
            f"- Auswärtssieg\n"
            f"- Doppelte Chance 1X\n"
            f"- Doppelte Chance X2\n"
            f"- Über 1.5 Tore\n\n"
            f"Antworte NUR in diesem Format:\n"
            f"TIPP: [gewählter Tipp-Typ]\n"
            f"ANALYSE: [max. 2 Sätze Begründung]"
        )
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "Content-Type": "application/json",
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
            },
            json={
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 150,
                "messages": [{"role": "user", "content": prompt}]
            },
            timeout=20
        )
        if resp.status_code != 200:
            return None
        text         = resp.json().get("content", [{}])[0].get("text", "").strip()
        tipp         = ""
        analyse_text = ""
        for line in text.split("\n"):
            if line.startswith("TIPP:"):
                tipp = line.replace("TIPP:", "").strip()
            elif line.startswith("ANALYSE:"):
                analyse_text = line.replace("ANALYSE:", "").strip()
        if not tipp:
            return None
        return {"tipp": tipp, "analyse": analyse_text}
    except Exception as e:
        print(f"  [PreMatch] Claude Fehler: {e}")
        return None

def bot_prematch():
    """Sendet automatisch Pre-Match Tipps um 10:00, 16:00 und 20:00 Uhr."""
    import random
    print(f"[PreMatch-Bot] Gestartet | Posts um {PREMATCH_UHRZEITEN} Uhr")
    gesendet = set()  # "YYYY-MM-DD_HH" → bereits gesendet

    while True:
        try:
            now   = de_now()
            key   = f"{now.strftime('%Y-%m-%d')}_{now.hour}"
            datum = now.strftime("%Y-%m-%d")

            if now.hour in PREMATCH_UHRZEITEN and now.minute < 5 and key not in gesendet:
                print(f"  [PreMatch-Bot] Starte Post um {now.hour}:00 Uhr")

                # Fixtures laden und filtern
                fixtures = ls_get_fixtures(datum)
                top      = filtere_top_spiele(fixtures)

                if not top:
                    print(f"  [PreMatch-Bot] Keine Top-Liga Spiele heute")
                    gesendet.add(key)
                    time.sleep(60)
                    continue

                # Zufällig auswählen
                auswahl   = random.sample(top, min(PREMATCH_MAX_TIPPS, len(top)))
                analysen  = []

                for spiel in auswahl:
                    home     = (spiel.get("home_name")
                                or spiel.get("home", {}).get("name", "?"))
                    away     = (spiel.get("away_name")
                                or spiel.get("away", {}).get("name", "?"))
                    liga     = spiel.get("competition", {}).get("name", "?")
                    country  = (spiel.get("country") or {}).get("name", "")
                    anstoß   = spiel.get("time", "?")

                    result = claude_prematch_analyse(home, away, liga, anstoß)
                    if not result:
                        continue
                    analysen.append({
                        "home": home, "away": away,
                        "liga": liga, "country": country,
                        "anstoß": anstoß,
                        "tipp": result["tipp"],
                        "analyse": result["analyse"],
                    })
                    time.sleep(1)  # kurze Pause zwischen Claude-Calls

                if analysen:
                    uhr_emoji = "🌅" if now.hour == 10 else ("🌆" if now.hour == 16 else "🌙")
                    msg = (f"{uhr_emoji} <b>Pre-Match Tipps – {now.strftime('%d.%m.%Y')}</b>\n"
                           f"━━━━━━━━━━━━━━━━━━━━\n"
                           f"🤖 KI-Analyse powered by BetlabLIVE\n"
                           f"━━━━━━━━━━━━━━━━━━━━\n\n")

                    for i, a in enumerate(analysen, 1):
                        liga_str = f"{a['liga']}" + (f" ({a['country']})" if a['country'] else "")
                        msg += (f"🏆 <b>{liga_str}</b>\n"
                                f"⚽ <b>{a['home']} vs {a['away']}</b>\n"
                                f"🕐 Anstoß: <b>{a['anstoß']} Uhr</b>\n"
                                f"🎯 Tipp: <b>{a['tipp']}</b>\n"
                                f"📊 {a['analyse']}\n")
                        if i < len(analysen):
                            msg += "\n━━━━━━━━━━━━━━━━━━━━\n\n"

                    msg += (f"\n━━━━━━━━━━━━━━━━━━━━\n"
                            f"💬 Community & Live-Bots: discord.gg/G6dt3Kpf\n"
                            f"⚠️ 18+ | Verantwortungsvoll spielen")

                    send_telegram_gruppe(msg)
                    print(f"  [PreMatch-Bot] ✅ {len(analysen)} Tipps um {now.hour}:00 gesendet")

                gesendet.add(key)
                # Nur heutige Keys behalten
                heute_str = now.strftime('%Y-%m-%d')
                gesendet  = {k for k in gesendet if k.startswith(heute_str)}

        except Exception as e:
            print(f"  [PreMatch-Bot] Fehler: {e}")
        time.sleep(60)

# ============================================================
#  PRE-MATCH ERINNERUNG (15 Min vor Anstoß)
# ============================================================

def bot_prematch_erinnerung():
    """
    Prüft alle gesendeten Pre-Match Tipps und erinnert
    15 Minuten vor Anstoß nochmal in Telegram.
    """
    print("[Erinnerungs-Bot] Gestartet | Prüft 15-Min Erinnerungen")
    erinnert = set()  # match_id → bereits erinnert

    while True:
        try:
            now     = de_now()
            datum   = now.strftime("%Y-%m-%d")
            fixtures = ls_get_fixtures(datum)

            for spiel in fixtures:
                match_id = str(spiel.get("id", ""))
                if not match_id or match_id in erinnert:
                    continue
                home    = spiel.get("home_name", spiel.get("home", {}).get("name", "?"))
                away    = spiel.get("away_name", spiel.get("away", {}).get("name", "?"))
                liga    = spiel.get("competition", {}).get("name", "?")
                anstoß  = spiel.get("time", "")
                if not anstoß or ":" not in anstoß:
                    continue
                try:
                    h, m    = map(int, anstoß.split(":"))
                    kickoff = now.replace(hour=h, minute=m, second=0, microsecond=0)
                    diff    = (kickoff - now).total_seconds() / 60
                    # Erinnerung 15-18 Minuten vor Anstoß
                    if 13 <= diff <= 18:
                        liga_lower = liga.lower()
                        if any(l in liga_lower for l in PREMATCH_LIGEN):
                            msg = (f"⏰ <b>Anstoß in ~15 Minuten!</b>\n"
                                   f"━━━━━━━━━━━━━━━━━━━━\n"
                                   f"🏆 {liga}\n"
                                   f"⚽ <b>{home} vs {away}</b>\n"
                                   f"🕐 Anstoß: <b>{anstoß} Uhr</b>\n"
                                   f"━━━━━━━━━━━━━━━━━━━━\n"
                                   f"💬 discord.gg/G6dt3Kpf")
                            send_telegram_gruppe(msg)
                            erinnert.add(match_id)
                            print(f"  [Erinnerung] {home} vs {away} in ~{diff:.0f} Min")
                except:
                    continue
        except Exception as e:
            print(f"  [Erinnerungs-Bot] Fehler: {e}")
        time.sleep(60)

# ============================================================
#  WATCHDOG
# ============================================================

_bot_targets = {}  # thread_name → target_function (wird beim Start befüllt)

def bot_watchdog():
    """Überwacht alle Bot-Threads und startet sie neu falls sie abstürzen."""
    print("[Watchdog] Gestartet")
    time.sleep(30)  # Erst warten bis alle Bots hochgefahren sind
    while True:
        try:
            aktive = {t.name: t for t in threading.enumerate()}
            for name, target in _bot_targets.items():
                if name not in aktive or not aktive[name].is_alive():
                    print(f"  [Watchdog] ⚠️ {name} ist tot – starte neu!")
                    msg = (f"⚠️ <b>Watchdog Alert!</b>\n"
                           f"Bot <b>{name}</b> ist abgestürzt.\n"
                           f"Starte automatisch neu...\n"
                           f"🕐 {jetzt()} Uhr")
                    send_telegram(msg)
                    t = threading.Thread(target=target, daemon=True, name=name)
                    t.start()
                    print(f"  [Watchdog] ✅ {name} neu gestartet")
        except Exception as e:
            print(f"  [Watchdog] Fehler: {e}")
        time.sleep(60)

# ============================================================
#  START
# ============================================================

if __name__ == "__main__":
    print("=" * 50)
    print("  ⚽ FUSSBALL BOTS v27")
    print("  Telegram Befehle · Bankroll · Multi-Signal · API-Monitor · Persistenz · Comeback+")
    print("=" * 50 + "\n")

    statistik_laden()
    beobachtete_spiele_laden()

    bot_definitionen = [
        ("Ecken-Bot",        bot_ecken),
        ("Ecken-Über-Bot",   bot_ecken_over),
        ("Karten-Bot",       bot_karten),
        ("Torwart-Bot",      bot_torwart),
        ("Druck-Bot",        bot_druck),
        ("Comeback-Bot",     bot_comeback),
        ("Torflut-Bot",      bot_torflut),
        ("Rotkarte-Bot",     bot_rotkarte),
        ("Tore-Bot",         bot_tore_analyse),
        ("PreMatch-Bot",     bot_prematch),
        ("Telegram-Bot",     bot_telegram_befehle),
        ("Erinnerungs-Bot",  bot_prematch_erinnerung),
        ("Auswertung-Bot",   bot_auswertung_und_berichte),
    ]

    # Targets für Watchdog merken
    for name, target in bot_definitionen:
        _bot_targets[name] = target

    threads = []
    for name, target in bot_definitionen:
        t = threading.Thread(target=target, daemon=True, name=name)
        threads.append(t)
        t.start()
        time.sleep(2)

    # Watchdog starten
    watchdog = threading.Thread(target=bot_watchdog, daemon=True, name="Watchdog")
    watchdog.start()

    print("Alle Bots laufen!\n")
    while True:
        time.sleep(60)
