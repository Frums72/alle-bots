# v52 - KRITISCHE Auswertungs-Fixes + Discord Vote-Rang-System
import os
import requests
import re
import time
import threading
from datetime import datetime, timezone, timedelta

# ============================================================
#  KONFIGURATION
# ============================================================
API_KEY            = os.environ.get("LS_API_KEY",     "OHvYYqv2LTNBi8qU")
API_SECRET         = os.environ.get("LS_API_SECRET",  "G8lerfJK8OJ8TqMH7iG6Jb8u4V6n3wiK")

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_TOKEN", "8706066107:AAHHVuC-M_gz-sr5sbm-7zkb7QGygfoRHoM")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "7272001004")

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

ODDS_API_KEY       = os.environ.get("ODDS_API_KEY", "866948de5d6c34ca51faf6bd77e0bb2a")
PANDASCORE_API_KEY = os.environ.get("PANDASCORE_KEY", "qeVqpzDJKy5rt6Ky7vmWCdqEkkUt9j53togKOK9gzeIvGNCEbMg")
FOOTBALLDATA_KEY   = os.environ.get("FOOTBALLDATA_KEY", "e17f09662062462b850a73f857e1f974")

# Value Bet Finder
VALUE_BET_MIN_QUOTE   = 1.6   # Mindestquote damit ein Value Bet interessant ist
VALUE_BET_MIN_VALUE   = 0.15  # Mindest-Edge (15%) damit Signal gesendet wird
DISCORD_WEBHOOK_VALUE = "https://discord.com/api/webhooks/1501252266630316163/aBo4o0HDN_Fh3eVj-WEvRZlzo970OQJcO1g6vKk4gJJ6hfRxco98m0p5KXDEQ-NBEZr1"  # #betlab-value-bets
DISCORD_WEBHOOK_CS2   = "https://discord.com/api/webhooks/1501252266630316163/aBo4o0HDN_Fh3eVj-WEvRZlzo970OQJcO1g6vKk4gJJ6hfRxco98m0p5KXDEQ-NBEZr1"  # #betlab-cs2
ANTHROPIC_API_KEY  = os.environ.get("ANTHROPIC_KEY", "")
CLAUDE_MAX_PRO_TAG = 3    # Maximal 3 Claude-Calls pro Tag

# ── Telegram Signal-Filter ───────────────────────────────────
# Bots die für Telegram deaktiviert sind (Discord läuft weiter)
TELEGRAM_FILTER_DATEI = "telegram_filter.json"
_telegram_deaktiviert = set()  # Bot-Namen die NICHT per Telegram gesendet werden

TELEGRAM_BOT_NAMEN = {
    "cs2":         "🎮 CS2-Bot",
    "ecken":       "📐 Ecken-Unter",
    "ecken_over":  "📐 Ecken-Über",
    "karten":      "🃏 Karten-Bot",
    "torwart":     "🧤 Torwart-Bot",
    "druck":       "🔥 Druck-Bot",
    "comeback":    "🔄 Comeback-Bot",
    "torflut":     "🌊 Torflut-Bot",
    "rotkarte":    "🟥 Rotkarte-Bot",
    "value":       "💎 Value-Bot",
    "arbitrage":   "💰 Arbitrage-Bot",
    "xg":          "📊 xG-Bot",
    "earlygoal":   "⚡ EarlyGoal-Bot",
    "hz2tore":     "🥅 HZ2-Tore-Bot",
    "cornerrush":  "📐 CornerRush-Bot",
    "sharp":       "💼 Sharp-Money-Bot",
    "anomalie":    "🚨 Anomalie-Bot",
    "prematch":    "📅 PreMatch-Bot",
    "tagesoverview": "🌅 Morgen-Übersicht",
}

def telegram_filter_laden():
    import json, os
    global _telegram_deaktiviert
    if not os.path.exists(TELEGRAM_FILTER_DATEI):
        return
    try:
        with open(TELEGRAM_FILTER_DATEI) as f:
            _telegram_deaktiviert = set(json.load(f))
        if _telegram_deaktiviert:
            print(f"  [TG-Filter] Deaktiviert: {', '.join(_telegram_deaktiviert)}")
    except Exception as e:
        print(f"  [TG-Filter] Ladefehler: {e}")

def telegram_filter_speichern():
    import json
    try:
        with open(TELEGRAM_FILTER_DATEI, "w") as f:
            json.dump(list(_telegram_deaktiviert), f)
    except Exception as e:
        print(f"  [TG-Filter] Speicherfehler: {e}")

def telegram_signal_erlaubt(bot_key: str) -> bool:
    """Gibt False zurück wenn dieser Bot-Typ für Telegram deaktiviert ist."""
    return bot_key not in _telegram_deaktiviert

# ── Claude Tages-Budget ──────────────────────────────────────
_claude_calls_heute   = 0
_claude_calls_datum   = ""

TOP_LIGEN_CLAUDE = {
    "Premier League", "Bundesliga", "La Liga", "Serie A",
    "Ligue 1", "Champions League", "Europa League",
    "Eredivisie", "Primeira Liga", "World Cup",
    "European Championship", "DFB-Pokal", "FA Cup",
    "Copa del Rey", "Coppa Italia",
}

def claude_budget_verfuegbar(liga: str = "") -> bool:
    """
    Gibt True zurück wenn:
    - Noch nicht 3 Claude-Calls heute verbraucht
    - Liga ist eine Top-Liga (falls angegeben)
    """
    global _claude_calls_heute, _claude_calls_datum
    heute = de_now().strftime("%Y-%m-%d")
    if _claude_calls_datum != heute:
        _claude_calls_heute  = 0
        _claude_calls_datum  = heute
    if _claude_calls_heute >= CLAUDE_MAX_PRO_TAG:
        print(f"  [Claude] Tages-Limit erreicht ({CLAUDE_MAX_PRO_TAG}/Tag) – Fallback")
        return False
    if liga and liga not in TOP_LIGEN_CLAUDE:
        print(f"  [Claude] {liga} ist keine Top-Liga – Fallback")
        return False
    return True

def claude_budget_erhoehen():
    """Zählt einen Claude-Call."""
    global _claude_calls_heute
    _claude_calls_heute += 1
    print(f"  [Claude] Call {_claude_calls_heute}/{CLAUDE_MAX_PRO_TAG} heute")
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
FUSSBALL_INTERVAL    = 2  # Reduziert von 3 Min für schnellere Signale
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

# ── GitHub Backup ────────────────────────────────────────────
GITHUB_TOKEN         = os.environ.get("GITHUB_TOKEN", "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")
GITHUB_REPO          = "Frums72/alle-bots"
GITHUB_BACKUP_UHRZEIT = 2  # Uhrzeit für tägliches Backup (02:00)

# ── Auswertungs-Fallback ─────────────────────────────────────
MAX_BEOBACHTUNG_STUNDEN = 3   # Nach X Stunden ohne FT → aus Beobachtung entfernen

# ── Signal Tracker (neues robustes System) ───────────────────
SIGNAL_TRACKER_DATEI = "signal_tracker.json"
# Status: "offen" → noch nicht ausgewertet, "ausgewertet" → fertig

# ── Tipp-Kombinationen ───────────────────────────────────────
KOMBI_SIGNAL_TYPEN   = {
    frozenset(["hz1tore", "torflut"]): "Torreiches Spiel",
    frozenset(["hz1tore", "vztore"]):  "Tore-Dominanz",
    frozenset(["druck", "comeback"]):  "Spannendes Duell",
    frozenset(["torwart", "druck"]):   "Druck + Chancen",
}
kombi_gesendet = set()  # match_id → bereits kombisignal gesendet
# ============================================================

LS_BASE = "https://livescore-api.com/api-client"
LS_AUTH = {"key": API_KEY, "secret": API_SECRET}

KARTEN_TYPEN   = {"Yellow Card", "Red Card", "Yellow Red Card"}
ROTKARTE_TYPEN = {"Red Card", "Yellow Red Card"}

# Shared Cache
_cache_matches   = []
_cache_timestamp = 0
_cache_lock      = threading.Lock()
CACHE_TTL        = 20  # Reduziert von 45s für schnellere Signale

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
NOTIFIED_DATEI      = "notified_sets.json"

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

def notified_sets_speichern():
    """Speichert alle notified Sets – verhindert Doppel-Signale nach Neustart."""
    import json
    try:
        data = {
            "ecken": list(notified_ecken), "ecken_over": list(notified_ecken_over),
            "karten": list(notified_karten), "torwart": list(notified_torwart),
            "druck": list(notified_druck), "comeback": list(notified_comeback),
            "torflut": list(notified_torflut), "rotkarte": list(notified_rotkarte),
            "hz1tore": list(notified_hz1tore), "vztore": list(notified_vztore),
            "auswertung_done": list(auswertung_done),
            "gespeichert": de_now().strftime("%Y-%m-%d %H:%M"),
        }
        with open(NOTIFIED_DATEI, "w") as f:
            json.dump(data, f)
    except Exception as e:
        print(f"  [Notified] Speicherfehler: {e}")

def notified_sets_laden():
    """Lädt notified Sets beim Start – kein Doppel-Signal nach Neustart."""
    import json, os
    global notified_ecken, notified_ecken_over, notified_karten, notified_torwart
    global notified_druck, notified_comeback, notified_torflut, notified_rotkarte
    global notified_hz1tore, notified_vztore, auswertung_done
    if not os.path.exists(NOTIFIED_DATEI):
        return
    try:
        with open(NOTIFIED_DATEI) as f:
            data = json.load(f)
        # Nur heutige Sets laden
        if data.get("gespeichert", "")[:10] != de_now().strftime("%Y-%m-%d"):
            print(f"  [Notified] Sets von gestern – nicht geladen")
            return
        notified_ecken      = set(data.get("ecken", []))
        notified_ecken_over = set(data.get("ecken_over", []))
        notified_karten     = set(data.get("karten", []))
        notified_torwart    = set(data.get("torwart", []))
        notified_druck      = set(data.get("druck", []))
        notified_comeback   = set(data.get("comeback", []))
        notified_torflut    = set(data.get("torflut", []))
        notified_rotkarte   = set(data.get("rotkarte", []))
        notified_hz1tore    = set(data.get("hz1tore", []))
        notified_vztore     = set(data.get("vztore", []))
        auswertung_done     = set(data.get("auswertung_done", []))
        total = sum(len(s) for s in [notified_ecken, notified_ecken_over, notified_karten,
            notified_torwart, notified_druck, notified_comeback, notified_torflut,
            notified_rotkarte, notified_hz1tore, notified_vztore])
        print(f"  [Notified] {total} Match-IDs geladen – kein Doppel-Signal nach Neustart ✅")
    except Exception as e:
        print(f"  [Notified] Ladefehler: {e}")

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
    except Exception:
        return 0, 0

def html_zu_discord(text):
    text = text.replace("<b>", "**").replace("</b>", "**")
    text = re.sub(r"<[^>]+>", "", text)
    return text

_signal_stunde_zaehler = {}

def signal_spam_check() -> bool:
    """Gibt False zurück wenn mehr als 8 Signale pro Stunde gesendet wurden."""
    stunde = de_now().strftime("%Y-%m-%d-%H")
    _signal_stunde_zaehler[stunde] = _signal_stunde_zaehler.get(stunde, 0) + 1
    for k in list(_signal_stunde_zaehler.keys()):
        if k != stunde:
            del _signal_stunde_zaehler[k]
    if _signal_stunde_zaehler[stunde] > 8:
        print(f"  [Spam] Signal unterdrückt – {_signal_stunde_zaehler[stunde]}/8 diese Stunde")
        return False
    return True

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
    except Exception:
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
    except Exception:
        return {"wind": 0, "regen": 0, "ts": now}

def schlechtes_wetter(country: str) -> bool:
    """Gibt True zurück wenn das Wetter die Spielbedingungen beeinflusst."""
    w = get_wetter(country)
    return w["wind"] >= WETTER_WIND_GRENZE or w["regen"] >= WETTER_REGEN_GRENZE

def wetter_analyse(country: str) -> dict:
    """
    Analysiert Wetter und gibt Tipp-Empfehlungen zurück.
    Starkregen → weniger Ecken, weniger Tore, mehr Zweikämpfe
    Sturm      → Flanken schwieriger, weniger Ecken
    Hitze      → Tempo sinkt in 2. HZ, weniger Tore ab Min 60
    """
    w = get_wetter(country)
    wind  = w.get("wind", 0)
    regen = w.get("regen", 0)
    tipps = []
    info  = []
    if regen >= 5:
        tipps.extend(["unter_ecken", "unter_tore"])
        info.append(f"🌧️ Starkregen ({regen}mm) → schwieriger Ball, weniger Ecken/Tore")
    elif regen >= 2:
        tipps.append("unter_ecken")
        info.append(f"🌦️ Regen ({regen}mm) → leicht reduzierte Ecken-Anzahl")
    if wind >= 40:
        tipps.extend(["unter_ecken", "mehr_karten"])
        info.append(f"💨 Sturm ({wind}km/h) → Flanken kaum möglich, Zweikämpfe härter")
    elif wind >= 30:
        tipps.append("unter_ecken")
        info.append(f"🌬️ Wind ({wind}km/h) → Flankenspiel erschwert")
    return {"tipps": tipps, "info": info, "wind": wind, "regen": regen,
            "schlecht": len(tipps) > 0}

def bot_wetter_tipp():
    """
    Sendet Wetter-basierte Wett-Tipps vor Top-Liga Spielen.
    Z.B. bei Sturm in England → Unter Ecken Premier League heute
    """
    print("[Wetter-Bot] Gestartet | Wetterbasierte Tipps")
    gesendet_wetter = set()
    while True:
        try:
            now   = de_now()
            datum = now.strftime("%Y-%m-%d")
            fixtures = ls_get_fixtures(datum)
            top = filtere_top_spiele(fixtures)
            laender = {}
            for f in top:
                land = (f.get("country") or {}).get("name", "")
                liga = f.get("competition", {}).get("name", "?")
                if land and land not in laender:
                    laender[land] = liga
            for land, liga in laender.items():
                key = f"{datum}_{land}"
                if key in gesendet_wetter:
                    continue
                analyse = wetter_analyse(land)
                if not analyse["schlecht"]:
                    continue
                gesendet_wetter.add(key)
                tipps_text = []
                for tipp in analyse["tipps"]:
                    if tipp == "unter_ecken":
                        tipps_text.append("📐 Unter Ecken")
                    elif tipp == "unter_tore":
                        tipps_text.append("⚽ Unter Tore")
                    elif tipp == "mehr_karten":
                        tipps_text.append("🃏 Über Karten")
                if not tipps_text:
                    continue
                info_text = "\n".join(analyse["info"])
                msg = (f"🌦️ <b>Wetter-Tipp!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                       f"🌍 Land: <b>{land}</b>\n"
                       f"🏆 Liga: {liga}\n"
                       f"━━━━━━━━━━━━━━━━━━━━\n"
                       f"{info_text}\n"
                       f"━━━━━━━━━━━━━━━━━━━━\n"
                       f"🎯 Wetter-Tipps für heute:\n"
                       f"{'  '.join(tipps_text)}\n"
                       f"━━━━━━━━━━━━━━━━━━━━\n🕐 {jetzt()} Uhr")
                send_telegram(msg)
                embed = {
                    "title": f"🌦️ Wetter-Tipp – {land}",
                    "color": 0x3498DB,
                    "fields": [
                        {"name": "🌍 Land",       "value": land,                    "inline": True},
                        {"name": "🏆 Liga",       "value": liga,                    "inline": True},
                        {"name": "💨 Wind",       "value": f"{analyse['wind']} km/h","inline": True},
                        {"name": "🌧️ Regen",     "value": f"{analyse['regen']} mm", "inline": True},
                        {"name": "ℹ️ Analyse",    "value": info_text,               "inline": False},
                        {"name": "🎯 Tipps",      "value": "  ".join(tipps_text),   "inline": False},
                    ],
                    "footer": {"text": f"Wetter-Bot • {heute()} {jetzt()}"},
                }
                send_discord_embed(DISCORD_WEBHOOK_DRUCK, embed)
                print(f"  [Wetter-Bot] ✅ {land}: {', '.join(tipps_text)}")
            bot_fehler_reset("Wetter-Bot")
        except Exception as e:
            bot_fehler_melden("Wetter-Bot", e)
        time.sleep(30 * 60)  # Alle 30 Minuten

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
            except Exception:
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
    except Exception:
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
    except Exception:
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
    except Exception:
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
    except Exception:
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
    except Exception:
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
TYP_NAMEN = {
    "ecken": "Ecken Unter", "ecken_over": "Ecken Über",
    "karten": "Karten Über 5", "torwart": "Mind. 1 Tor",
    "druck": "Druck/Ecken Dominanz", "comeback": "Beide Teams treffen",
    "torflut": "Torreich", "rotkarte": "Überzahl Tor",
    "hz1tore": "HZ1 Tore", "vztore": "Vollzeit Tore",
    "xg": "xG Signal", "value": "Value Bet",
}

def claude_tipp_review(home: str, away: str, typ: str, analyse: str,
                        liga: str = "") -> tuple:
    """
    Fragt Claude ob der Tipp Sinn ergibt.
    Nur bei Top-Ligen und max. 3x pro Tag.
    """
    if not ANTHROPIC_API_KEY or not ANTHROPIC_API_KEY.strip():
        return True, ""
    if not claude_budget_verfuegbar(liga):
        return True, ""
    try:
        typ_name = TYP_NAMEN.get(typ, typ)
        prompt = (
            f"Du bist ein erfahrener Sportwetten-Analyst. Analysiere auf Deutsch:\n\n"
            f"Spiel: {home} vs {away}\n"
            f"Tipp: {typ_name}\n"
            f"Live-Daten:\n{analyse}\n\n"
            f"Antworte NUR in diesem Format:\n"
            f"EMPFOHLEN: [2-3 präzise Sätze warum dieser Tipp Sinn ergibt – spezifisch auf die Daten bezogen]\n"
            f"oder\n"
            f"SKEPTISCH: [2-3 Sätze warum du skeptisch bist]"
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
                "max_tokens": 200,
                "messages": [{"role": "user", "content": prompt}]
            },
            timeout=15
        )
        if resp.status_code != 200:
            return True, ""
        text        = resp.json().get("content", [{}])[0].get("text", "").strip()
        empfohlen   = text.startswith("EMPFOHLEN")
        begruendung = text.replace("EMPFOHLEN:", "").replace("SKEPTISCH:", "").strip()
        claude_budget_erhoehen()
        return empfohlen, begruendung
    except Exception as e:
        print(f"  [Claude] Review Fehler: {e}")
        return True, ""

def claude_live_begruendung(home: str, away: str, typ: str,
                             stats: dict, score: str, minute: int) -> str:
    """
    Schreibt für JEDEN Bot-Typ eine spezifische Begründung basierend auf Live-Daten.
    Wird auch ohne ANTHROPIC_API_KEY mit Fallback-Texten verwendet.
    """
    typ_name = TYP_NAMEN.get(typ, typ)
    # Fallback ohne API – spezifische Texte je nach Typ
    fallbacks = {
        "druck":    f"{home} dominiert mit {stats.get('corners_home',0)} Ecken klar und erzeugt konstanten Druck auf das Tor.",
        "comeback": f"Trotz Rückstand dominiert das Team die Statistiken – Ausgleich ist wahrscheinlich.",
        "torwart":  f"Trotz {stats.get('shots_on_target_home',0)+stats.get('shots_on_target_away',0)} Schüssen aufs Tor ist das Spiel noch torlos – das erste Tor ist überfällig.",
        "torflut":  f"Bereits {score} nach der Halbzeit – beide Teams spielen offensiv und ein weiteres Tor ist sehr wahrscheinlich.",
        "rotkarte": f"Die numerische Überzahl verschafft dem Team einen klaren Vorteil für die verbleibenden Minuten.",
        "ecken":    f"Nur {stats.get('corners_home',0)+stats.get('corners_away',0)} Ecken in der ersten Hälfte – ein ruhiges Spiel deutet auf wenige weitere Ecken hin.",
        "karten":   f"Bereits {stats.get('corners_home',0)} Karten in Minute {minute} – der Schiedsrichter greift früh ein.",
    }
    if not ANTHROPIC_API_KEY or not ANTHROPIC_API_KEY.strip():
        return fallbacks.get(typ, "")
    if not claude_budget_verfuegbar(home):  # home als Liga-Proxy ignoriert → immer Fallback für Live
        return fallbacks.get(typ, "")
    try:
        stat_text = (f"Schüsse: {stats.get('shots_on_target_home',0)}|{stats.get('shots_on_target_away',0)} | "
                     f"Ecken: {stats.get('corners_home',0)}|{stats.get('corners_away',0)} | "
                     f"Ballbesitz: {stats.get('possession_home','?')}%|{stats.get('possession_away','?')}%")
        prompt = (f"Sportwetten-Analyst. 2 präzise Sätze auf Deutsch warum '{typ_name}' bei {home} vs {away} "
                  f"(Stand {score}, Min. {minute}) Sinn ergibt. Daten: {stat_text}. Nur die Begründung, keine Einleitung.")
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"Content-Type": "application/json",
                     "x-api-key": ANTHROPIC_API_KEY,
                     "anthropic-version": "2023-06-01"},
            json={"model": "claude-sonnet-4-20250514", "max_tokens": 150,
                  "messages": [{"role": "user", "content": prompt}]},
            timeout=10
        )
        if resp.status_code == 200:
            return resp.json().get("content", [{}])[0].get("text", "").strip()
        return fallbacks.get(typ, "")
    except Exception:
        return fallbacks.get(typ, "")

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
    except Exception:
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
    except Exception:
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
    check_rate_limit_warnung() if _api_monitor.get("heute", 0) % 5000 == 0 else None
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

# ── Auswertungs-Fallback ─────────────────────────────────────
def auswertung_fallback_check():
    """Entfernt Spiele aus beobachtete_spiele die zu lange keine Auswertung hatten."""
    jetzt_ts = time.time()
    zu_entfernen = []
    for match_id, spiel in list(beobachtete_spiele.items()):
        if match_id in auswertung_done:
            zu_entfernen.append(match_id)
            continue
        signal_zeit = spiel.get("signal_zeit", jetzt_ts)
        stunden = (jetzt_ts - signal_zeit) / 3600
        if stunden > MAX_BEOBACHTUNG_STUNDEN:
            zu_entfernen.append(match_id)
            print(f"  [Fallback] {spiel.get('home','?')} vs {spiel.get('away','?')} "
                  f"nach {stunden:.1f}h entfernt ({spiel.get('typ','')})")
    for mid in zu_entfernen:
        beobachtete_spiele.pop(mid, None)
    if zu_entfernen:
        beobachtete_spiele_speichern()
        print(f"  [Fallback] {len(zu_entfernen)} Spiele aus Beobachtung entfernt")

# ── Doppelt-Signal Schutz (verbessert) ───────────────────────
beobachtete_spiele_multi = {}  # match_id → {typ: spiel_data} – mehrere Tipps pro Spiel

def beobachtung_hinzufuegen(match_id: str, spiel: dict):
    """Fügt Spiel zur Beobachtung hinzu – mehrere Typen pro Spiel möglich."""
    typ = spiel.get("typ", "unbekannt")
    if match_id not in beobachtete_spiele_multi:
        beobachtete_spiele_multi[match_id] = {}
    beobachtete_spiele_multi[match_id][typ] = spiel
    # Hauptdict für Kompatibilität weiterhin befüllen
    beobachtete_spiele[match_id] = spiel
    beobachtete_spiele_speichern()
    # Im Signal-Tracker registrieren für robuste Auswertung
    tracker_signal_hinzufuegen(match_id, spiel)
    # Notified Sets persistieren
    notified_sets_speichern()
    # Kombi-Signal prüfen
    kombi_signal_check(match_id)

def kombi_signal_check(match_id: str):
    """Prüft ob mehrere Bots auf dasselbe Spiel getippt haben → Kombi-Signal."""
    if match_id in kombi_gesendet:
        return
    typen = set(beobachtete_spiele_multi.get(match_id, {}).keys())
    if len(typen) < 2:
        return
    for kombi_typen, kombi_name in KOMBI_SIGNAL_TYPEN.items():
        if kombi_typen.issubset(typen):
            spiel     = beobachtete_spiele[match_id]
            home      = spiel.get("home", "?")
            away      = spiel.get("away", "?")
            alle_tipps = []
            for t in kombi_typen:
                s = beobachtete_spiele_multi[match_id].get(t, {})
                if s.get("richtung") and s.get("linie"):
                    alle_tipps.append(f"{t.upper()}: {s['richtung']} {s['linie']}")
                elif s.get("typ"):
                    alle_tipps.append(t.upper())
            msg = (f"⚡ <b>KOMBI-SIGNAL! – {kombi_name}</b>\n"
                   f"━━━━━━━━━━━━━━━━━━━━\n"
                   f"📌 <b>{home} vs {away}</b>\n"
                   f"🤖 Mehrere Bots tippen auf dieses Spiel:\n"
                   f"{'  '.join([f'✅ {t}' for t in alle_tipps])}\n"
                   f"━━━━━━━━━━━━━━━━━━━━\n"
                   f"🎯 Erhöhtes Vertrauen in dieses Spiel!\n"
                   f"🕐 {jetzt()} Uhr")
            send_telegram(msg)
            kombi_gesendet.add(match_id)
            print(f"  [Kombi-Signal] {home} vs {away}: {kombi_name}")
            break

# ── Over/Under Dynamische Berechnung ─────────────────────────
def berechne_dynamische_grenzen(h2h_avg: float, form_avg: float = None,
                                  typ: str = "vz") -> tuple:
    """
    Berechnet dynamische Über/Unter Grenzen statt fester Schwellenwerte.
    Gewichtet H2H (70%) und aktuelle Form (30%).
    """
    if form_avg is not None:
        gewichteter_avg = round(h2h_avg * 0.7 + form_avg * 0.3, 2)
    else:
        gewichteter_avg = h2h_avg

    if typ == "hz1":
        ueber_grenze = 1.0 + (gewichteter_avg * 0.1)  # dynamisch angepasst
        unter_grenze = 0.5 + (gewichteter_avg * 0.05)
        ueber_grenze = max(0.9, min(1.4, ueber_grenze))
        unter_grenze = max(0.4, min(0.8, unter_grenze))
    else:  # vz
        ueber_grenze = 2.3 + (gewichteter_avg * 0.15)
        unter_grenze = 1.5 + (gewichteter_avg * 0.1)
        ueber_grenze = max(2.2, min(3.0, ueber_grenze))
        unter_grenze = max(1.3, min(2.0, unter_grenze))

    return round(ueber_grenze, 2), round(unter_grenze, 2)

# ── Verletzungs-Check ────────────────────────────────────────
_lineup_cache = {}
LINEUP_TTL    = 1800

def get_team_lineup(match_id: str) -> dict:
    """Holt Aufstellung für ein Spiel (gecacht 30 Min)."""
    now = time.time()
    if match_id in _lineup_cache and now - _lineup_cache[match_id]["ts"] < LINEUP_TTL:
        return _lineup_cache[match_id]["data"]
    try:
        params = {**LS_AUTH, "match_id": match_id}
        resp   = api_get_with_retry(f"{LS_BASE}/matches/lineups.json", params)
        data   = resp.json().get("data", {}) or {}
        _lineup_cache[match_id] = {"data": data, "ts": now}
        return data
    except Exception:
        return {}

def verletzungs_check(match_id: str, home: str, away: str) -> str:
    """
    Prüft ob wichtige Spieler fehlen.
    Gibt Warntext zurück wenn viele Spieler nicht in der Aufstellung sind.
    """
    try:
        lineup = get_team_lineup(match_id)
        if not lineup:
            return ""
        home_lineup = lineup.get("home", {})
        away_lineup = lineup.get("away", {})
        home_count  = len(home_lineup.get("starting_eleven", []) or [])
        away_count  = len(away_lineup.get("starting_eleven", []) or [])
        warnungen   = []
        if home_count > 0 and home_count < 11:
            warnungen.append(f"⚠️ {home}: Nur {home_count}/11 Spieler gelistet")
        if away_count > 0 and away_count < 11:
            warnungen.append(f"⚠️ {away}: Nur {away_count}/11 Spieler gelistet")
        return "\n".join(warnungen)
    except Exception:
        return ""

# ── GitHub Backup ─────────────────────────────────────────────
PERSISTENZ_DATEIEN = [
    "statistik.json", "signal_tracker.json", "beobachtete_spiele.json",
    "notified_sets.json", "bankroll.json", "dynamische_filter.json",
    "whitelist.json", "community_tipps.json", "admins.json",
    "bekannte_user.json", "manuell_tipps.json", "ab_test.json",
]
GITHUB_DATA_PFAD = "data/latest"  # Fester Pfad in Repo → immer überschrieben

def github_backup():
    """Pusht ALLE Datendateien auf GitHub (fester Pfad → wird überschrieben)."""
    import json, base64, os
    if not GITHUB_TOKEN or GITHUB_TOKEN.startswith("GITHUB"):
        print("  [Backup] Kein GitHub Token konfiguriert")
        return
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }
    gesichert = 0
    for datei in PERSISTENZ_DATEIEN:
        if not os.path.exists(datei):
            continue
        try:
            with open(datei, "rb") as f:
                inhalt = base64.b64encode(f.read()).decode()
            pfad    = f"{GITHUB_DATA_PFAD}/{datei}"
            api_url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{pfad}"
            sha     = None
            check   = requests.get(api_url, headers=headers, timeout=10)
            if check.status_code == 200:
                sha = check.json().get("sha")
            payload = {
                "message": f"Data-Backup {de_now().strftime('%Y-%m-%d %H:%M')}",
                "content": inhalt,
            }
            if sha:
                payload["sha"] = sha
            resp = requests.put(api_url, headers=headers,
                                json=payload, timeout=15)
            if resp.status_code in (200, 201):
                gesichert += 1
                print(f"  [Backup] ✅ {datei} → GitHub")
            else:
                print(f"  [Backup] ❌ {datei}: {resp.status_code}")
        except Exception as e:
            print(f"  [Backup] Fehler bei {datei}: {e}")
    return gesichert

def github_restore():
    """
    Lädt beim Start alle Datendateien von GitHub herunter.
    Verhindert Datenverlust nach Railway-Neustart.
    """
    import base64, os
    if not GITHUB_TOKEN or GITHUB_TOKEN.startswith("GITHUB"):
        print("  [Restore] Kein GitHub Token – übersprungen")
        return 0
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }
    wiederhergestellt = 0
    for datei in PERSISTENZ_DATEIEN:
        # Nur herunterladen wenn Datei NICHT lokal existiert (Neustart)
        if os.path.exists(datei):
            print(f"  [Restore] {datei} bereits lokal vorhanden – übersprungen")
            continue
        try:
            pfad    = f"{GITHUB_DATA_PFAD}/{datei}"
            api_url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{pfad}"
            resp    = requests.get(api_url, headers=headers, timeout=10)
            if resp.status_code != 200:
                print(f"  [Restore] {datei}: nicht auf GitHub ({resp.status_code})")
                continue
            inhalt = base64.b64decode(resp.json().get("content", ""))
            with open(datei, "wb") as f:
                f.write(inhalt)
            wiederhergestellt += 1
            print(f"  [Restore] ✅ {datei} ← GitHub")
        except Exception as e:
            print(f"  [Restore] Fehler bei {datei}: {e}")
    if wiederhergestellt > 0:
        print(f"  [Restore] {wiederhergestellt} Dateien wiederhergestellt ✅")
        send_telegram(f"🔄 <b>Daten wiederhergestellt!</b>\n"
                      f"━━━━━━━━━━━━━━━━━━━━\n"
                      f"✅ {wiederhergestellt} Dateien von GitHub geladen\n"
                      f"📊 Statistiken, Bankroll & Signale sind wiederhergestellt\n"
                      f"🕐 {jetzt()} Uhr")
    return wiederhergestellt

def bot_github_backup():
    """Stündlicher GitHub Backup + täglicher vollständiger Backup."""
    print(f"[Backup-Bot] Gestartet | Stündlich + täglich {GITHUB_BACKUP_UHRZEIT}:00 Uhr")
    backup_gesendet   = None
    letzter_std_backup = 0
    while True:
        try:
            now    = de_now()
            now_ts = time.time()
            # Stündlicher Backup (alle 60 Min)
            if now_ts - letzter_std_backup >= 3600:
                letzter_std_backup = now_ts
                github_backup()
                print(f"  [Backup] Stündlicher Backup abgeschlossen")
            # Täglicher Backup mit Telegram-Meldung
            if now.hour == GITHUB_BACKUP_UHRZEIT and backup_gesendet != now.date():
                github_backup()
                backup_gesendet = now.date()
                send_telegram(f"💾 <b>Tages-Backup abgeschlossen</b>\n"
                              f"✅ {len(PERSISTENZ_DATEIEN)} Dateien → GitHub\n"
                              f"🛡️ Daten sind sicher bei Neustart\n"
                              f"🕐 {jetzt()} Uhr")
        except Exception as e:
            print(f"  [Backup-Bot] Fehler: {e}")
        time.sleep(60)

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
            {"name": "🏆 Liga",          "value": f"{comp}",           "inline": True},
            {"name": "🌍 Land",          "value": f"{country}",        "inline": True},
            {"name": "📊 Halbzeitstand", "value": f"**{score}**",      "inline": True},
            {"name": "⚽ Spiel",         "value": f"{home} vs {away}", "inline": False},
            {"name": "📐 Ecken zur Halbzeit",
             "value": f"🔵 {home}: **{corners_home}**\n🔴 {away}: **{corners_away}**\n📊 Gesamt: **{corners}**", "inline": False},
            {"name": "🎯 Empfehlung", "value": f"Unter **{grenze} Ecken** (Gesamtspiel){qt}", "inline": False},
        ],
        "footer": {"text": f"Ecken-Bot • {heute()} {jetzt()}"},
    }

def discord_ecken_over_tipp(home, away, comp, country, score, minute, corners_home, corners_away, corners, quote):
    qt = f"\n💶 **Quote:** {quote}" if quote else ""
    return {
        "title": "📐 Ecken ÜBER Tipp", "color": FARBE_ECKEN_OVER,
        "fields": [
            {"name": "🏆 Liga",      "value": f"{comp}",                        "inline": True},
            {"name": "🌍 Land",      "value": f"{country}",                     "inline": True},
            {"name": "📊 Stand",     "value": f"**{score}** | Min. **{minute}'**", "inline": True},
            {"name": "⚽ Spiel",     "value": f"{home} vs {away}",              "inline": False},
            {"name": "📐 Ecken bisher",
             "value": f"🔵 {home}: **{corners_home}**\n🔴 {away}: **{corners_away}**\n📊 Gesamt: **{corners}**", "inline": False},
            {"name": "🎯 Empfehlung", "value": f"Über **14 Ecken** (Gesamtspiel){qt}", "inline": False},
        ],
        "footer": {"text": f"Ecken-Über-Bot • {heute()} {jetzt()}"},
    }

def discord_karten_tipp(home, away, comp, country, score, minute, karten_liste, quote):
    qt = f"\n💶 **Quote:** {quote}" if quote else ""
    return {
        "title": "🃏 Karten Signal", "color": FARBE_KARTEN,
        "fields": [
            {"name": "🏆 Liga",   "value": f"{comp}",                         "inline": True},
            {"name": "🌍 Land",   "value": f"{country}",                      "inline": True},
            {"name": "📊 Stand",  "value": f"**{score}** | Min. **{minute}'**", "inline": True},
            {"name": "⚽ Spiel",  "value": f"{home} vs {away}",               "inline": False},
            {"name": "🃏 Karten bis Minute 40", "value": "\n".join(karten_liste) or "–", "inline": False},
            {"name": "🎯 Empfehlung", "value": f"Über **5 Karten** gesamt{qt}", "inline": False},
        ],
        "footer": {"text": f"Karten-Bot • {heute()} {jetzt()}"},
    }

def discord_torwart_tipp(home, away, comp, country, shots_home, shots_away, saves_h, saves_a, poss_h, poss_a, min_text, quote):
    qt = f"\n💶 **Quote:** {quote}" if quote else ""
    return {
        "title": "🧤 Torwart Alarm", "color": FARBE_TORWART,
        "fields": [
            {"name": "🏆 Liga",             "value": f"{comp}",           "inline": True},
            {"name": "🌍 Land",             "value": f"{country}",        "inline": True},
            {"name": "📊 Stand",            "value": f"**0:0** | {min_text}", "inline": True},
            {"name": "⚽ Spiel",            "value": f"{home} vs {away}", "inline": False},
            {"name": "🎯 Schüsse aufs Tor",
             "value": f"🔵 {home}: **{shots_home}**\n🔴 {away}: **{shots_away}**\n📊 Gesamt: **{shots_home+shots_away}**", "inline": True},
            {"name": "🧤 Paraden",          "value": f"🔵 {saves_h} | 🔴 {saves_a}", "inline": True},
            {"name": "⚽ Ballbesitz",       "value": f"{poss_h}% | {poss_a}%",        "inline": True},
            {"name": "🎯 Empfehlung", "value": f"Mindestens **1 Tor** fällt noch{qt}", "inline": False},
        ],
        "footer": {"text": f"Torwart-Bot • {heute()} {jetzt()}"},
    }

def discord_druck_tipp(home, away, comp, country, score, minute, druck_team,
                        ecken_stark, ecken_schwach, fk_stark, fk_schwach, quote):
    qt = f"\n💶 **Quote:** {quote}" if quote else ""
    return {
        "title": "🔥 Druck Signal", "color": FARBE_DRUCK,
        "fields": [
            {"name": "🏆 Liga",    "value": f"{comp}",                         "inline": True},
            {"name": "🌍 Land",    "value": f"{country}",                      "inline": True},
            {"name": "📊 Stand",   "value": f"**{score}** | Min. **{minute}'**", "inline": True},
            {"name": "⚽ Spiel",   "value": f"{home} vs {away}",               "inline": False},
            {"name": "🔥 Dominantes Team", "value": f"**{druck_team}**",       "inline": False},
            {"name": "📐 Ecken",   "value": f"**{ecken_stark}** : {ecken_schwach}", "inline": True},
            {"name": "🦵 Freistöße","value": f"**{fk_stark}** : {fk_schwach}", "inline": True},
            {"name": "🎯 Empfehlung", "value": f"Nächste Ecke / Tor für **{druck_team}**{qt}", "inline": False},
        ],
        "footer": {"text": f"Druck-Bot • {heute()} {jetzt()}"},
    }

def discord_comeback_tipp(home, away, comp, country, score, minute,
                           rueckliegend, fuehrend, shots_r, shots_f, poss_r, quote):
    qt = f"\n💶 **Quote:** {quote}" if quote else ""
    return {
        "title": "🔄 Comeback Signal", "color": FARBE_COMEBACK,
        "fields": [
            {"name": "🏆 Liga",    "value": f"{comp}",                         "inline": True},
            {"name": "🌍 Land",    "value": f"{country}",                      "inline": True},
            {"name": "📊 Stand",   "value": f"**{score}** | Min. **{minute}'**", "inline": True},
            {"name": "⚽ Spiel",   "value": f"{home} vs {away}",               "inline": False},
            {"name": "📉 Rückliegend", "value": f"**{rueckliegend}**",         "inline": True},
            {"name": "📈 Führend",     "value": f"**{fuehrend}**",             "inline": True},
            {"name": "🎯 Schüsse Rückliegend", "value": f"**{shots_r}** | Gegner: {shots_f}", "inline": True},
            {"name": "⚽ Ballbesitz",  "value": f"**{poss_r}%**",              "inline": True},
            {"name": "🎯 Empfehlung", "value": f"Beide Teams treffen (Comeback){qt}", "inline": False},
        ],
        "footer": {"text": f"Comeback-Bot • {heute()} {jetzt()}"},
    }

def discord_torflut_tipp(home, away, comp, country, score_hz1, tore_hz1, grenze, quote,
                          shots_ges=0, poss_h="?", poss_a="?"):
    qt = f"\n💶 **Quote:** {quote}" if quote else ""
    return {
        "title": "🌊 Torflut Signal", "color": FARBE_TORFLUT,
        "fields": [
            {"name": "🏆 Liga",            "value": f"{comp}",          "inline": True},
            {"name": "🌍 Land",            "value": f"{country}",       "inline": True},
            {"name": "📊 Halbzeitstand",   "value": f"**{score_hz1}**", "inline": True},
            {"name": "⚽ Spiel",           "value": f"{home} vs {away}", "inline": False},
            {"name": "⚽ Tore HZ1",        "value": f"**{tore_hz1}** Tore", "inline": True},
            {"name": "🎯 Schüsse gesamt",  "value": f"**{shots_ges}**", "inline": True},
            {"name": "⚽ Ballbesitz",      "value": f"{poss_h}% | {poss_a}%", "inline": True},
            {"name": "🎯 Empfehlung",      "value": f"Über **{grenze} Tore** im Gesamtspiel{qt}", "inline": False},
        ],
        "footer": {"text": f"Torflut-Bot • {heute()} {jetzt()}"},
    }

def discord_rotkarte_tipp(home, away, comp, country, score, minute,
                           rote_karte_team, ueberzahl_team, spieler, quote):
    qt = f"\n💶 **Quote:** {quote}" if quote else ""
    return {
        "title": "🟥 Rote Karte Signal", "color": FARBE_ROTKARTE,
        "fields": [
            {"name": "🏆 Liga",    "value": f"{comp}",                         "inline": True},
            {"name": "🌍 Land",    "value": f"{country}",                      "inline": True},
            {"name": "📊 Stand",   "value": f"**{score}** | Min. **{minute}'**", "inline": True},
            {"name": "⚽ Spiel",   "value": f"{home} vs {away}",               "inline": False},
            {"name": "🟥 Rote Karte für", "value": f"**{spieler}**",           "inline": True},
            {"name": "🔴 Team",    "value": f"{rote_karte_team}",              "inline": True},
            {"name": "💪 Überzahl-Team", "value": f"**{ueberzahl_team}**",     "inline": True},
            {"name": "🎯 Empfehlung", "value": f"Nächstes Tor für **{ueberzahl_team}**{qt}", "inline": False},
        ],
        "footer": {"text": f"Rotkarte-Bot • {heute()} {jetzt()}"},
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
    # ROI Tracking
    if quote and quote > 1.0:
        einsatz_wert = EINSATZ
        roi_gewinn   = round((quote - 1) * einsatz_wert, 2) if gewonnen else -einsatz_wert
        if "roi" not in statistik[typ]:
            statistik[typ]["roi"] = 0.0
        statistik[typ]["roi"] = round(statistik[typ].get("roi", 0.0) + roi_gewinn, 2)
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
           f"{ei} Simulation: <b>{'+' if gn>=0 else ''}{gn}€</b>\n"
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
    # Discord Embed für Tagesbericht
    gw_ges  = sum(statistik[t]["gewonnen"] for t in statistik)
    vl_ges  = sum(statistik[t]["verloren"] for t in statistik)
    ges_ges = gw_ges + vl_ges
    pct_ges = round(gw_ges / ges_ges * 100) if ges_ges else 0
    gn_ges  = round(sum(statistik[t]["gewinn"] for t in statistik), 2)
    br_embed = bankroll_laden()
    diff_embed = round(br_embed - BANKROLL, 2)
    farbe_embed = 0x2ECC71 if gn_ges >= 0 else 0xE74C3C
    tages_embed = {
        "title": f"📋 Tagesbericht – {heute()}",
        "color": farbe_embed,
        "fields": [
            {"name": "✅ Gewonnen",      "value": f"**{gw_ges}**",  "inline": True},
            {"name": "❌ Verloren",      "value": f"**{vl_ges}**",  "inline": True},
            {"name": "🎯 Trefferquote", "value": f"**{pct_ges}%**", "inline": True},

            {"name": "💰 Bankroll",     "value": f"**{br_embed}€** ({'+' if diff_embed >= 0 else ''}{diff_embed}€)", "inline": True},
            {"name": "📊 Nach Wetttyp", "value": "\n".join([
                f"📐 Ecken U: {statistik['ecken']['gewonnen']}/{statistik['ecken']['gewonnen']+statistik['ecken']['verloren']}",
                f"📐 Ecken Ü: {statistik['ecken_over']['gewonnen']}/{statistik['ecken_over']['gewonnen']+statistik['ecken_over']['verloren']}",
                f"🃏 Karten: {statistik['karten']['gewonnen']}/{statistik['karten']['gewonnen']+statistik['karten']['verloren']}",
                f"🧤 Torwart: {statistik['torwart']['gewonnen']}/{statistik['torwart']['gewonnen']+statistik['torwart']['verloren']}",
                f"🔥 Druck: {statistik['druck']['gewonnen']}/{statistik['druck']['gewonnen']+statistik['druck']['verloren']}",
                f"🔄 Comeback: {statistik['comeback']['gewonnen']}/{statistik['comeback']['gewonnen']+statistik['comeback']['verloren']}",
                f"🌊 Torflut: {statistik['torflut']['gewonnen']}/{statistik['torflut']['gewonnen']+statistik['torflut']['verloren']}",
                f"🟥 Rotkarte: {statistik['rotkarte']['gewonnen']}/{statistik['rotkarte']['gewonnen']+statistik['rotkarte']['verloren']}",
                f"🥅 HZ1-Tore: {statistik['hz1tore']['gewonnen']}/{statistik['hz1tore']['gewonnen']+statistik['hz1tore']['verloren']}",
                f"🏆 VZ-Tore: {statistik['vztore']['gewonnen']}/{statistik['vztore']['gewonnen']+statistik['vztore']['verloren']}",
            ]), "inline": False},
        ],
        "footer": {"text": f"BetlabLIVE • {heute()} {jetzt()}"},
    }
    send_discord_embed(DISCORD_WEBHOOK_BILANZ, tages_embed)
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
           f"{ei} Simulation: <b>{'+' if gn>=0 else ''}{gn}€</b>\n"
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
    # Discord Embed für Wochenbericht
    farbe_w = 0x2ECC71 if gn >= 0 else 0xE74C3C
    rang_w  = bot_rangliste()
    woche_embed = {
        "title": f"📅 Wochenbericht – KW {de_now().isocalendar()[1]}",
        "color": farbe_w,
        "fields": [
            {"name": "✅ Gewonnen",      "value": f"**{gw}**",  "inline": True},
            {"name": "❌ Verloren",      "value": f"**{vl}**",  "inline": True},
            {"name": "🎯 Trefferquote", "value": f"**{pct}%**", "inline": True},
            {"name": "🏆 Bot-Rangliste (Woche)", "value": rang_w or "Keine Daten", "inline": False},
        ],
        "footer": {"text": f"BetlabLIVE • KW {de_now().isocalendar()[1]} • {heute()}"},
    }
    send_discord_embed(DISCORD_WEBHOOK_BILANZ, woche_embed)
    ab_test_auswerten()
    wöchentliche_xp_auswertung()
    sende_discord_rangliste()
    for t in wochen_statistik:
        wochen_statistik[t] = {"gewonnen": 0, "verloren": 0, "gewinn": 0.0}
    # Weekly Performance Chart
    chart_pfad = erstelle_performance_chart()
    if chart_pfad:
        sende_chart_telegram(chart_pfad)
    print(f"  [Bericht] Wochenbericht gesendet")

# ============================================================
#  AUSWERTUNG
# ============================================================


def _hole_tore_via_events(match_id: str) -> tuple:
    """Holt Tore via Events-Endpoint als Fallback wenn API 0-0 zurückgibt."""
    try:
        events = ls_get_events(match_id)
        h = len([e for e in events if e.get("event") in ("Goal","goal","GOAL") and e.get("home_away") == "home"])
        a = len([e for e in events if e.get("event") in ("Goal","goal","GOAL") and e.get("home_away") == "away"])
        return h, a
    except Exception:
        return 0, 0

def _hole_hz1_tore_via_events(match_id: str) -> int:
    """Holt HZ1-Tore via Events (Tore in Minute <= 45)."""
    try:
        events = ls_get_events(match_id)
        hz1 = [e for e in events
                if e.get("event") in ("Goal","goal","GOAL")
                and _safe_int(e.get("time", 99)) <= 45]
        return len(hz1)
    except Exception:
        return -1

def auswertung_ecken(spiel):
    match_id  = spiel["match_id"]
    hz1_ecken = spiel["hz1_ecken"]
    grenze    = hz1_ecken * 2 + 1
    home, away, quote = spiel["home"], spiel["away"], spiel.get("quote")
    liga = spiel.get("competition", spiel.get("liga", ""))
    try:
        # Triple-Verifikation für FT-Bestätigung
        result = ls_get_match_result(match_id, home, away, liga)
        if not result:
            return None  # Noch kein verlässliches Ergebnis
        stats       = get_statistiken(match_id)
        total_ecken = stats["corners_home"] + stats["corners_away"]
        if total_ecken == 0 and hz1_ecken > 0:
            total_ecken = hz1_ecken
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
    liga = spiel.get("competition", spiel.get("liga", ""))
    try:
        result = ls_get_match_result(match_id, home, away, liga)
        if not result:
            return None
        stats       = get_statistiken(match_id)
        total_ecken = stats["corners_home"] + stats["corners_away"]
        if total_ecken == 0 and hz1_ecken >= 7:
            total_ecken = hz1_ecken
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
    liga = spiel.get("competition", spiel.get("liga", ""))
    try:
        result = ls_get_match_result(match_id, home, away, liga)
        if not result:
            return None
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
    liga = spiel.get("competition", spiel.get("liga", ""))
    try:
        result = ls_get_match_result(match_id, home, away, liga)
        if not result:
            return None
        score    = result.get("score", "")
        h, a     = parse_score(score) if score else (0, 0)
        # Fallback via Events wenn API 0-0 zurückgibt
        if h == 0 and a == 0:
            try:
                events = ls_get_events(match_id)
                tore_e = len([e for e in events if e.get("event") in ("Goal", "goal")])
                if tore_e > 0:
                    h, a = 1, 0  # Mind. 1 Tor gefallen
                    score = f"mind. {tore_e} Tor(e)"
            except Exception:
                pass
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
    match_id     = spiel["match_id"]
    druck_team   = spiel["druck_team"]
    score_signal = spiel.get("score_signal", "")
    home, away, quote = spiel["home"], spiel["away"], spiel.get("quote")
    h_sig, a_sig = parse_score(score_signal) if score_signal else (0, 0)
    liga = spiel.get("competition", spiel.get("liga", ""))
    try:
        result = ls_get_match_result(match_id, home, away, liga)
        if not result:
            return None
        score = result.get("score", "")
        h, a  = parse_score(score) if score else (0, 0)

        # Fallback via Events wenn API 0-0 zurückgibt
        if h == 0 and a == 0 and (h_sig > 0 or a_sig > 0):
            try:
                events = ls_get_events(match_id)
                h = len([e for e in events if e.get("event") in ("Goal","goal") and e.get("home_away") == "home"])
                a = len([e for e in events if e.get("event") in ("Goal","goal") and e.get("home_away") == "away"])
                score = f"{h} - {a}" if h + a > 0 else score_signal
            except Exception:
                h, a = h_sig, a_sig
                score = score_signal

        # Gewonnen wenn Druck-Team NACH dem Signal noch getroffen hat
        # Vergleich: Endstand vs Stand beim Signal
        if druck_team == home:
            gewonnen = h > h_sig  # Hat Heim nach Signal getroffen?
        else:
            gewonnen = a > a_sig  # Hat Gast nach Signal getroffen?

        # Fallback: Wenn kein Signal-Score vorhanden, check ob Druck-Team führt
        if not score_signal:
            gewonnen = (h > a) if druck_team == home else (a > h)

        update_statistik("druck", gewonnen, quote)
        emoji = "✅ GEWONNEN" if gewonnen else "❌ VERLOREN"
        ql = f"💶 Quote: <b>{quote}</b> → Gewinn: <b>+{round((quote-1)*EINSATZ,2)}€</b>\n" if quote and gewonnen else ""
        return (f"📊 <b>Auswertung – Druck</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                f"📌 {home} vs {away}\n🔥 Druck-Team: <b>{druck_team}</b>\n"
                f"📊 Stand bei Signal: <b>{score_signal}</b>\n"
                f"📈 Endstand: <b>{score}</b>\n{ql}"
                f"━━━━━━━━━━━━━━━━━━━━\n{emoji}\n🕐 {jetzt()} Uhr")
    except Exception as e:
        print(f"  [Auswertung] Druck Fehler: {e}")
        return None

def auswertung_comeback(spiel):
    match_id     = spiel["match_id"]
    rueckliegend = spiel["rueckliegend"]
    home, away, quote = spiel["home"], spiel["away"], spiel.get("quote")
    # Score beim Signal speichern für Fallback
    score_signal = spiel.get("score_signal", "")
    h_sig, a_sig = parse_score(score_signal) if score_signal else (0, 0)
    liga = spiel.get("competition", spiel.get("liga", ""))
    try:
        result = ls_get_match_result(match_id, home, away, liga)
        if not result:
            return None
        score = result.get("score", "")
        h, a  = parse_score(score) if score else (0, 0)

        # Wenn API 0-0 zurückgibt aber zum Signalzeitpunkt schon Tore gefallen:
        # → API-Fehler, verwende Score vom Signal als Basis
        if h == 0 and a == 0 and (h_sig > 0 or a_sig > 0):
            print(f"  [Auswertung] Comeback: API gibt 0-0, aber Signal-Score war {score_signal} → verwende Events")
            # Via Events prüfen
            try:
                events = ls_get_events(match_id)
                tore_home = len([e for e in events if e.get("event") in ("Goal", "goal") and e.get("home_away") == "home"])
                tore_away = len([e for e in events if e.get("event") in ("Goal", "goal") and e.get("home_away") == "away"])
                if tore_home > 0 or tore_away > 0:
                    h, a = tore_home, tore_away
                    score = f"{h} - {a}"
                else:
                    # Fallback: Signal-Score nehmen
                    h, a = h_sig, a_sig
                    score = score_signal
            except Exception:
                h, a = h_sig, a_sig
                score = score_signal

        # Beide Teams treffen: h >= 1 UND a >= 1
        # Wichtig: Wenn zum Signalzeitpunkt SCHON beide Teams getroffen hatten → direkt gewonnen
        bereits_beide_getroffen = h_sig >= 1 and a_sig >= 1
        gewonnen = (h >= 1 and a >= 1) or bereits_beide_getroffen
        update_statistik("comeback", gewonnen, quote)
        emoji = "✅ GEWONNEN" if gewonnen else "❌ VERLOREN"
        ql = f"💶 Quote: <b>{quote}</b> → Gewinn: <b>+{round((quote-1)*EINSATZ,2)}€</b>\n" if quote and gewonnen else ""
        hinweis = " (zum Signalzeitpunkt bereits erfüllt)" if bereits_beide_getroffen else ""
        return (f"📊 <b>Auswertung – Comeback</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                f"📌 {home} vs {away}\n🔄 Rückliegend: <b>{rueckliegend}</b>\n"
                f"📊 Stand bei Signal: <b>{score_signal}</b>\n"
                f"🎯 Tipp: Beide Teams treffen{hinweis}\n"
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
    liga = spiel.get("competition", spiel.get("liga", ""))
    try:
        result = ls_get_match_result(match_id, home, away, liga)
        if not result:
            return None
        score = result.get("score", "")
        h, a  = parse_score(score) if score else (0, 0)
        tore  = h + a
        if tore == 0 and hz1_tore > 0:
            h_e, a_e = _hole_tore_via_events(match_id)
            if h_e + a_e > 0:
                h, a  = h_e, a_e
                tore  = h + a
                score = f"{h} - {a}"
            else:
                # Mindestens HZ1-Tore sind sicher gefallen
                tore  = hz1_tore
                score = f"mind. {hz1_tore} (HZ1)"
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
        liga = spiel.get("competition", spiel.get("liga", ""))
        result = ls_get_match_result(match_id, home, away, liga)
        if not result:
            return None
        score = result.get("score", "")
        h_end, a_end = parse_score(score) if score else (0, 0)
        h_sig, a_sig = parse_score(score_signal) if score_signal else (0, 0)
        if h_end == 0 and a_end == 0 and (h_sig > 0 or a_sig > 0):
            try:
                events = ls_get_events(match_id)
                h_end = len([e for e in events if e.get("event") in ("Goal","goal") and e.get("home_away") == "home"])
                a_end = len([e for e in events if e.get("event") in ("Goal","goal") and e.get("home_away") == "away"])
                score = f"{h_end} - {a_end}" if h_end + a_end > 0 else score_signal
            except Exception:
                h_end, a_end = h_sig, a_sig
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
    liga = spiel.get("competition", spiel.get("liga", ""))
    try:
        result = ls_get_match_result(match_id, home, away, liga)
        if not result:
            return None
        ht    = result.get("ht_score", "")
        if not ht:
            # HZ1 Score nicht in single match – versuche nochmal direkt
            import time as _t; _t.sleep(5)
            match = ls_get_single_match(match_id)
            ht    = (match.get("scores") or {}).get("ht_score", "")
        if not ht:
            # Fallback: Tore via Events zählen (nur Minute <= 45)
            hz1_tore_events = _hole_hz1_tore_via_events(match_id)
            if hz1_tore_events >= 0:
                ht = f"{hz1_tore_events} - 0 (Events)"
                print(f"  [Auswertung] Hz1Tore: Events-Fallback → {hz1_tore_events} Tore in HZ1")
            else:
                print(f"  [Auswertung] Hz1Tore: kein HZ1-Score für {home} vs {away} – übersprungen")
                return None
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
    liga = spiel.get("competition", spiel.get("liga", ""))
    try:
        result = ls_get_match_result(match_id, home, away, liga)
        if not result:
            return None
        score    = result.get("score", "")
        h, a     = parse_score(score) if score else (0, 0)
        vz_tore  = h + a
        if vz_tore == 0:
            h_e, a_e = _hole_tore_via_events(match_id)
            if h_e + a_e > 0:
                h, a    = h_e, a_e
                vz_tore = h + a
                score   = f"{h} - {a}"
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
    # Alle FT-Stati werden mit .upper() verglichen – kein Case-Fehler möglich
FT_STATI_SET    = {"FT", "FINISHED", "AET", "PEN", "FULL TIME",
                   "AFTER EXTRA TIME", "PENALTIES", "ENDED", "FT.", "AET."}

def ist_spiel_fertig(status: str, time_val: str = "") -> bool:
    """Robuste FT-Erkennung mit .upper() Normalisierung."""
    s = str(status or "").upper().strip()
    t = str(time_val or "").upper().strip()
    if s in FT_STATI_SET:
        return True
    if t in {"FT", "FULL TIME", "AET", "ENDED", "FINISHED"}:
        return True
    return False

# Kompatibilität
FT_STATI = FT_STATI_SET

def bot_auswertung_und_berichte():
    """Tagesbericht, Wochenbericht und direkte FT-Erkennung."""
    print("[Auswertung-Bot] Gestartet | Direkte FT-Erkennung alle 2 Min.")
    tagesbericht_gesendet  = None
    wochenbericht_gesendet = None
    monatsbericht_gesendet = None
    leerer_status = {}
    auswertung_done = set()

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
                    # PDF Monatsbericht
                    import calendar
                    lm_name = (de_now().replace(day=1) - timedelta(days=1)).strftime("%B %Y")
                    pdf_pfad = erstelle_monatsbericht_pdf()
                    if pdf_pfad:
                        sende_pdf_telegram(pdf_pfad, lm_name)
                    # Performance Chart
                    chart_pfad = erstelle_performance_chart()
                    if chart_pfad:
                        sende_chart_telegram(chart_pfad)

            # Fallback: zu alte Beobachtungen entfernen
            auswertung_fallback_check()

            # Live-Liste einmal laden – für alle Spiele verwenden
            try:
                alle_live = get_live_matches()
                live_ids  = {str(m.get("id")) for m in alle_live}
            except Exception as e:
                print(f"  [Auswertung] Live-Liste Fehler: {e}")
                live_ids = set()

            # FT-Erkennung: Live-Liste + Single-Match kombiniert
            for match_id, spiel in list(beobachtete_spiele.items()):
                if match_id in auswertung_done:
                    continue

                home_str = spiel.get("home", "?")
                away_str = spiel.get("away", "?")

                # Mindest-Wartezeit prüfen (vor API-Call um Anfragen zu sparen)
                signal_zeit = spiel.get("signal_zeit", 0)
                minuten_seit_signal = (time.time() - signal_zeit) / 60 if signal_zeit else 999
                min_warte_check = {
                    "ecken": 50, "torflut": 50, "hz1tore": 35,
                    "vztore": 75, "karten": 75, "torwart": 30,
                    "comeback": 25, "druck": 25, "rotkarte": 20, "ecken_over": 20,
                }.get(spiel.get("typ", ""), 25)
                if signal_zeit > 0 and minuten_seit_signal < min_warte_check:
                    continue

                # Methode 1: Nicht in Live-Liste?
                nicht_live = match_id not in live_ids

                # Methode 2: Single-Match Status prüfen
                status = ""
                minute = 0
                try:
                    match    = ls_get_single_match(match_id)
                    status   = str(match.get("status", "") or "")
                    time_val = str(match.get("time", "") or "")
                    minute   = _safe_int(time_val) if time_val.isdigit() else 0
                    if time_val.upper() in ("FT", "FULL TIME", "AET"):
                        status = "FT"
                except Exception as e:
                    # API-Fehler → leerer_status erhöhen
                    leerer_status[match_id] = leerer_status.get(match_id, 0) + 1
                    print(f"  [Auswertung] API-Fehler {home_str} vs {away_str} ({leerer_status[match_id]}x): {e}")

                status_ft = status in FT_STATI

                # Leerer Status zählen
                if status == "" and minute == 0 and not status_ft:
                    leerer_status[match_id] = leerer_status.get(match_id, 0) + 1
                    print(f"  [Auswertung] {home_str} vs {away_str} | Kein Status ({leerer_status[match_id]}x)")
                elif status and status not in FT_STATI:
                    print(f"  [Auswertung] {home_str} vs {away_str} | {status} | {minute}'")
                    if nicht_live:
                        print(f"  [Auswertung] Nicht in Live-Liste aber Status={status} – zähle")
                    else:
                        leerer_status.pop(match_id, None)
                        ft_bestaetigung.pop(match_id, None)
                        continue

                # FT erkennen wenn: Status FT, ODER (nicht live + 2x kein Status)
                # Robuste FT-Erkennung
                status_raw = str(spiel_live.get("status", "") if spiel_live else "")
                time_raw   = str(spiel_live.get("time",   "") if spiel_live else "")
                ist_fertig = (ist_spiel_fertig(status_raw, time_raw) or
                              (nicht_live and leerer_status.get(match_id, 0) >= 2))

                if not ist_fertig:
                    continue

                # Spiel beendet – SOFORT auswerten (kein Timer)!
                print(f"  [Auswertung] ✅ Werte aus: {home_str} vs {away_str}")
                time.sleep(10)

                typ        = spiel["typ"]
                webhook    = spiel["webhook"]
                auswert_fn = AUSWERTUNG_FNS.get(typ)
                msg        = auswert_fn(spiel) if auswert_fn else None

                if msg:
                    send_telegram(msg)
                    gewonnen = "GEWONNEN" in msg
                    if typ in ("ecken", "ecken_over"):
                        hz1    = spiel.get("hz1_ecken", 0)
                        grenze = hz1 * 2 + 1 if typ == "ecken" else 14
                        m_e    = re.search(r"Tatsächlich.*?(\d+)", msg)
                        total  = m_e.group(1) if m_e else "?"
                        details = {"📐 Ecken HZ1": f"**{hz1}**",
                                   "🎯 Tipp": f"{'Unter' if typ == 'ecken' else 'Über'} **{grenze}** Ecken",
                                   "📈 Endstand": f"**{total}** Ecken gesamt"}
                    elif typ == "karten":
                        m_k    = re.search(r"Tatsächlich.*?(\d+)", msg)
                        total  = m_k.group(1) if m_k else "?"
                        details = {"🃏 Karten Signal": f"**{spiel.get('karten_anzahl','?')}**",
                                   "🎯 Tipp": "Über **5** Karten",
                                   "📈 Endstand": f"**{total}** Karten"}
                    elif typ == "torwart":
                        m_s    = re.search(r"Endstand.*?(\d+ - \d+)", msg)
                        details = {"🎯 Tipp": "Mind. **1 Tor**",
                                   "📈 Endstand": f"**{m_s.group(1) if m_s else '?'}**"}
                    elif typ == "druck":
                        details = {"🔥 Druck-Team": f"**{spiel.get('druck_team','?')}**",
                                   "📈 Ergebnis": "✅ Tor" if gewonnen else "❌ Kein Tor"}
                    elif typ == "comeback":
                        m_s    = re.search(r"Endstand.*?(\d+ - \d+)", msg)
                        details = {"🔄 Rückliegend": f"**{spiel.get('rueckliegend','?')}**",
                                   "🎯 Tipp": "Beide Teams treffen",
                                   "📈 Endstand": f"**{m_s.group(1) if m_s else '?'}**"}
                    elif typ == "torflut":
                        m_s    = re.search(r"Endstand.*?(\d+ - \d+)", msg)
                        details = {"⚽ Tore HZ1": f"**{spiel.get('hz1_tore','?')}**",
                                   "🎯 Tipp": f"Über **{spiel.get('grenze','?')}** Tore",
                                   "📈 Endstand": f"**{m_s.group(1) if m_s else '?'}**"}
                    elif typ == "rotkarte":
                        details = {"💪 Überzahl": f"**{spiel.get('ueberzahl_team','?')}**",
                                   "📊 Stand Signal": f"**{spiel.get('score_signal','?')}**"}
                    elif typ in ("hz1tore", "vztore"):
                        details = {"🎯 Tipp": f"**{spiel.get('richtung','?').capitalize()} {spiel.get('linie','?')}** Tore",
                                   "📈 Ergebnis": "✅ Gewonnen" if gewonnen else "❌ Verloren"}
                    else:
                        details = {"📊 Typ": f"**{typ.upper()}**"}
                    embed = discord_auswertung(typ, home_str, away_str, gewonnen, details)
                    send_discord_embed(webhook, embed)
                    print(f"  [Auswertung] ✅ Gesendet: {home_str} vs {away_str} ({typ})")
                    if not gewonnen:
                        threading.Thread(target=claude_verloren_analyse,
                            args=(home_str, away_str, typ, msg), daemon=True).start()
                    auswertung_done.add(match_id)
                else:
                    ft_bestaetigung.pop(match_id, None)
                    print(f"  [Auswertung] ⚠️ Kein Ergebnis für {home_str} vs {away_str} – retry")
                leerer_status.pop(match_id, None)
                ft_bestaetigung.pop(match_id, None)

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
                grenze  = corners * 2 + 1
                if corners == 0:
                    print(f"  [Ecken-Bot] {home} vs {away} | Keine Ecken-Statistik von API")
                    continue
                if corners > MAX_CORNERS:
                    print(f"  [Ecken-Bot] {home} vs {away} | Zu viele Ecken: {corners} > {MAX_CORNERS}")
                    continue
                if corners <= MAX_CORNERS:
                    if not tipp_erlaubt(match_id, "Ecken-Bot"):
                        continue
                    # Liga-Filter + Whitelist
                    if not liga_erlaubt(comp):
                        continue
                    if not whitelist_check(comp, home, away):
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
                    grenze = corners * 2 + 1 + wetter_bonus
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
                    # Ecken-Markt bei Bookmarkern verfügbar?
                    if not prüfe_ecken_verfuegbar(home, away):
                        notified_ecken.add(match_id)
                        continue
                    beobachtung_hinzufuegen(match_id, {
                        "typ": "ecken", "match_id": match_id,
                        "home": home, "away": away, "hz1_ecken": corners,
                        "quote": quote, "einsatz": einsatz, "liga": comp,
                        "webhook": DISCORD_WEBHOOK_ECKEN,
                        "signal_zeit": time.time(), "bot": "Ecken-Bot"
                    })
                    signal_eintragen(match_id, "ecken", home, away, comp, corners, grenze, quote, einsatz)
                    gegentipp_registrieren(match_id, "ecken", "unter", "Ecken-Bot")
                    print(f"  [Ecken-Bot] OK: {home} vs {away} | K:{konfidenz}/10 | Claude:{'✅' if cl_ok else '⚠️'}")
                time.sleep(0.5)
            bot_fehler_reset("Ecken-Bot")
        except Exception as e:
            bot_fehler_melden("Ecken-Bot", e)
        try:
            dynamischer_sleep(get_live_matches())
        except Exception:
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
                beobachtung_hinzufuegen(match_id, {
                    "typ": "ecken_over", "match_id": match_id,
                    "home": home, "away": away, "hz1_ecken": corners,
                    "quote": quote, "einsatz": einsatz, "liga": comp,
                    "webhook": DISCORD_WEBHOOK_ECKEN, "signal_zeit": time.time(), "bot": "Ecken-Über-Bot"
                    })
                signal_eintragen(match_id, "ecken_over", home, away, comp, corners, 14, quote, einsatz)
                print(f"  [Ecken-Über-Bot] OK: {home} vs {away} ({corners} Ecken in Min. {minute})")
                time.sleep(0.5)
            bot_fehler_reset("Ecken-Über-Bot")
        except Exception as e:
            bot_fehler_melden("Ecken-Über-Bot", e)
        try:
            dynamischer_sleep(get_live_matches())
        except Exception:
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
                    beobachtung_hinzufuegen(match_id, {
                        "typ": "karten", "match_id": match_id,
                        "home": home, "away": away, "karten_anzahl": len(karten),
                        "quote": quote, "webhook": DISCORD_WEBHOOK_KARTEN, "signal_zeit": time.time(), "bot": "Karten-Bot"
                    })
                    print(f"  [Karten-Bot] OK: {home} vs {away} ({len(karten)} Karten)")
                time.sleep(0.5)
            bot_fehler_reset("Karten-Bot")
        except Exception as e:
            bot_fehler_melden("Karten-Bot", e)
        try:
            dynamischer_sleep(get_live_matches())
        except Exception:
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
                beobachtung_hinzufuegen(match_id, {
                    "typ": "torwart", "match_id": match_id,
                    "home": home, "away": away,
                    "quote": quote, "webhook": DISCORD_WEBHOOK_TORWART, "signal_zeit": time.time(), "bot": "Torwart-Bot"
                    })
                print(f"  [Torwart-Bot] OK: {home} vs {away} | {shots_ges} Schüsse")
                time.sleep(0.5)
            bot_fehler_reset("Torwart-Bot")
        except Exception as e:
            bot_fehler_melden("Torwart-Bot", e)
        try:
            dynamischer_sleep(get_live_matches())
        except Exception:
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
                beobachtung_hinzufuegen(match_id, {
                    "typ": "druck", "match_id": match_id,
                    "home": home, "away": away, "druck_team": druck_team,
                    "score_signal": score,
                    "quote": quote, "webhook": DISCORD_WEBHOOK_DRUCK, "signal_zeit": time.time(), "bot": "Druck-Bot"
                    })
                print(f"  [Druck-Bot] OK: {home} vs {away} | {druck_team} dominiert ({ecken_stark}:{ecken_schwach} Ecken)")
                time.sleep(0.5)
            bot_fehler_reset("Druck-Bot")
        except Exception as e:
            bot_fehler_melden("Druck-Bot", e)
        try:
            dynamischer_sleep(get_live_matches())
        except Exception:
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
                beobachtung_hinzufuegen(match_id, {
                    "typ": "comeback", "match_id": match_id,
                    "home": home, "away": away, "rueckliegend": rueckliegend,
                    "score_signal": score_str,
                    "quote": quote, "webhook": DISCORD_WEBHOOK_COMEBACK, "signal_zeit": time.time(), "bot": "Comeback-Bot", "competition": comp
                    })
                print(f"  [Comeback-Bot] OK: {home} vs {away} | {rueckliegend} liegt zurück aber dominiert")
                time.sleep(0.5)
            bot_fehler_reset("Comeback-Bot")
        except Exception as e:
            bot_fehler_melden("Comeback-Bot", e)
        try:
            dynamischer_sleep(get_live_matches())
        except Exception:
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
                # Stats laden für Discord Embed + intelligente Grenze
                stats_tf   = get_statistiken(match_id)
                shots_h_tf = stats_tf["shots_on_target_home"]
                shots_a_tf = stats_tf["shots_on_target_away"]
                shots_tf   = shots_h_tf + shots_a_tf
                poss_th    = stats_tf["possession_home"]
                poss_ta    = stats_tf["possession_away"]
                if shots_tf == 0:
                    shots_h_tf = stats_tf["dangerous_attacks_home"]
                    shots_a_tf = stats_tf["dangerous_attacks_away"]
                    shots_tf   = shots_h_tf + shots_a_tf
                # Intelligente Grenze basierend auf Spielsituation
                # Schüsse aufs Tor in HZ1 = Indikator für weiteres Torpotenzial
                # Formel: Tore HZ1 + erwartete HZ2 Tore (min. 1, max. 4)
                shots_pro_tor = max(1, shots_tf / max(tore_hz1, 1))
                # Erwartete Tore in HZ2 basierend auf Schüssen-Rate
                erwartete_hz2 = round(shots_tf / max(shots_pro_tor, 1.5), 1)
                erwartete_hz2 = max(1, min(4, erwartete_hz2))
                grenze_roh    = tore_hz1 + erwartete_hz2
                # Sinnvolle Grenze wählen (0.5er Schritte)
                if grenze_roh <= tore_hz1 + 1.5:
                    grenze = tore_hz1 + 1
                elif grenze_roh <= tore_hz1 + 2.5:
                    grenze = tore_hz1 + 2
                else:
                    grenze = tore_hz1 + 3
                # Bei sehr torreichem Spiel mind. +2 empfehlen
                if tore_hz1 >= 4:
                    grenze = max(grenze, tore_hz1 + 2)
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
                    discord_torflut_tipp(home, away, comp, country, score_str, tore_hz1, grenze, quote, shots_tf, poss_th, poss_ta))
                notified_torflut.add(match_id)
                beobachtung_hinzufuegen(match_id, {
                    "typ": "torflut", "match_id": match_id,
                    "home": home, "away": away, "hz1_tore": tore_hz1,
                    "grenze": grenze, "quote": quote, "webhook": DISCORD_WEBHOOK_TORFLUT, "signal_zeit": time.time(), "bot": "Torflut-Bot"
                    })
                print(f"  [Torflut-Bot] OK: {home} vs {away} | {tore_hz1} Tore in HZ1")
                time.sleep(0.5)
            bot_fehler_reset("Torflut-Bot")
        except Exception as e:
            bot_fehler_melden("Torflut-Bot", e)
        try:
            dynamischer_sleep(get_live_matches())
        except Exception:
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
                beobachtung_hinzufuegen(match_id, {
                    "typ": "rotkarte", "match_id": match_id,
                    "home": home, "away": away,
                    "ueberzahl_team": ueberzahl_team,
                    "score_signal": score,
                    "quote": quote, "webhook": DISCORD_WEBHOOK_ROTKARTE, "signal_zeit": time.time(), "bot": "Rotkarte-Bot"
                    })
                print(f"  [Rotkarte-Bot] OK: {home} vs {away} | {ueberzahl_team} in Überzahl (Min. {karte_min})")
                time.sleep(0.5)
            bot_fehler_reset("Rotkarte-Bot")
        except Exception as e:
            bot_fehler_melden("Rotkarte-Bot", e)
        try:
            dynamischer_sleep(get_live_matches())
        except Exception:
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

                # Form einmal berechnen – für beide Tipp-Typen
                form_home_avg = get_team_saisonform(home_id)
                form_away_avg = get_team_saisonform(away_id)
                form_avg_val  = round((form_home_avg + form_away_avg) / 2, 2) if form_home_avg and form_away_avg else None

                # ── HZ1-Tore Tipp ──────────────────────────────
                if match_id not in notified_hz1tore and ana.get("avg_hz1") is not None:
                    # Dynamische Grenzen berechnen
                    dyn_ueber_hz1, dyn_unter_hz1 = berechne_dynamische_grenzen(ana["avg_hz1"], form_avg_val, "hz1")
                    tipp_hz1 = tipp_aus_avg(ana["avg_hz1"], dyn_ueber_hz1, dyn_unter_hz1)
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
                        beobachtung_hinzufuegen(match_id, {
                            "typ": "hz1tore", "match_id": match_id,
                            "home": home, "away": away, "liga": comp,
                            "richtung": richtung, "linie": linie,
                            "quote": quote, "einsatz": einsatz,
                            "webhook": DISCORD_WEBHOOK_HZ1TORE,
                            "signal_zeit": time.time()
                        })
                        signal_eintragen(match_id, "hz1tore", home, away, comp, ana["avg_hz1"], linie, quote, einsatz)
                        gegentipp_registrieren(match_id, "hz1tore", richtung, "Tore-Bot")
                        print(f"  [Tore-Bot] HZ1 OK: {home} vs {away} | {richtung} {linie} (Ø {ana['avg_hz1']})")

                # ── VZ-Tore Tipp ────────────────────────────────
                if match_id not in notified_vztore:
                    dyn_ueber_vz, dyn_unter_vz = berechne_dynamische_grenzen(ana["avg_vz"], form_avg_val, "vz")
                    tipp_vz = tipp_aus_avg(ana["avg_vz"], dyn_ueber_vz, dyn_unter_vz)
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
                        beobachtung_hinzufuegen(match_id, {
                            "typ": "vztore", "match_id": match_id,
                            "home": home, "away": away, "liga": comp,
                            "richtung": richtung, "linie": linie,
                            "quote": quote, "einsatz": einsatz,
                            "webhook": DISCORD_WEBHOOK_VZTORE,
                            "signal_zeit": time.time()
                        })
                        signal_eintragen(match_id, "vztore", home, away, comp, ana["avg_vz"], linie, quote, einsatz)
                        gegentipp_registrieren(match_id, "vztore", richtung, "Tore-Bot")
                        print(f"  [Tore-Bot] VZ OK: {home} vs {away} | {richtung} {linie} (Ø {ana['avg_vz']})")

                time.sleep(0.5)
            bot_fehler_reset("Tore-Bot")
        except Exception as e:
            bot_fehler_melden("Tore-Bot", e)
        try:
            dynamischer_sleep(get_live_matches())
        except Exception:
            time.sleep(FUSSBALL_INTERVAL * 60)

# ============================================================
#  TELEGRAM BEFEHLE (/status /pause /statistik /bankroll)
# ============================================================


# ── Command Help Texte ──────────────────────────────────────
CMD_HILFE = {
    "/statistik": (
        "📊 <b>Statistik</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        "Zeigt deine aktuelle Tages-Bilanz:\n\n"
        "✅ Gewonnene Tipps\n❌ Verlorene Tipps\n"
        "🎯 Trefferquote in %\n💰 ROI (Return on Investment)\n"
        "⏰ Beste Tageszeit für Signale\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "Einfach schreiben: <code>/statistik</code>"
    ),
    "/live": (
        "🔴 <b>Live Signale</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        "Zeigt alle Signale die gerade aktiv sind\n"
        "und noch ausgewertet werden.\n\n"
        "Du siehst welche Spiele der Bot gerade\n"
        "beobachtet und auf ein Ergebnis wartet.\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "Einfach schreiben: <code>/live</code>"
    ),
    "/chart": (
        "📈 <b>Performance Chart</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        "Sendet dir ein Liniendiagramm das zeigt\n"
        "wie sich deine Trefferquote entwickelt hat.\n\n"
        "🟢 Über 50% = grün\n🔴 Unter 50% = rot\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "Einfach schreiben: <code>/chart</code>"
    ),
    "/bankroll": (
        "💰 <b>Bankroll</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        "Zeigt deine aktuelle Bankroll\n"
        "und die Veränderung seit dem Start.\n\n"
        "💡 Tipp: Mit <code>/compound</code> kannst du\n"
        "simulieren wie deine Bankroll wächst.\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "Einfach schreiben: <code>/bankroll</code>"
    ),
    "/tipp": (
        "🎯 <b>Manueller Tipp</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        "Tipp manuell eingeben und tracken lassen.\n\n"
        "📝 Format:\n"
        "<code>/tipp [Spiel] [Bet] [Quote]</code>\n\n"
        "📌 Beispiele:\n"
        "<code>/tipp ManCity Über2.5 1.85</code>\n"
        "<code>/tipp BVBvsBayern HeimsiegX 3.20</code>\n\n"
        "Nach dem Spiel:\n"
        "<code>/gewonnen</code> oder <code>/verloren</code>"
    ),
    "/erklaer": (
        "🔍 <b>Bot Erklärung</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        "Erklärt dir wie ein bestimmter Bot funktioniert.\n\n"
        "📝 Format: <code>/erklaer [botname]</code>\n\n"
        "📌 Verfügbare Bots:\n"
        "<code>ecken</code> | <code>karten</code> | <code>druck</code>\n"
        "<code>comeback</code> | <code>torwart</code> | <code>value</code> | <code>xg</code>\n\n"
        "Beispiel: <code>/erklaer ecken</code>"
    ),
    "/whitelist": (
        "📋 <b>Whitelist – Liga & Team Filter</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        "Schränke den Bot auf bestimmte Ligen ein.\n\n"
        "📝 Befehle:\n"
        "<code>/whitelist on</code> – aktivieren\n"
        "<code>/whitelist off</code> – deaktivieren\n"
        "<code>/whitelist liga [Name]</code> – Liga hinzufügen\n"
        "<code>/whitelist team [Name]</code> – Team hinzufügen\n"
        "<code>/whitelist reset</code> – alles leeren\n\n"
        "Beispiel: <code>/whitelist liga Bundesliga</code>"
    ),
    "/suche": (
        "🔎 <b>Signal Archiv Suche</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        "Durchsucht alle vergangenen Signale.\n\n"
        "📝 Format: <code>/suche [Begriff]</code>\n\n"
        "📌 Beispiele:\n"
        "<code>/suche Manchester</code>\n"
        "<code>/suche Bundesliga</code>\n"
        "<code>/suche Bayern</code>\n\n"
        "Zeigt die letzten 10 Treffer."
    ),
    "/export": (
        "📤 <b>Daten Export</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        "Erstellt eine Excel-Datei mit allen\n"
        "Signalen der letzten 30 Tage.\n\n"
        "📊 Enthält: Datum, Spiel, Tipp, Quote,\n"
        "EV-Score, Ergebnis und mehr.\n\n"
        "⚠️ Benötigt: <code>pip install openpyxl</code>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "Einfach schreiben: <code>/export</code>"
    ),
    "/compound": (
        "📈 <b>Bankroll Simulation</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        "Simuliert wie deine Bankroll über 20 Wochen\n"
        "wächst wenn du Gewinne reinvestierst.\n\n"
        "Nutzt das Kelly-Kriterium und deine\n"
        "echte Trefferquote für die Berechnung.\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "Einfach schreiben: <code>/compound</code>"
    ),
    "/pause": (
        "⏸️ <b>Bot pausieren</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        "Pausiert alle Signal-Bots oder\n"
        "setzt sie wieder fort.\n\n"
        "⚠️ Im pausierten Zustand:\n"
        "Keine Signale, keine PreMatch-Tipps.\n"
        "Auswertungen laufen weiter.\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "Einfach schreiben: <code>/pause</code>"
    ),
    "/addadmin": (
        "👤 <b>Admin hinzufügen</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        "Gibt einer anderen Person Zugriff\n"
        "auf alle Bot-Befehle.\n\n"
        "📝 Format: <code>/addadmin [Telegram-ID]</code>\n\n"
        "💡 Tipp: Telegram-ID herausfinden\n"
        "via @userinfobot"
    ),
}

def sende_cmd_hilfe(chat_id: str, cmd: str):
    """Sendet Hilfe-Text für einen Command."""
    text = CMD_HILFE.get(cmd)
    if text:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                      json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"}, timeout=10)
        return True
    return False

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
                msg_obj  = update.get("message", {}) or {}
                chat_id  = str(msg_obj.get("chat", {}).get("id", ""))
                user_id  = str(msg_obj.get("from", {}).get("id", ""))
                username = msg_obj.get("from", {}).get("first_name", "Anonym")
                text     = msg_obj.get("text", "").strip()

                # Sprache erkennen + speichern
                if text and len(text) > 5 and user_id:
                    _user_sprache[user_id] = erkenne_sprache(text)

                # Menu Callback verarbeiten
                if "callback_query" not in update and not text:
                    continue

                # Onboarding für neue User
                if user_id and user_id not in BEKANNTE_USER:
                    BEKANNTE_USER.add(user_id)
                    bekannte_user_speichern()
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                  json={"chat_id": chat_id, "text": ONBOARDING_NACHRICHT,
                                        "parse_mode": "HTML"}, timeout=10).lower()

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
                    # ROI berechnen
                    roi_ges = sum(statistik[t].get("roi", 0.0) for t in statistik)
                    roi_emoji = "📈" if roi_ges >= 0 else "📉"
                    # Beste Spielzeit heute
                    beste_h = sorted(
                        [(h, s) for h, s in stunden_statistik.items() if s["gewonnen"]+s["verloren"] > 0],
                        key=lambda x: x[1]["gewonnen"] / max(x[1]["gewonnen"]+x[1]["verloren"], 1),
                        reverse=True
                    )
                    beste_h_text = f"{beste_h[0][0]}:00 Uhr" if beste_h else "–"
                    antwort = (f"📊 <b>Statistik heute</b>\n"
                               f"━━━━━━━━━━━━━━━━━━━━\n"
                               f"✅ Gewonnen: <b>{gw}</b>\n"
                               f"❌ Verloren: <b>{vl}</b>\n"
                               f"🎯 Trefferquote: <b>{pct}%</b>\n"
                               f"{roi_emoji} ROI: <b>{'+' if roi_ges >= 0 else ''}{round(roi_ges, 2)}€</b>\n"
                               f"{streak_sym} Streak: <b>{abs(streak_aktuell)}x {'Gewinn' if streak_aktuell > 0 else 'Verlust'}</b>\n"
                               f"⏰ Beste Zeit: <b>{beste_h_text}</b>\n"
                               f"━━━━━━━━━━━━━━━━━━━━\n"
                               + analysiere_tageszeit()[:200] + "\n"
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

                elif text == "/erklaer":
                    sende_cmd_hilfe(chat_id, "/erklaer")

                elif text.startswith("/erklaer "):
                    bot_name = text.replace("/erklaer ", "").strip().lower()
                    erkl = SIGNAL_ERKLAERUNGEN.get(bot_name)
                    if erkl:
                        antwort = erkl
                    else:
                        verfuegbar = ", ".join(SIGNAL_ERKLAERUNGEN.keys())
                        antwort = f"❓ Unbekannt. Verfügbar: {verfuegbar}"
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                  json={"chat_id": chat_id, "text": antwort, "parse_mode": "HTML"}, timeout=10)

                elif text == "/gegner":
                    antwort = analysiere_gegner() or "Noch zu wenig Daten (mind. 30 Tipps)"
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                  json={"chat_id": chat_id, "text": antwort, "parse_mode": "HTML"}, timeout=10)

                elif text.startswith("/start inv_"):
                    einlader_id = text.replace("/start inv_", "").strip()
                    if einlader_id != user_id:
                        einlader_daten = _community_system["einladungen"].get(einlader_id, {})
                        einlader_name  = einlader_daten.get("name", "Unbekannt")
                        registriere_einladung(einlader_id, einlader_name, user_id, username)
                        antwort = (f"👋 <b>Willkommen, {username}!</b>\n"
                                   f"Du wurdest von <b>{einlader_name}</b> eingeladen.\n"
                                   f"Schreib /start für das Hauptmenü!")
                        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                      json={"chat_id": chat_id, "text": antwort, "parse_mode": "HTML"}, timeout=10)
                    sende_hauptmenu(chat_id)

                elif text in ("/start", "/menu"):
                    sende_hauptmenu(chat_id)
                    continue

                elif text == "/effizienz":
                    eff = berechne_markt_effizienz()
                    if not eff.get("ausreichend"):
                        antwort = "⚠️ Noch zu wenig Value-Tipps (mind. 10)"
                    else:
                        zeilen = []
                        for kat, d in eff.get("kategorien", {}).items():
                            kat_labels = {"sehr_gut": "💎 Sehr gut (EV≥15%)",
                                          "gut": "✅ Gut (EV 8-15%)",
                                          "grenzwertig": "🟡 Grenzwertig (EV 0-8%)"}
                            zeilen.append(f"{kat_labels.get(kat,kat)}: {d['pct']}% | ROI {d['roi']:+.1f}% ({d['tipps']} Tipps)")
                        antwort = "📊 <b>Wettmarkt-Effizienz</b>\n━━━━━━━━━━━━━━━━━━━━\n" + "\n".join(zeilen)
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                  json={"chat_id": chat_id, "text": antwort, "parse_mode": "HTML"}, timeout=10)

                elif text == "/compound":
                    sim = simuliere_compound_bankroll(wochen=20)
                    if not sim.get("ausreichend"):
                        antwort = "⚠️ Noch zu wenig Daten (mind. 10 Tipps)"
                    else:
                        verlauf_text = " → ".join([f"{v}€" for v in sim["verlauf"][::4]])
                        antwort = (f"📈 <b>Compound Bankroll Simulation</b>\n"
                                   f"━━━━━━━━━━━━━━━━━━━━\n"
                                   f"💰 Start: <b>{sim['start']}€</b>\n"
                                   f"🎯 Trefferquote: <b>{sim['trefferquote']}%</b>\n"
                                   f"📐 Kelly-Einsatz: <b>{sim['kelly_pct']}% pro Tipp</b>\n"
                                   f"━━━━━━━━━━━━━━━━━━━━\n"
                                   f"Nach 20 Wochen: <b>{sim['end']}€</b>\n"
                                   f"📈 Rendite: <b>{sim['rendite_pct']:+.1f}%</b>\n"
                                   f"📊 Verlauf: {verlauf_text}")
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                  json={"chat_id": chat_id, "text": antwort, "parse_mode": "HTML"}, timeout=10)

                elif text == "/export":
                    pfad = erstelle_excel_export()
                    if pfad:
                        try:
                            with open(pfad, "rb") as f:
                                requests.post(
                                    f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument",
                                    data={"chat_id": chat_id, "caption": f"📊 Daten-Export – letzte 30 Tage"},
                                    files={"document": f}, timeout=30
                                )
                        except Exception as e:
                            requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                          json={"chat_id": chat_id, "text": f"⚠️ Export Fehler: {e}"}, timeout=10)
                    else:
                        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                      json={"chat_id": chat_id, "text": "⚠️ openpyxl nicht installiert: pip install openpyxl"}, timeout=10)

                elif text == "/suche":
                    sende_cmd_hilfe(chat_id, "/suche")

                elif text.startswith("/suche "):
                    suchbegriff = text.replace("/suche ", "").strip()
                    antwort     = suche_signale(suchbegriff)
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                  json={"chat_id": chat_id, "text": antwort, "parse_mode": "HTML"}, timeout=10)

                elif text == "/clustering":
                    cluster = analysiere_tipp_clustering()
                    if cluster:
                        zeilen = [f"🏆 {k}: {v['quote']}% ({v['tipps']} Tipps)" for k, v in cluster.items()]
                        antwort = "🧩 <b>Tipp-Clustering</b>\n━━━━━━━━━━━━━━━━━━━━\n" + "\n".join(zeilen)
                    else:
                        antwort = "⚠️ Noch zu wenig Daten (mind. 50 Tipps)"
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                  json={"chat_id": chat_id, "text": antwort, "parse_mode": "HTML"}, timeout=10)

                elif text in ("/filter", "/signale", "/filter_off", "/filter_on"):
                    an   = [f"✅ <b>{n}</b>  →  <code>/filter_off {k}</code>"
                            for k, n in TELEGRAM_BOT_NAMEN.items() if k not in _telegram_deaktiviert]
                    aus  = [f"❌ <b>{n}</b>  →  <code>/filter_on {k}</code>"
                            for k, n in TELEGRAM_BOT_NAMEN.items() if k in _telegram_deaktiviert]
                    antwort = (
                        f"📱 <b>Signal-Filter</b>\n"
                        f"━━━━━━━━━━━━━━━━━━━━\n"
                        f"Hier kannst du einzelne Signale\n"
                        f"für Telegram an- oder ausschalten.\n"
                        f"Discord bekommt immer alle Signale.\n"
                        f"━━━━━━━━━━━━━━━━━━━━\n"
                        f"🟢 <b>Aktiv ({len(an)}):</b>\n" + "\n".join(an) +
                        (f"\n\n🔴 <b>Deaktiviert ({len(aus)}):</b>\n" + "\n".join(aus) if aus else "") +
                        f"\n━━━━━━━━━━━━━━━━━━━━\n"
                        f"Alles aus: <code>/filter_off alle</code>\n"
                        f"Alles an:  <code>/filter_on alle</code>"
                    )
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                  json={"chat_id": chat_id, "text": antwort, "parse_mode": "HTML"}, timeout=10)

                elif text.startswith("/filter_off "):
                    key = text.replace("/filter_off ", "").strip().lower()
                    if key == "alle":
                        _telegram_deaktiviert.update(TELEGRAM_BOT_NAMEN.keys())
                        telegram_filter_speichern()
                        antwort = (f"❌ <b>Alle Signale für Telegram deaktiviert</b>\n"
                                   f"━━━━━━━━━━━━━━━━━━━━\n"
                                   f"📱 Telegram: Keine Signale mehr\n"
                                   f"🖥️ Discord: Läuft weiter wie gewohnt\n"
                                   f"━━━━━━━━━━━━━━━━━━━━\n"
                                   f"Wieder aktivieren: <code>/filter_on alle</code>")
                    elif key in TELEGRAM_BOT_NAMEN:
                        _telegram_deaktiviert.add(key)
                        telegram_filter_speichern()
                        antwort = (f"❌ <b>{TELEGRAM_BOT_NAMEN[key]} deaktiviert</b>\n"
                                   f"━━━━━━━━━━━━━━━━━━━━\n"
                                   f"📱 Telegram: Keine {TELEGRAM_BOT_NAMEN[key]} Signale\n"
                                   f"🖥️ Discord: Läuft weiter wie gewohnt\n"
                                   f"━━━━━━━━━━━━━━━━━━━━\n"
                                   f"Wieder aktivieren: <code>/filter_on {key}</code>")
                    else:
                        antwort = (f"❓ <b>Key nicht gefunden: {key}</b>\n"
                                   f"Schreib <code>/filter</code> um alle Keys zu sehen")
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                  json={"chat_id": chat_id, "text": antwort, "parse_mode": "HTML"}, timeout=10)

                elif text.startswith("/filter_on "):
                    key = text.replace("/filter_on ", "").strip().lower()
                    if key == "alle":
                        _telegram_deaktiviert.clear()
                        telegram_filter_speichern()
                        antwort = (f"✅ <b>Alle Signale für Telegram aktiviert</b>\n"
                                   f"📱 Telegram + 🖥️ Discord erhalten wieder alle Signale")
                    elif key in TELEGRAM_BOT_NAMEN:
                        _telegram_deaktiviert.discard(key)
                        telegram_filter_speichern()
                        antwort = (f"✅ <b>{TELEGRAM_BOT_NAMEN[key]} aktiviert</b>\n"
                                   f"📱 Telegram + 🖥️ Discord erhalten wieder {TELEGRAM_BOT_NAMEN[key]} Signale")
                    else:
                        antwort = (f"❓ <b>Key nicht gefunden: {key}</b>\n"
                                   f"Schreib <code>/filter</code> um alle Keys zu sehen")
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                  json={"chat_id": chat_id, "text": antwort, "parse_mode": "HTML"}, timeout=10)

                elif text in ("/checkin", "/check"):
                    antwort = mache_daily_checkin(user_id, username)
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                  json={"chat_id": chat_id, "text": antwort, "parse_mode": "HTML"}, timeout=10)

                elif text in ("/rang", "/level", "/xp"):
                    daten = _rang_daten.get(user_id)
                    if not daten:
                        antwort = f"🆕 Du bist noch nicht im System.\nNutze /checkin zum Starten!"
                    else:
                        lv  = daten.get("level", 1)
                        lv_name = daten.get("level_name", "🆕 Newcomer")
                        xp  = daten.get("xp", 0)
                        _, _, bis = berechne_level(xp)
                        gw  = daten.get("gewinne", 0)
                        vl  = daten.get("verluste", 0)
                        streak = daten.get("streak", 0)
                        pct = round(gw/max(gw+vl,1)*100)
                        # XP-Balken
                        balken_len = 10
                        if bis > 0:
                            aktuell_level_xp = next((s[0] for s in LEVEL_STUFEN if s[1]==lv), 0)
                            naechstes_xp = aktuell_level_xp + bis
                            fortschritt = int((xp - aktuell_level_xp) / max(bis, 1) * balken_len)
                        else:
                            fortschritt = balken_len
                        balken = "█" * fortschritt + "░" * (balken_len - fortschritt)
                        antwort = (f"🏅 <b>{username}s Rang-Profil</b>\n"
                                   f"━━━━━━━━━━━━━━━━━━━━\n"
                                   f"🎯 Level: <b>{lv} – {lv_name}</b>\n"
                                   f"⭐ XP: <b>{xp:,}</b>\n"
                                   f"[{balken}] {'' if bis==0 else f'noch {bis:,} XP'}\n"
                                   f"━━━━━━━━━━━━━━━━━━━━\n"
                                   f"✅ Gewinne: <b>{gw}</b> | ❌ Verluste: <b>{vl}</b>\n"
                                   f"🎯 Quote: <b>{pct}%</b>\n"
                                   f"🔥 Streak: <b>{streak}</b>\n"
                                   f"━━━━━━━━━━━━━━━━━━━━\n"
                                   f"📅 /checkin für täglich +{XP_QUELLEN['daily_checkin']} XP")
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                  json={"chat_id": chat_id, "text": antwort, "parse_mode": "HTML"}, timeout=10)

                elif text == "/xprangliste":
                    antwort = f"🏆 <b>XP Rangliste</b>\n━━━━━━━━━━━━━━━━━━━━\n{xp_rangliste()}"
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                  json={"chat_id": chat_id, "text": antwort, "parse_mode": "HTML"}, timeout=10)

                elif text == "/features":
                    inv_data = _community_system["einladungen"].get(user_id, {})
                    inv_count = inv_data.get("count", 0)
                    feat_text = invite_features_text(inv_count)
                    antwort = (f"🔓 <b>Deine freigeschalteten Features</b>\n"
                               f"━━━━━━━━━━━━━━━━━━━━\n"
                               f"👥 Einladungen: <b>{inv_count}</b>\n"
                               f"━━━━━━━━━━━━━━━━━━━━\n"
                               f"{feat_text}\n"
                               f"━━━━━━━━━━━━━━━━━━━━\n"
                               f"📤 Dein Link: <code>t.me/frums72bot?start=inv_{user_id}</code>")
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                  json={"chat_id": chat_id, "text": antwort, "parse_mode": "HTML"}, timeout=10)

                elif text == "/meinestatistik":
                    daten = _community_system["rollen"].get(user_id)
                    if not daten:
                        antwort = (f"📊 <b>Deine Statistik</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                                   f"Du hast noch keine Community-Tipps abgegeben.\n"
                                   f"Nutze /tipp um anzufangen!")
                    else:
                        ges  = daten["tipps"]
                        gw   = daten["gewinne"]
                        vl   = daten.get("verluste", 0)
                        pct  = round(gw / max(ges, 1) * 100)
                        rolle = daten.get("rolle", "🆕 Neuling")
                        streak = daten.get("streak", 0)
                        beste  = daten.get("beste_streak", 0)
                        # Nächste Rolle
                        naechste = ""
                        for min_g, name, _ in ROLLEN:
                            if gw < min_g:
                                naechste = f"\n🎯 Noch {min_g - gw} Gewinne bis {name}"
                                break
                        antwort = (f"📊 <b>Deine Statistik, {username}</b>\n"
                                   f"━━━━━━━━━━━━━━━━━━━━\n"
                                   f"🏅 Rolle: <b>{rolle}</b>\n"
                                   f"━━━━━━━━━━━━━━━━━━━━\n"
                                   f"✅ Gewonnen: <b>{gw}</b>\n"
                                   f"❌ Verloren: <b>{vl}</b>\n"
                                   f"🎯 Trefferquote: <b>{pct}%</b>\n"
                                   f"🔥 Aktueller Streak: <b>{streak}</b>\n"
                                   f"⭐ Bester Streak: <b>{beste}</b>{naechste}")
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                  json={"chat_id": chat_id, "text": antwort, "parse_mode": "HTML"}, timeout=10)

                elif text == "/einladungen":
                    daten = _community_system["einladungen"].get(user_id)
                    if not daten:
                        antwort = (f"🎁 <b>Einladungs-Programm</b>\n"
                                   f"━━━━━━━━━━━━━━━━━━━━\n"
                                   f"Du hast noch niemanden eingeladen.\n\n"
                                   f"📤 Dein Einladungslink:\n"
                                   f"<code>t.me/frums72bot?start=inv_{user_id}</code>\n\n"
                                   f"🎁 Belohnungen ab 1 Einladung!")
                    else:
                        count  = daten["count"]
                        belohn = daten.get("belohnungen", [])
                        # Nächste Belohnung
                        naechste_key = min([k for k in INVITE_BELOHNUNGEN if k > count] or [999])
                        naechste_txt = f"\n🎯 Noch {naechste_key - count} bis: {INVITE_BELOHNUNGEN.get(naechste_key,'–')}" if naechste_key < 999 else ""
                        antwort = (f"🎁 <b>Deine Einladungen, {username}</b>\n"
                                   f"━━━━━━━━━━━━━━━━━━━━\n"
                                   f"👥 Eingeladen: <b>{count}</b> Personen{naechste_txt}\n"
                                   f"━━━━━━━━━━━━━━━━━━━━\n"
                                   f"🏆 Verdiente Belohnungen:\n"
                                   + ("\n".join([f"✅ {b}" for b in belohn]) if belohn else "Noch keine") +
                                   f"\n━━━━━━━━━━━━━━━━━━━━\n"
                                   f"📤 Dein Link:\n<code>t.me/frums72bot?start=inv_{user_id}</code>")
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                  json={"chat_id": chat_id, "text": antwort, "parse_mode": "HTML"}, timeout=10)

                elif text == "/rollenrangliste":
                    rollen_data = _community_system.get("rollen", {})
                    sortiert = sorted(rollen_data.items(),
                                      key=lambda x: x[1].get("gewinne", 0), reverse=True)[:10]
                    if not sortiert:
                        antwort = "Noch keine Einträge. Nutze /tipp um anzufangen!"
                    else:
                        medals = ["🥇","🥈","🥉"]
                        zeilen = []
                        for i, (uid, d) in enumerate(sortiert):
                            m   = medals[i] if i < 3 else f"{i+1}."
                            pct = round(d.get("gewinne",0)/max(d.get("tipps",1),1)*100)
                            zeilen.append(f"{m} {d.get('name','?')} | {d.get('gewinne',0)}W | {pct}% | {d.get('rolle','🆕')}")
                        antwort = ("🏆 <b>Community Rollenrangliste</b>\n"
                                   "━━━━━━━━━━━━━━━━━━━━\n" + "\n".join(zeilen))
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                  json={"chat_id": chat_id, "text": antwort, "parse_mode": "HTML"}, timeout=10)

                elif text.startswith("/challenge"):
                    if "join" in text:
                        monat = de_now().strftime("%Y-%m")
                        if monat not in _community_system["challenges"]:
                            sende_monatliche_challenge()
                        _community_system["challenges"][monat]["teilnehmer"][user_id] = {
                            "name": username, "beigetreten": jetzt()
                        }
                        community_system_speichern()
                        antwort = (f"✅ <b>Du nimmst an der Challenge teil!</b>\n"
                                   f"Dokumentiere deine Tipps mit /tipp\n"
                                   f"Viel Erfolg! 🎯")
                    else:
                        monat = de_now().strftime("%Y-%m")
                        teilnehmer = _community_system["challenges"].get(monat, {}).get("teilnehmer", {})
                        antwort = (f"🏆 <b>Monatliche Challenge – {de_now().strftime('%B %Y')}</b>\n"
                                   f"━━━━━━━━━━━━━━━━━━━━\n"
                                   f"👥 Teilnehmer: <b>{len(teilnehmer)}</b>\n\n"
                                   f"💰 Wer macht aus 10€ am meisten?\n\n"
                                   f"🎁 Preise: Premium, Bot-Analyse, VIP\n"
                                   f"━━━━━━━━━━━━━━━━━━━━\n"
                                   f"Mitmachen: <code>/challenge join</code>")
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                  json={"chat_id": chat_id, "text": antwort, "parse_mode": "HTML"}, timeout=10)

                elif text == "/claude":
                    heute_str = de_now().strftime("%Y-%m-%d")
                    if _claude_calls_datum != heute_str:
                        calls = 0
                    else:
                        calls = _claude_calls_heute
                    rest  = max(0, CLAUDE_MAX_PRO_TAG - calls)
                    antwort = (f"🤖 <b>Claude API Budget</b>\n"
                               f"━━━━━━━━━━━━━━━━━━━━\n"
                               f"📊 Heute genutzt: <b>{calls}/{CLAUDE_MAX_PRO_TAG}</b>\n"
                               f"✅ Noch verfügbar: <b>{rest} Calls</b>\n"
                               f"🏆 Top-Ligen: {len(TOP_LIGEN_CLAUDE)}\n"
                               f"━━━━━━━━━━━━━━━━━━━━\n"
                               f"{'✅ Budget verfügbar' if rest > 0 else '❌ Limit erreicht – Fallback aktiv'}\n"
                               f"🕐 {jetzt()} Uhr")
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                  json={"chat_id": chat_id, "text": antwort, "parse_mode": "HTML"}, timeout=10)

                elif text == "/kalibrierung":
                    kal = kalibriere_konfidenz()
                    if kal:
                        zeilen = [f"Konfidenz {k}/10: erwartet {v['erwartet']}% | echt {v['echt']}% (Δ{v['abweichung']:+}%)"
                                  for k, v in sorted(kal.items())]
                        antwort = "🧠 <b>Konfidenz-Kalibrierung</b>\n━━━━━━━━━━━━━━━━━━━━\n" + "\n".join(zeilen)
                    else:
                        antwort = "⚠️ Noch zu wenig Daten (mind. 100 ausgewertete Tipps)"
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                  json={"chat_id": chat_id, "text": antwort, "parse_mode": "HTML"}, timeout=10)

                elif text == "/whitelist":
                    sende_cmd_hilfe(chat_id, "/whitelist")
                    continue

                elif text.startswith("/whitelist "):
                    teile_wl = text.split(" ", 2)
                    if len(teile_wl) == 1:
                        ligen_wl = ", ".join(_whitelist.get("ligen", [])) or "–"
                        teams_wl = ", ".join(_whitelist.get("teams", [])) or "–"
                        aktiv_wl = "✅ Aktiv" if _whitelist.get("aktiv") else "❌ Inaktiv"
                        antwort  = (f"📋 <b>Whitelist Status: {aktiv_wl}</b>\n"
                                    f"🏆 Ligen: {ligen_wl}\n👥 Teams: {teams_wl}\n"
                                    f"Befehle:\n/whitelist on – aktivieren\n/whitelist off – deaktivieren\n"
                                    f"/whitelist liga [Name] – Liga hinzufügen\n/whitelist team [Name] – Team hinzufügen\n/whitelist reset – leeren")
                    elif teile_wl[1] == "on":
                        _whitelist["aktiv"] = True
                        whitelist_speichern()
                        antwort = "✅ Whitelist aktiviert"
                    elif teile_wl[1] == "off":
                        _whitelist["aktiv"] = False
                        whitelist_speichern()
                        antwort = "❌ Whitelist deaktiviert"
                    elif teile_wl[1] == "reset":
                        _whitelist["ligen"] = []
                        _whitelist["teams"] = []
                        whitelist_speichern()
                        antwort = "🗑️ Whitelist geleert"
                    elif teile_wl[1] == "liga" and len(teile_wl) > 2:
                        _whitelist.setdefault("ligen", []).append(teile_wl[2])
                        whitelist_speichern()
                        antwort = f"✅ Liga hinzugefügt: {teile_wl[2]}"
                    elif teile_wl[1] == "team" and len(teile_wl) > 2:
                        _whitelist.setdefault("teams", []).append(teile_wl[2])
                        whitelist_speichern()
                        antwort = f"✅ Team hinzugefügt: {teile_wl[2]}"
                    else:
                        antwort = "Benutzung: /whitelist [on|off|liga|team|reset]"
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                  json={"chat_id": chat_id, "text": antwort, "parse_mode": "HTML"}, timeout=10)

                elif text == "/tipp":
                    sende_cmd_hilfe(chat_id, "/tipp")

                elif text.startswith("/tipp "):
                    teile_t = msg_obj.get("text","").strip().split(" ", 3)
                    if len(teile_t) >= 4:
                        spiel_t = teile_t[1]
                        bet_t   = teile_t[2]
                        quote_t = teile_t[3] if len(teile_t) > 3 else None
                        tipp_obj = {
                            "typ": "manuell", "home": spiel_t, "away": "",
                            "tipp": bet_t, "quote": float(quote_t) if quote_t else None,
                            "status": "offen", "signal_zeit": time.time(),
                            "versuche": 0, "letzter_versuch": 0,
                        }
                        _manuell_tipps.append(tipp_obj)
                        manuell_tipps_speichern()
                        antwort = (f"✅ <b>Manueller Tipp gespeichert!</b>\n"
                                   f"⚽ Spiel: <b>{spiel_t}</b>\n"
                                   f"🎯 Tipp: <b>{bet_t}</b>\n"
                                   f"💶 Quote: <b>{quote_t or 'keine'}</b>\n"
                                   f"Ergebnis mit: /gewonnen oder /verloren")
                    else:
                        antwort = "Benutzung: /tipp [Spiel] [Bet] [Quote]\nBeispiel: /tipp ManCity Über2.5 1.85"
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                  json={"chat_id": chat_id, "text": antwort, "parse_mode": "HTML"}, timeout=10)

                elif text in ("/gewonnen", "/verloren"):
                    if _manuell_tipps:
                        letzter = _manuell_tipps[-1]
                        letzter["status"]   = "ausgewertet"
                        letzter["gewonnen"] = text == "/gewonnen"
                        manuell_tipps_speichern()
                        typ_m = "torwart"  # Default für Statistik
                        update_statistik(typ_m, letzter["gewonnen"], letzter.get("quote"))
                        antwort = f"{'✅ GEWONNEN' if letzter['gewonnen'] else '❌ VERLOREN'} – {letzter['tipp']} ({letzter['home']})"
                    else:
                        antwort = "Kein offener Tipp gefunden"
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                  json={"chat_id": chat_id, "text": antwort, "parse_mode": "HTML"}, timeout=10)

                elif text == "/chart":
                    pfad = erstelle_performance_chart()
                    if pfad:
                        sende_chart_telegram(pfad)
                    else:
                        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                      json={"chat_id": chat_id, "text": "⚠️ Noch zu wenig Daten für Chart", "parse_mode": "HTML"}, timeout=10)

                elif text == "/api":
                    antwort = f"📡 <b>API Monitor</b>\n━━━━━━━━━━━━━━━━━━━━\n{api_monitor_bericht()}\n━━━━━━━━━━━━━━━━━━━━\n🕐 {jetzt()} Uhr"
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                  json={"chat_id": chat_id, "text": antwort, "parse_mode": "HTML"}, timeout=10)

                elif text == "/addadmin":
                    if ist_admin(chat_id):
                        sende_cmd_hilfe(chat_id, "/addadmin")

                elif text.startswith("/addadmin ") and ist_admin(chat_id):
                    neuer_id = text.replace("/addadmin ", "").strip()
                    if neuer_id not in [str(a) for a in ADMIN_IDS]:
                        ADMIN_IDS.append(neuer_id)
                        admins_speichern()
                        antwort = f"✅ Admin hinzugefügt: {neuer_id}"
                    else:
                        antwort = "⚠️ Diese ID ist bereits Admin"
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                  json={"chat_id": chat_id, "text": antwort, "parse_mode": "HTML"}, timeout=10)

                elif text.startswith("/removeadmin ") and ist_admin(chat_id):
                    rem_id = text.replace("/removeadmin ", "").strip()
                    if rem_id in [str(a) for a in ADMIN_IDS] and rem_id != str(TELEGRAM_CHAT_ID):
                        ADMIN_IDS.remove(rem_id)
                        admins_speichern()
                        antwort = f"🗑️ Admin entfernt: {rem_id}"
                    else:
                        antwort = "⚠️ ID nicht gefunden oder Haupt-Admin"
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                  json={"chat_id": chat_id, "text": antwort, "parse_mode": "HTML"}, timeout=10)

                elif text in ("/auswertung", "/status_auswertung"):
                    offene     = tracker_get_offene()
                    alle       = list(_signal_tracker.values())
                    ausgewertet = [s for s in alle if s.get("status") == "ausgewertet"]
                    gewonnen_t  = [s for s in ausgewertet if s.get("gewonnen")]
                    verloren_t  = [s for s in ausgewertet if not s.get("gewonnen")]
                    pct = round(len(gewonnen_t) / len(ausgewertet) * 100) if ausgewertet else 0
                    offen_liste = "\n".join([
                        f"  • {s.get('home','?')} vs {s.get('away','?')} ({s.get('typ','')})"
                        for _, s in offene[:5]
                    ]) or "  Keine offenen Signale"
                    antwort = (f"📋 <b>Signal-Tracker Status</b>\n"
                               f"━━━━━━━━━━━━━━━━━━━━\n"
                               f"📨 Gesamt Signale: <b>{len(alle)}</b>\n"
                               f"✅ Ausgewertet: <b>{len(ausgewertet)}</b>\n"
                               f"⏳ Noch offen: <b>{len(offene)}</b>\n"
                               f"🎯 Trefferquote: <b>{pct}%</b> ({len(gewonnen_t)}W/{len(verloren_t)}L)\n"
                               f"━━━━━━━━━━━━━━━━━━━━\n"
                               f"⏳ <b>Offene Signale:</b>\n{offen_liste}\n"
                               f"━━━━━━━━━━━━━━━━━━━━\n🕐 {jetzt()} Uhr")
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

PREMATCH_TOP_LIGEN = {
    "Premier League", "Bundesliga", "La Liga", "Serie A", "Ligue 1",
    "Champions League", "Europa League", "Conference League",
    "Eredivisie", "Primeira Liga", "Super Lig", "Russian Premier League",
    "Belgian Pro League", "Scottish Premiership", "Championship",
    "Bundesliga 2", "Serie B", "2. Bundesliga", "Ligue 2",
    "DFB-Pokal", "FA Cup", "Copa del Rey", "Coppa Italia",
    "World Cup", "European Championship", "Nations League",
    "MLS", "Brasileirão", "Argentine Primera Division",
}

def filtere_top_spiele(fixtures: list) -> list:
    """Filtert nur Spiele aus Top-Ligen."""
    top = []
    for f in fixtures:
        liga = f.get("competition", {}).get("name", "").lower()
        if any(l in liga for l in PREMATCH_LIGEN):
            top.append(f)
    return top

def claude_prematch_analyse(home: str, away: str, liga: str, anstoß: str,
                             bereits_tipps: list = None) -> dict | None:
    """
    Pre-Match Analyse via Claude – max. 3x pro Tag, nur Top-Ligen.
    """
    bereits = ", ".join(bereits_tipps) if bereits_tipps else "keine"
    if not ANTHROPIC_API_KEY or not ANTHROPIC_API_KEY.strip() or        not claude_budget_verfuegbar(liga):
        # Fallback ohne API oder Budget: abwechselnde Tipps
        import random
        fallbacks = [
            {"tipp": "Über 2.5 Tore",    "analyse": "Beide Teams sind offensivstark und spielen aktiv nach vorne.", "konfidenz": 6},
            {"tipp": "Heimsieg",          "analyse": "Der Heimvorteil spielt eine entscheidende Rolle bei diesem Duell.", "konfidenz": 6},
            {"tipp": "Unter 2.5 Tore",   "analyse": "Beide Defensivreihen sind stabil und lassen wenig zu.", "konfidenz": 5},
            {"tipp": "Beide Teams treffen","analyse": "Beide Mannschaften kommen regelmäßig zu Torabschlüssen.", "konfidenz": 6},
            {"tipp": "Doppelte Chance 1X","analyse": "Der Gastgeber ist leichter Favorit, ein Unentschieden ist aber möglich.", "konfidenz": 7},
        ]
        verfuegbar = [f for f in fallbacks if f["tipp"] not in (bereits_tipps or [])]
        return random.choice(verfuegbar) if verfuegbar else random.choice(fallbacks)
    try:
        prompt = (
            f"Du bist ein erfahrener Sportwetten-Analyst. Analysiere dieses Spiel basierend auf deinem Wissen über diese Teams.\n\n"
            f"Spiel: {home} vs {away}\n"
            f"Liga: {liga}\n"
            f"Anstoß: {anstoß} Uhr\n\n"
            f"Bereits gewählte Tipps in diesem Post (NICHT wiederholen): {bereits}\n\n"
            f"Wähle den WAHRSCHEINLICHSTEN Tipp für dieses konkrete Spiel.\n"
            f"Berücksichtige: aktuelle Form, Heimvorteil, Defensivstärke, Offensivstärke, direkte Duelle.\n"
            f"Erlaubte Tipp-Typen (EINER davon):\n"
            f"- Über 2.5 Tore\n"
            f"- Unter 2.5 Tore\n"
            f"- Über 1.5 Tore\n"
            f"- Unter 1.5 Tore HZ1\n"
            f"- Über 0.5 Tore HZ1\n"
            f"- Beide Teams treffen\n"
            f"- Keine Beide Teams treffen\n"
            f"- Heimsieg\n"
            f"- Auswärtssieg\n"
            f"- Doppelte Chance 1X\n"
            f"- Doppelte Chance X2\n"
            f"- Unentschieden\n\n"
            f"Antworte NUR so:\n"
            f"TIPP: [Tipp-Typ]\n"
            f"KONFIDENZ: [1-10]\n"
            f"ANALYSE: [2-3 konkrete Sätze warum, spezifisch auf das Spiel bezogen]"
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
                "max_tokens": 200,
                "messages": [{"role": "user", "content": prompt}]
            },
            timeout=20
        )
        if resp.status_code != 200:
            return None
        text         = resp.json().get("content", [{}])[0].get("text", "").strip()
        tipp         = ""
        analyse_text = ""
        konfidenz    = 6
        for line in text.split("\n"):
            if line.startswith("TIPP:"):
                tipp = line.replace("TIPP:", "").strip()
            elif line.startswith("ANALYSE:"):
                analyse_text = line.replace("ANALYSE:", "").strip()
            elif line.startswith("KONFIDENZ:"):
                try:
                    konfidenz = int(line.replace("KONFIDENZ:", "").strip())
                except Exception:
                    konfidenz = 6
        if not tipp:
            return None
        return {"tipp": tipp, "analyse": analyse_text, "konfidenz": konfidenz}
    except Exception as e:
        print(f"  [PreMatch] Claude Fehler: {e}")
        return None

def bot_prematch():
    """Sendet automatisch Pre-Match Tipps um 10:00, 16:00 und 20:00 Uhr."""
    import random
    print(f"[PreMatch-Bot] Gestartet | Posts um {PREMATCH_UHRZEITEN} Uhr")
    gesendet = set()

    while True:
        try:
            now   = de_now()
            key   = f"{now.strftime('%Y-%m-%d')}_{now.hour}"
            datum = now.strftime("%Y-%m-%d")

            if now.hour in PREMATCH_UHRZEITEN and now.minute < 5 and key not in gesendet:
                print(f"  [PreMatch-Bot] Starte Post um {now.hour}:00 Uhr")

                fixtures = ls_get_fixtures(datum)
                top      = filtere_top_spiele(fixtures)

                # Zeitfenster-Filter je nach Uhrzeit
                def anstoß_stunde(spiel):
                    try:
                        return int(spiel.get("time", "0:0").split(":")[0])
                    except Exception:
                        return 0

                if now.hour == 10:
                    # 10 Uhr: nur Spiele die vor 16 Uhr starten
                    top = [s for s in top if anstoß_stunde(s) < 16]
                elif now.hour == 16:
                    # 16 Uhr: nur Spiele zwischen 16 und 20 Uhr
                    top = [s for s in top if 16 <= anstoß_stunde(s) < 20]
                elif now.hour == 20:
                    # 20 Uhr: nur Spiele ab 20 Uhr
                    top = [s for s in top if anstoß_stunde(s) >= 20]

                if not top:
                    print(f"  [PreMatch-Bot] Keine passenden Spiele für {now.hour}:00 Uhr")
                    gesendet.add(key)
                    time.sleep(60)
                    continue

                auswahl  = random.sample(top, min(PREMATCH_MAX_TIPPS, len(top)))
                analysen = []
                bereits_tipps = []

                for spiel in auswahl:
                    home    = (spiel.get("home_name") or spiel.get("home", {}).get("name", "?"))
                    away    = (spiel.get("away_name") or spiel.get("away", {}).get("name", "?"))
                    liga    = spiel.get("competition", {}).get("name", "?")
                    country = (spiel.get("country") or {}).get("name", "")
                    anstoß  = spiel.get("time", "?")

                    # Bereits verwendete Tipps übergeben damit Claude variiert
                    result = claude_prematch_analyse(home, away, liga, anstoß, bereits_tipps)
                    if not result:
                        continue
                    bereits_tipps.append(result["tipp"])
                    match_id_fix = str(spiel.get("id", spiel.get("fixture_id", "")))
                    verletzung   = verletzungs_check(match_id_fix, home, away) if match_id_fix else ""
                    analysen.append({
                        "home": home, "away": away,
                        "liga": liga, "country": country,
                        "anstoß": anstoß,
                        "tipp": result["tipp"],
                        "analyse": result["analyse"],
                        "konfidenz": result.get("konfidenz", 6),
                        "verletzung": verletzung,
                    })
                    time.sleep(1)

                if analysen:
                    uhr_emoji = "🌅" if now.hour == 10 else ("🌆" if now.hour == 16 else "🌙")
                    msg = (f"{uhr_emoji} <b>Pre-Match Tipps – {now.strftime('%d.%m.%Y')}</b>\n"
                           f"━━━━━━━━━━━━━━━━━━━━\n"
                           f"🤖 KI-Analyse powered by BetlabLIVE\n"
                           f"━━━━━━━━━━━━━━━━━━━━\n\n")

                    for i, a in enumerate(analysen, 1):
                        liga_str   = f"{a['liga']}" + (f" ({a['country']})" if a['country'] else "")
                        verletzung = f"\n{a['verletzung']}" if a.get('verletzung') else ""
                        ke         = konfidenz_emoji(a['konfidenz'])
                        msg += (f"🏆 <b>{liga_str}</b>\n"
                                f"⚽ <b>{a['home']} vs {a['away']}</b>\n"
                                f"🕐 Anstoß: <b>{a['anstoß']} Uhr</b>\n"
                                f"🎯 Tipp: <b>{a['tipp']}</b>\n"
                                f"{ke} Konfidenz: <b>{a['konfidenz']}/10</b>\n"
                                f"📊 {a['analyse']}{verletzung}\n")
                        if i < len(analysen):
                            msg += "\n━━━━━━━━━━━━━━━━━━━━\n\n"

                    msg += (f"\n━━━━━━━━━━━━━━━━━━━━\n"
                            f"💬 Community & Live-Bots: discord.gg/G6dt3Kpf\n"
                            f"⚠️ 18+ | Verantwortungsvoll spielen")

                    send_telegram_gruppe(msg)
                    print(f"  [PreMatch-Bot] ✅ {len(analysen)} Tipps um {now.hour}:00 Uhr | Tipps: {bereits_tipps}")

                gesendet.add(key)
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
                except Exception:
                    continue
        except Exception as e:
            print(f"  [Erinnerungs-Bot] Fehler: {e}")
        time.sleep(60)

# ============================================================
#  WATCHDOG
# ============================================================

_bot_targets = {}  # thread_name → target_function (wird beim Start befüllt)


# ============================================================
#  VALUE BET FINDER
# ============================================================

notified_value = set()

# Erwartete Wahrscheinlichkeit basierend auf Spielsituation
VALUE_REGELN = [
    # (typ, beschreibung, tipp, mindest_quote, berechne_prob_fn)
    # Karten: Minute 60+, nur 0-1 Karten → Unter 4.5 sehr wahrscheinlich
    {
        "typ": "karten_unter",
        "name": "Karten Unter 4.5",
        "check": lambda s, ev, st: (
            60 <= _safe_int(s.get("time", 0)) <= 80
            and len([e for e in ev if e.get("event") in KARTEN_TYPEN]) <= 1
        ),
        "prob": lambda s, ev, st: 0.82,
        "linie": 4.5,
        "richtung": "unter",
    },
    # Karten: Minute 75+, 3 Karten → Über 4.5 möglich
    {
        "typ": "karten_ueber",
        "name": "Karten Über 4.5",
        "check": lambda s, ev, st: (
            _safe_int(s.get("time", 0)) >= 75
            and len([e for e in ev if e.get("event") in KARTEN_TYPEN]) == 3
        ),
        "prob": lambda s, ev, st: 0.70,
        "linie": 4.5,
        "richtung": "über",
    },
    # Ecken: Minute 70+, unter 7 Ecken gesamt → Unter 9.5 sehr wahrscheinlich
    {
        "typ": "ecken_unter",
        "name": "Ecken Unter 9.5",
        "check": lambda s, ev, st: (
            _safe_int(s.get("time", 0)) >= 70
            and st["corners_home"] + st["corners_away"] <= 6
        ),
        "prob": lambda s, ev, st: 0.80,
        "linie": 9.5,
        "richtung": "unter",
    },
    # Tore: Minute 70+, 0:0 → Unter 1.5 Tore sehr wahrscheinlich
    {
        "typ": "tore_unter",
        "name": "Tore Unter 1.5",
        "check": lambda s, ev, st: (
            _safe_int(s.get("time", 0)) >= 70
            and s.get("scores", {}).get("score", "") in ("0 - 0", "0-0")
        ),
        "prob": lambda s, ev, st: 0.75,
        "linie": 1.5,
        "richtung": "unter",
    },
    # Tore: Minute 60+, 3+ Tore → Über 3.5 sehr wahrscheinlich
    {
        "typ": "tore_ueber",
        "name": "Tore Über 3.5",
        "check": lambda s, ev, st: (
            _safe_int(s.get("time", 0)) >= 60
            and sum(parse_score(s.get("scores", {}).get("score", "0-0"))) >= 3
        ),
        "prob": lambda s, ev, st: 0.78,
        "linie": 3.5,
        "richtung": "über",
    },
    # Ecken: Minute 60+, bereits 10+ Ecken → Über 11.5 wahrscheinlich
    {
        "typ": "ecken_ueber_live",
        "name": "Ecken Über 11.5",
        "check": lambda s, ev, st: (
            _safe_int(s.get("time", 0)) >= 60
            and st["corners_home"] + st["corners_away"] >= 10
        ),
        "prob": lambda s, ev, st: 0.72,
        "linie": 11.5,
        "richtung": "über",
    },
]

def get_live_odds_fuer_spiel(home: str, away: str) -> dict:
    """Holt alle verfügbaren Live-Quoten für ein Spiel."""
    if not ODDS_API_KEY:
        return {}
    try:
        url    = "https://api.the-odds-api.com/v4/sports/soccer/odds/"
        params = {"apiKey": ODDS_API_KEY, "regions": "eu",
                  "markets": "totals,spreads", "oddsFormat": "decimal"}
        resp   = requests.get(url, params=params, timeout=8)
        if resp.status_code != 200:
            return {}
        for game in resp.json():
            h = game.get("home_team", "").lower()
            a = game.get("away_team", "").lower()
            if home.lower()[:5] in h or away.lower()[:5] in a:
                quoten = {}
                for bm in game.get("bookmakers", []):
                    for market in bm.get("markets", []):
                        key = market.get("key", "")
                        for outcome in market.get("outcomes", []):
                            name  = outcome.get("name", "")
                            point = outcome.get("point", "")
                            q     = round(outcome.get("price", 0), 2)
                            k     = f"{key}_{name}_{point}"
                            if k not in quoten or q > quoten[k]["quote"]:
                                quoten[k] = {"quote": q, "name": name,
                                             "point": point, "market": key,
                                             "bookmaker": bm.get("title", "")}
                return quoten
        return {}
    except Exception as e:
        print(f"  [Value] Odds Fehler: {e}")
        return {}

def berechne_value(prob: float, quote: float) -> float:
    """Berechnet den Value Edge: (prob * quote) - 1"""
    return round(prob * quote - 1, 3)

def prüfe_ecken_verfuegbar(home: str, away: str) -> bool:
    """
    Prüft ob Ecken-Wetten bei mindestens 2 Bookmarkern angeboten werden.
    Verhindert Signale bei Ligen wo keine Ecken-Märkte existieren.
    """
    if not ODDS_API_KEY:
        return True  # Ohne API → immer erlauben
    try:
        url    = "https://api.the-odds-api.com/v4/sports/soccer/odds/"
        params = {"apiKey": ODDS_API_KEY, "regions": "eu",
                  "markets": "alternate_totals", "oddsFormat": "decimal"}
        resp   = requests.get(url, params=params, timeout=6)
        if resp.status_code != 200:
            return True  # API-Fehler → Signal trotzdem senden
        home_s = home[:6].lower()
        away_s = away[:6].lower()
        for game in resp.json():
            h = game.get("home_team", "").lower()
            a = game.get("away_team", "").lower()
            if home_s not in h and away_s not in a:
                continue
            # Prüfe ob corners Markt vorhanden
            bm_mit_ecken = 0
            for bm in game.get("bookmakers", []):
                for market in bm.get("markets", []):
                    if "corner" in market.get("key", "").lower():
                        bm_mit_ecken += 1
                        break
            if bm_mit_ecken >= 2:
                return True
            elif bm_mit_ecken == 0:
                print(f"  [Ecken] Kein Ecken-Markt bei Bookmarkern für {home} vs {away} – Signal unterdrückt")
                return False
        return True  # Spiel nicht gefunden → Signal trotzdem senden
    except Exception as e:
        print(f"  [Ecken] Verfügbarkeits-Check Fehler: {e}")
        return True

def vergleiche_quoten_bookmaker(home: str, away: str) -> list:
    """Vergleicht Quoten zwischen Bookmarkern und findet Ausreißer."""
    if not ODDS_API_KEY:
        return []
    try:
        url    = "https://api.the-odds-api.com/v4/sports/soccer/odds/"
        params = {"apiKey": ODDS_API_KEY, "regions": "eu",
                  "markets": "totals", "oddsFormat": "decimal"}
        resp   = requests.get(url, params=params, timeout=8)
        if resp.status_code != 200:
            return []
        ausreisser = []
        for game in resp.json():
            h = game.get("home_team", "").lower()
            a = game.get("away_team", "").lower()
            if home.lower()[:5] not in h and away.lower()[:5] not in a:
                continue
            # Alle Quoten pro Markt sammeln
            markt_quoten = {}
            for bm in game.get("bookmakers", []):
                for market in bm.get("markets", []):
                    for outcome in market.get("outcomes", []):
                        k = f"{outcome.get('name')}_{outcome.get('point', '')}"
                        if k not in markt_quoten:
                            markt_quoten[k] = []
                        markt_quoten[k].append({
                            "quote": outcome.get("price", 0),
                            "bookmaker": bm.get("title", "?"),
                            "name": outcome.get("name"),
                            "point": outcome.get("point", ""),
                        })
            # Ausreißer finden: Quote > 20% über Durchschnitt
            for k, quoten_liste in markt_quoten.items():
                if len(quoten_liste) < 3:
                    continue
                alle_q = [q["quote"] for q in quoten_liste if q["quote"] > 1.0]
                if not alle_q:
                    continue
                avg_q  = sum(alle_q) / len(alle_q)
                max_q  = max(alle_q)
                if max_q < VALUE_BET_MIN_QUOTE:
                    continue
                abweichung = (max_q - avg_q) / avg_q
                if abweichung >= 0.12:  # 12% über Durchschnitt = Ausreißer
                    bester = next(q for q in quoten_liste if q["quote"] == max_q)
                    ausreisser.append({
                        "name": bester["name"],
                        "point": bester["point"],
                        "beste_quote": round(max_q, 2),
                        "avg_quote": round(avg_q, 2),
                        "bookmaker": bester["bookmaker"],
                        "abweichung": round(abweichung * 100, 1),
                    })
        return ausreisser
    except Exception as e:
        print(f"  [Value] Bookmaker-Vergleich Fehler: {e}")
        return []

def bot_value_bet():
    """Sucht nach Value Bets – Quoten-Fehler + Spielsituation."""
    print(f"[Value-Bot] Gestartet | Min. Quote {VALUE_BET_MIN_QUOTE} | Min. Edge {VALUE_BET_MIN_VALUE*100:.0f}%")
    while True:
        try:
            matches = get_live_matches()
            laufend = [m for m in matches if m.get("status") in
                       ("IN PLAY", "ADDED TIME", "HALF TIME BREAK")]
            print(f"[{jetzt()}] [Value-Bot] {len(laufend)} Spiele geprüft")
            for game in laufend:
                match_id = str(game.get("id"))
                home    = game.get("home", {}).get("name", "?")
                away    = game.get("away", {}).get("name", "?")
                comp    = game.get("competition", {}).get("name", "?")
                country = (game.get("country") or {}).get("name", "?")
                minute  = _safe_int(game.get("time", 0))
                score   = game.get("scores", {}).get("score", "?")
                try:
                    events = get_events(match_id)
                    stats  = get_statistiken(match_id)
                except Exception as e:
                    print(f"  [Value-Bot] Stats Fehler {home}: {e}")
                    continue
                # Alle Value-Regeln prüfen
                for regel in VALUE_REGELN:
                    regel_key = f"{match_id}_{regel['typ']}"
                    if regel_key in notified_value:
                        continue
                    try:
                        if not regel["check"](game, events, stats):
                            continue
                    except Exception:
                        continue
                    prob  = regel["prob"](game, events, stats)
                    linie = regel["linie"]
                    rich  = regel["richtung"]
                    # Passende Quote suchen
                    quoten = get_live_odds_fuer_spiel(home, away)
                    if not quoten:
                        continue
                    beste_quote = None
                    bester_bm   = ""
                    for k, v in quoten.items():
                        if (rich == "unter" and "Under" in v["name"] and
                                abs(float(v.get("point", 0)) - linie) < 0.1):
                            if beste_quote is None or v["quote"] > beste_quote:
                                beste_quote = v["quote"]
                                bester_bm   = v["bookmaker"]
                        elif (rich == "über" and "Over" in v["name"] and
                                abs(float(v.get("point", 0)) - linie) < 0.1):
                            if beste_quote is None or v["quote"] > beste_quote:
                                beste_quote = v["quote"]
                                bester_bm   = v["bookmaker"]
                    if not beste_quote or beste_quote < VALUE_BET_MIN_QUOTE:
                        continue
                    edge = berechne_value(prob, beste_quote)
                    if edge < VALUE_BET_MIN_VALUE:
                        continue
                    # Value Bet gefunden!
                    notified_value.add(regel_key)
                    edge_pct = round(edge * 100, 1)
                    karten_anz = len([e for e in events if e.get("event") in KARTEN_TYPEN])
                    ecken_anz  = stats["corners_home"] + stats["corners_away"]
                    h_tore, a_tore = parse_score(score)
                    kontext = f"Karten: {karten_anz}" if "karten" in regel["typ"] else                               f"Ecken: {ecken_anz}" if "ecken" in regel["typ"] else                               f"Stand: {score} ({h_tore+a_tore} Tore)"
                    msg = (f"💎 <b>VALUE BET GEFUNDEN!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                           f"🏆 {comp} ({country})\n📌 {home} vs {away}\n"
                           f"📊 Stand: <b>{score}</b> | Minute: <b>{minute}'</b>\n"
                           f"━━━━━━━━━━━━━━━━━━━━\n"
                           f"🎯 Tipp: <b>{regel['name']}</b>\n"
                           f"📊 Situation: {kontext}\n"
                           f"📈 Wahrscheinlichkeit: <b>{round(prob*100)}%</b>\n"
                           f"💶 Beste Quote: <b>{beste_quote}</b> ({bester_bm})\n"
                           f"💎 Value Edge: <b>+{edge_pct}%</b>\n"
                           f"━━━━━━━━━━━━━━━━━━━━\n🕐 {jetzt()} Uhr")
                    send_telegram(msg)
                    embed = {
                        "title": "💎 Value Bet – Fehlerhafte Quote entdeckt!",
                        "color": 0xF1C40F,
                        "fields": [
                            {"name": "🏆 Liga",     "value": f"{comp}",            "inline": True},
                            {"name": "🌍 Land",     "value": f"{country}",         "inline": True},
                            {"name": "⏱️ Minute",   "value": f"**{minute}'**",    "inline": True},
                            {"name": "⚽ Spiel",    "value": f"{home} vs {away}", "inline": False},
                            {"name": "📊 Stand",    "value": f"**{score}**",       "inline": True},
                            {"name": "📋 Situation","value": kontext,               "inline": True},
                            {"name": "🎯 Value Tipp","value": f"**{regel['name']}**", "inline": False},
                            {"name": "📈 Wahrsch.", "value": f"**{round(prob*100)}%**", "inline": True},
                            {"name": "💶 Quote",    "value": f"**{beste_quote}** ({bester_bm})", "inline": True},
                            {"name": "💎 Edge",     "value": f"**+{edge_pct}%**",  "inline": True},
                        ],
                        "footer": {"text": f"Value-Bot • {heute()} {jetzt()}"},
                    }
                    send_discord_embed(DISCORD_WEBHOOK_VALUE, embed)
                    print(f"  [Value-Bot] ✅ {home} vs {away} | {regel['name']} | Edge +{edge_pct}%")
                # EXTRA: Bookmaker-Ausreißer Check (unabhängig von Spielsituation)
                ausreisser = vergleiche_quoten_bookmaker(home, away)
                for ar in ausreisser:
                    ar_key = f"{match_id}_bm_{ar['name']}_{ar['point']}"
                    if ar_key in notified_value:
                        continue
                    notified_value.add(ar_key)
                    msg_ar = (f"💎 <b>Quoten-Ausreißer gefunden!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                              f"🏆 {comp} ({country})\n📌 {home} vs {away}\n"
                              f"📊 Stand: <b>{score}</b> | Min. <b>{minute}'</b>\n"
                              f"━━━━━━━━━━━━━━━━━━━━\n"
                              f"🎯 Markt: <b>{ar['name']} {ar['point']}</b>\n"
                              f"💶 Beste Quote: <b>{ar['beste_quote']}</b> ({ar['bookmaker']})\n"
                              f"📊 Ø Marktquote: <b>{ar['avg_quote']}</b>\n"
                              f"💎 Abweichung: <b>+{ar['abweichung']}%</b> über Markt\n"
                              f"━━━━━━━━━━━━━━━━━━━━\n🕐 {jetzt()} Uhr")
                    send_telegram(msg_ar)
                    embed_ar = {
                        "title": "💎 Quoten-Ausreißer – Möglicher Bookmaker-Fehler!",
                        "color": 0xF39C12,
                        "fields": [
                            {"name": "🏆 Liga",      "value": f"{comp}",                    "inline": True},
                            {"name": "🌍 Land",      "value": f"{country}",                 "inline": True},
                            {"name": "⏱️ Minute",    "value": f"**{minute}'**",            "inline": True},
                            {"name": "⚽ Spiel",     "value": f"{home} vs {away}",          "inline": False},
                            {"name": "🎯 Markt",     "value": f"**{ar['name']} {ar['point']}**", "inline": True},
                            {"name": "💶 Beste Quote","value": f"**{ar['beste_quote']}** ({ar['bookmaker']})", "inline": True},
                            {"name": "📊 Ø Markt",   "value": f"**{ar['avg_quote']}**",    "inline": True},
                            {"name": "💎 Abweichung","value": f"**+{ar['abweichung']}%** über Markt", "inline": True},
                        ],
                        "footer": {"text": f"Value-Bot • {heute()} {jetzt()}"},
                    }
                    send_discord_embed(DISCORD_WEBHOOK_VALUE, embed_ar)
                    print(f"  [Value-Bot] 💎 Ausreißer: {home} vs {away} | {ar['name']} {ar['point']} | +{ar['abweichung']}%")
                time.sleep(0.5)
            bot_fehler_reset("Value-Bot")
        except Exception as e:
            bot_fehler_melden("Value-Bot", e)
        time.sleep(FUSSBALL_INTERVAL * 60)

# ============================================================
#  CS2 BOT (via PandaScore API)
# ============================================================

notified_cs2 = set()
CS2_BASE = "https://api.pandascore.co"

def cs2_get_live_matches() -> list:
    """Holt laufende CS2 Matches von PandaScore."""
    if not PANDASCORE_API_KEY or PANDASCORE_API_KEY.startswith("PANDASCORE"):
        return []
    try:
        headers = {"Authorization": f"Bearer {PANDASCORE_API_KEY}"}
        resp    = requests.get(f"{CS2_BASE}/csgo/matches/running",
                               headers=headers, timeout=10)
        if resp.status_code != 200:
            print(f"  [CS2] API Fehler: {resp.status_code}")
            return []
        return resp.json() or []
    except Exception as e:
        print(f"  [CS2] Fehler: {e}")
        return []

def cs2_get_match_details(match_id: int) -> dict:
    """Holt Details für ein CS2 Match."""
    if not PANDASCORE_API_KEY or PANDASCORE_API_KEY.startswith("PANDASCORE"):
        return {}
    try:
        headers = {"Authorization": f"Bearer {PANDASCORE_API_KEY}"}
        resp    = requests.get(f"{CS2_BASE}/csgo/matches/{match_id}",
                               headers=headers, timeout=10)
        return resp.json() if resp.status_code == 200 else {}
    except Exception as e:
        print(f"  [CS2] Match Details Fehler: {e}")
        return {}

def cs2_get_upcoming_matches() -> list:
    """Holt bevorstehende CS2 Matches (nächste 24h)."""
    if not PANDASCORE_API_KEY or PANDASCORE_API_KEY.startswith("PANDASCORE"):
        return []
    try:
        headers = {"Authorization": f"Bearer {PANDASCORE_API_KEY}"}
        resp    = requests.get(f"{CS2_BASE}/csgo/matches/upcoming",
                               headers=headers,
                               params={"per_page": 20, "sort": "begin_at"},
                               timeout=10)
        return resp.json() if resp.status_code == 200 else []
    except Exception as e:
        print(f"  [CS2] Upcoming Fehler: {e}")
        return []

def cs2_analysiere_team(team_id: int) -> dict:
    """Analysiert letzte 5 Spiele eines Teams."""
    if not PANDASCORE_API_KEY or PANDASCORE_API_KEY.startswith("PANDASCORE"):
        return {}
    try:
        headers = {"Authorization": f"Bearer {PANDASCORE_API_KEY}"}
        resp    = requests.get(f"{CS2_BASE}/csgo/teams/{team_id}/matches",
                               headers=headers,
                               params={"per_page": 5, "filter[status]": "finished"},
                               timeout=10)
        if resp.status_code != 200:
            return {}
        matches = resp.json() or []
        siege = 0
        for m in matches:
            results = m.get("results", [])
            winner  = m.get("winner", {})
            if winner and str(winner.get("id", "")) == str(team_id):
                siege += 1
        form_pct = round(siege / len(matches) * 100) if matches else 0
        return {"siege": siege, "spiele": len(matches), "form_pct": form_pct}
    except Exception as e:
        print(f"  [CS2] Team Analyse Fehler: {e}")
        return {}

def bot_cs2():
    """CS2 Bot – Live-Signale + Pre-Match Analyse + Statistiken."""
    print("[CS2-Bot] Gestartet | Live + PreMatch | PandaScore")
    prematch_gesendet = set()
    while True:
        try:
            # ── LIVE MATCHES ──────────────────────────────────
            matches = cs2_get_live_matches()
            print(f"[{jetzt()}] [CS2-Bot] {len(matches)} Live | PandaScore")
            for match in matches:
                match_id = str(match.get("id", ""))
                if match_id in notified_cs2:
                    continue
                opponents = match.get("opponents", [])
                if len(opponents) < 2:
                    continue
                team1 = (opponents[0].get("opponent") or {}).get("name", "?")
                team2 = (opponents[1].get("opponent") or {}).get("name", "?")
                t1_id = (opponents[0].get("opponent") or {}).get("id", 0)
                t2_id = (opponents[1].get("opponent") or {}).get("id", 0)
                liga       = (match.get("league") or {}).get("name", "?")
                tournament = (match.get("tournament") or {}).get("name", "?")
                results    = match.get("results", [])
                score1     = results[0].get("score", 0) if len(results) > 0 else 0
                score2     = results[1].get("score", 0) if len(results) > 1 else 0
                n_games    = match.get("number_of_games", 3)
                wins_needed = (n_games // 2) + 1
                # Aktuelle Map Infos
                games_liste = match.get("games", []) or []
                akt_map     = next((g for g in games_liste if g.get("status") == "running"), None)
                map_score   = ""
                map_name    = ""
                if akt_map:
                    map_name  = (akt_map.get("map") or {}).get("name", "")
                    r_list    = akt_map.get("results", [])
                    if len(r_list) >= 2:
                        map_score = f"{r_list[0].get('score',0)}:{r_list[1].get('score',0)}"
                signal = None
                # Signal 1: Match Point
                if score1 == wins_needed - 1 or score2 == wins_needed - 1:
                    fuehrend = team1 if score1 > score2 else team2
                    signal = {
                        "typ": "match_point",
                        "titel": "🎯 CS2 – Match Point!",
                        "text": f"**{fuehrend}** ist auf Match Point!",
                        "tipp": f"Match-Gewinner: **{fuehrend}**",
                        "farbe": 0xFF6B35,
                        "extra": f"Stand: **{score1}:{score2}** | BO{n_games}",
                    }
                # Signal 2: 2:0 oder höhere Dominanz
                elif abs(score1 - score2) >= 2 and (score1 + score2) >= 2:
                    fuehrend = team1 if score1 > score2 else team2
                    signal = {
                        "typ": "dominanz",
                        "titel": "🔥 CS2 – Klare Dominanz!",
                        "text": f"**{fuehrend}** dominiert das Match",
                        "tipp": f"Match-Gewinner: **{fuehrend}**",
                        "farbe": 0x3498DB,
                        "extra": f"Stand: **{score1}:{score2}** | BO{n_games}",
                    }
                # Signal 3: Hohe Map-Runden (enge Map = Over Runden möglich)
                elif akt_map and map_score:
                    teile = map_score.split(":")
                    if len(teile) == 2:
                        r1, r2 = _safe_int(teile[0]), _safe_int(teile[1])
                        if r1 + r2 >= 24:  # Sehr enge Map (Overtime möglich)
                            signal = {
                                "typ": "overtime",
                                "titel": "⚡ CS2 – Overtime droht!",
                                "text": f"Sehr enge Map auf **{map_name}**",
                                "tipp": f"Overtime auf {map_name} | Beide Teams eng",
                                "farbe": 0x9B59B6,
                                "extra": f"Map-Stand: **{map_score}** | Match: {score1}:{score2}",
                            }
                if not signal:
                    continue
                notified_cs2.add(match_id)
                stream_url = ""
                for stream in (match.get("streams_list") or []):
                    if stream.get("main"):
                        stream_url = stream.get("raw_url", "")
                        break
                stream_text = f"\n🎮 Stream: {stream_url}" if stream_url else ""
                msg = (f"🎮 <b>CS2 Signal!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                       f"🏆 {liga} – {tournament}\n"
                       f"📌 {team1} vs {team2}\n"
                       f"━━━━━━━━━━━━━━━━━━━━\n"
                       f"{signal['text']}\n"
                       f"📊 {signal['extra']}\n"
                       f"🎯 Tipp: <b>{signal['tipp']}</b>{stream_text}\n"
                       f"━━━━━━━━━━━━━━━━━━━━\n🕐 {jetzt()} Uhr")
                if telegram_signal_erlaubt("cs2"):
                    send_telegram(msg)
                embed = {
                    "title": f"{signal['titel']}: {team1} vs {team2}",
                    "color": signal["farbe"],
                    "fields": [
                        {"name": "🏆 Turnier",   "value": f"{liga} – {tournament}",             "inline": False},
                        {"name": "🔵 Team 1",    "value": f"**{team1}**",                        "inline": True},
                        {"name": "🔴 Team 2",    "value": f"**{team2}**",                        "inline": True},
                        {"name": "📊 Stand",     "value": signal["extra"],                       "inline": True},
                        {"name": "📋 Format",    "value": f"BO{n_games}",                        "inline": True},
                        {"name": "🗺️ Akt. Map",  "value": f"{map_name} ({map_score})" if map_name else "–", "inline": True},
                        {"name": "🎯 Tipp",      "value": f"**{signal['tipp']}**",               "inline": False},
                    ],
                    "footer": {"text": f"CS2-Bot • {heute()} {jetzt()}"},
                }
                if stream_url:
                    embed["fields"].append({"name": "🎮 Stream", "value": stream_url, "inline": False})
                send_discord_embed(DISCORD_WEBHOOK_CS2, embed)
                print(f"  [CS2-Bot] ✅ {team1} vs {team2} | {signal['typ']}")
                time.sleep(0.5)

            # ── PRE-MATCH ANALYSE ─────────────────────────────
            try:
                upcoming = cs2_get_upcoming_matches()
                for match in upcoming[:10]:
                    match_id = f"pre_{match.get('id', '')}"
                    if match_id in prematch_gesendet:
                        continue
                    # Nur Matches in den nächsten 2 Stunden
                    begin_at = match.get("begin_at", "")
                    if not begin_at:
                        continue
                    try:
                        from datetime import datetime, timezone
                        begin_dt = datetime.fromisoformat(begin_at.replace("Z", "+00:00"))
                        diff_min = (begin_dt - datetime.now(timezone.utc)).total_seconds() / 60
                        if not (5 <= diff_min <= 120):  # Innerhalb der nächsten 2 Stunden
                            continue
                    except Exception:
                        continue
                    opponents = match.get("opponents", [])
                    if len(opponents) < 2:
                        continue
                    team1   = (opponents[0].get("opponent") or {}).get("name", "?")
                    team2   = (opponents[1].get("opponent") or {}).get("name", "?")
                    t1_id   = (opponents[0].get("opponent") or {}).get("id", 0)
                    t2_id   = (opponents[1].get("opponent") or {}).get("id", 0)
                    liga    = (match.get("league") or {}).get("name", "?")
                    tourn   = (match.get("tournament") or {}).get("name", "?")
                    n_games = match.get("number_of_games", 3)
                    anstoß  = begin_dt.strftime("%H:%M") if begin_at else "?"
                    # Team-Form analysieren
                    form1 = cs2_analysiere_team(t1_id)
                    form2 = cs2_analysiere_team(t2_id)
                    p1 = form1.get("form_pct", 50)
                    p2 = form2.get("form_pct", 50)

                    # Nur senden wenn klarer Favorit + genug Daten vorhanden
                    # Bedingung: Formunterschied mind. 25% UND mind. 3 Spiele pro Team
                    hat_daten = form1.get("spiele", 0) >= 3 and form2.get("spiele", 0) >= 3
                    diff_form = abs(p1 - p2)
                    if not hat_daten or diff_form < 25:
                        print(f"  [CS2-Bot] PreMatch übersprungen: {team1} vs {team2} | Form-Diff {diff_form}% < 25% oder zu wenig Daten")
                        prematch_gesendet.add(match_id)  # nicht nochmal prüfen
                        continue

                    favorit  = team1 if p1 > p2 else team2
                    underdog = team2 if p1 > p2 else team1
                    fav_pct  = max(p1, p2)
                    und_pct  = min(p1, p2)

                    # Value-Score: je größer der Unterschied, desto besser der Bet
                    value_score = diff_form
                    value_label = "Sehr stark" if value_score >= 40 else "Klar" if value_score >= 30 else "Leicht"

                    form1_text = f"{form1.get('siege',0)}/{form1.get('spiele',0)} ({p1}%)"
                    form2_text = f"{form2.get('siege',0)}/{form2.get('spiele',0)} ({p2}%)"
                    msg = (f"🎮 <b>CS2 Pre-Match!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                           f"🏆 {liga} – {tourn}\n"
                           f"📌 {team1} vs {team2}\n"
                           f"🕐 Anstoß: <b>{anstoß} Uhr</b> | BO{n_games}\n"
                           f"━━━━━━━━━━━━━━━━━━━━\n"
                           f"📊 Form {team1}: {form1_text}\n"
                           f"📊 Form {team2}: {form2_text}\n"
                           f"🎯 Favorit: <b>{favorit}</b> ({value_label} favorisiert)\n"
                           f"💎 Tipp: <b>{favorit} gewinnt das Match</b>\n"
                           f"━━━━━━━━━━━━━━━━━━━━\n🕐 {jetzt()} Uhr")
                    send_telegram(msg)
                    farbe = 0x2ECC71 if value_score >= 40 else 0x3498DB if value_score >= 30 else 0x95A5A6
                    embed = {
                        "title": f"🎮 CS2 Pre-Match – In ~{int(diff_min)} Minuten!",
                        "color": farbe,
                        "fields": [
                            {"name": "🏆 Turnier",   "value": f"{liga} – {tourn}",              "inline": False},
                            {"name": "🎮 Match",     "value": f"**{team1}** vs **{team2}**",    "inline": False},
                            {"name": "🕐 Anstoß",    "value": f"**{anstoß} Uhr**",              "inline": True},
                            {"name": "📋 Format",    "value": f"BO{n_games}",                   "inline": True},
                            {"name": f"📊 {team1}",  "value": form1_text,                       "inline": True},
                            {"name": f"📊 {team2}",  "value": form2_text,                       "inline": True},
                            {"name": "🎯 Favorit",   "value": f"**{favorit}** ({value_label})", "inline": True},
                            {"name": "💎 Value",     "value": f"Form-Edge: **+{diff_form}%**",  "inline": True},
                            {"name": "🎯 Tipp",      "value": f"**{favorit} gewinnt**",         "inline": False},
                        ],
                        "footer": {"text": f"CS2-Bot • {heute()} {jetzt()}"},
                    }
                    send_discord_embed(DISCORD_WEBHOOK_CS2, embed)
                    prematch_gesendet.add(match_id)
                    print(f"  [CS2-Bot] ✅ PreMatch: {team1} vs {team2} | {favorit} favorisiert | Edge +{diff_form}%")
            except Exception as e:
                print(f"  [CS2-Bot] PreMatch Fehler: {e}")

            bot_fehler_reset("CS2-Bot")
        except Exception as e:
            bot_fehler_melden("CS2-Bot", e)
        time.sleep(5 * 60)  # CS2 alle 5 Minuten






# ============================================================
#  WEB-DASHBOARD – Live Statistiken im Browser
# ============================================================

def bot_web_dashboard():
    """Startet einen einfachen Webserver auf Port 8080 mit Live-Statistiken."""
    try:
        from http.server import HTTPServer, BaseHTTPRequestHandler
        import json

        class DashboardHandler(BaseHTTPRequestHandler):
            def log_message(self, format, *args):
                pass  # Keine Server-Logs

            def do_GET(self):
                if self.path == "/api/stats":
                    # JSON API für Stats
                    gw  = sum(statistik[t]["gewonnen"] for t in statistik)
                    vl  = sum(statistik[t]["verloren"] for t in statistik)
                    ges = gw + vl
                    pct = round(gw/ges*100) if ges else 0
                    br  = bankroll_laden()
                    offene = len(tracker_get_offene())
                    data = {
                        "gewonnen": gw, "verloren": vl, "trefferquote": pct,
                        "bankroll": br, "offene_signale": offene,
                        "streak": streak_aktuell, "streak_beste": streak_beste,
                        "nach_typ": {t: statistik[t] for t in statistik},
                        "api_calls": _api_monitor.get("heute", 0),
                        "zeit": jetzt(),
                    }
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Access-Control-Allow-Origin", "*")
                    self.end_headers()
                    self.wfile.write(json.dumps(data).encode())

                elif self.path in ("/", "/index.html"):
                    html = """<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>BetlabLIVE Dashboard</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { background: #0d1117; color: #e6edf3; font-family: system-ui; padding: 20px; }
  h1 { color: #58a6ff; margin-bottom: 20px; font-size: 24px; }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 16px; margin-bottom: 24px; }
  .card { background: #161b22; border: 1px solid #30363d; border-radius: 12px; padding: 20px; text-align: center; }
  .card .val { font-size: 36px; font-weight: 700; margin: 8px 0; }
  .card .lbl { font-size: 13px; color: #8b949e; }
  .green { color: #3fb950; } .red { color: #f85149; } .blue { color: #58a6ff; } .yellow { color: #d29922; }
  table { width: 100%; border-collapse: collapse; background: #161b22; border-radius: 12px; overflow: hidden; }
  th, td { padding: 12px 16px; text-align: left; border-bottom: 1px solid #21262d; }
  th { background: #21262d; font-size: 12px; color: #8b949e; text-transform: uppercase; }
  .badge { display: inline-block; padding: 2px 8px; border-radius: 20px; font-size: 12px; }
  .badge-green { background: #1f3a2a; color: #3fb950; }
  .badge-red { background: #3a1f1f; color: #f85149; }
  .footer { margin-top: 20px; color: #8b949e; font-size: 12px; }
</style>
</head>
<body>
<h1>⚽ BetlabLIVE Dashboard</h1>
<div class="grid" id="cards"></div>
<table><thead><tr><th>Bot</th><th>Gewonnen</th><th>Verloren</th><th>Quote</th><th>Status</th></tr></thead>
<tbody id="bots"></tbody></table>
<div class="footer" id="footer"></div>
<script>
async function update() {
  const r = await fetch('/api/stats');
  const d = await r.json();
  const pct = d.trefferquote;
  document.getElementById('cards').innerHTML = `
    <div class="card"><div class="val green">${d.gewonnen}</div><div class="lbl">✅ Gewonnen</div></div>
    <div class="card"><div class="val red">${d.verloren}</div><div class="lbl">❌ Verloren</div></div>
    <div class="card"><div class="val ${pct>=55?'green':pct>=45?'yellow':'red'}">${pct}%</div><div class="lbl">🎯 Trefferquote</div></div>
    <div class="card"><div class="val blue">${d.bankroll}€</div><div class="lbl">💰 Bankroll</div></div>
    <div class="card"><div class="val yellow">${d.offene_signale}</div><div class="lbl">⏳ Offene Signale</div></div>
    <div class="card"><div class="val ${d.streak>=0?'green':'red'}">${d.streak > 0 ? '+' : ''}${d.streak}</div><div class="lbl">🔥 Streak</div></div>
  `;
  const namen = {ecken:'📐 Ecken U',ecken_over:'📐 Ecken Ü',karten:'🃏 Karten',
    torwart:'🧤 Torwart',druck:'🔥 Druck',comeback:'🔄 Comeback',
    torflut:'🌊 Torflut',rotkarte:'🟥 Rotkarte',hz1tore:'🥅 HZ1-Tore',vztore:'🏆 VZ-Tore'};
  let rows = '';
  for (const [k, v] of Object.entries(d.nach_typ)) {
    const ges = v.gewonnen + v.verloren;
    const q = ges > 0 ? Math.round(v.gewonnen/ges*100) : 0;
    const badge = ges === 0 ? '' : q >= 55 ? `<span class="badge badge-green">${q}%</span>` : `<span class="badge badge-red">${q}%</span>`;
    rows += `<tr><td>${namen[k]||k}</td><td class="green">${v.gewonnen}</td><td class="red">${v.verloren}</td><td>${badge}</td><td>${ges===0?'–':'Aktiv'}</td></tr>`;
  }
  document.getElementById('bots').innerHTML = rows;
  document.getElementById('footer').textContent = `Letzte Aktualisierung: ${d.zeit} Uhr | API heute: ${d.api_calls.toLocaleString()} Calls`;
}
update();
setInterval(update, 30000);
</script>
</body></html>"""
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.end_headers()
                    self.wfile.write(html.encode())
                else:
                    self.send_response(404)
                    self.end_headers()

        server = HTTPServer(("0.0.0.0", 8080), DashboardHandler)
        print("[Dashboard] Gestartet auf Port 8080")
        server.serve_forever()
    except Exception as e:
        print(f"[Dashboard] Fehler: {e}")

# ============================================================
#  TELEGRAM GRUPPEN-BOT – Community Tipp-Tracking
# ============================================================

community_tipps  = {}  # user_id → [{"tipp": str, "spiel": str, "zeit": str, "ergebnis": None}]
COMMUNITY_DATEI  = "community_tipps.json"

def community_laden():
    import json, os
    global community_tipps
    if not os.path.exists(COMMUNITY_DATEI):
        return
    try:
        with open(COMMUNITY_DATEI) as f:
            community_tipps = json.load(f)
        print(f"  [Community] {sum(len(v) for v in community_tipps.values())} Tipps geladen")
    except Exception as e:
        print(f"  [Community] Ladefehler: {e}")

def community_speichern():
    import json
    try:
        with open(COMMUNITY_DATEI, "w") as f:
            json.dump(community_tipps, f, indent=2)
    except Exception as e:
        print(f"  [Community] Speicherfehler: {e}")

def bot_telegram_gruppe():
    """
    Gruppen-Bot: Kein eigener getUpdates – läuft jetzt über bot_telegram_befehle.
    Verhindert Token-Konflikt der Befehle verschluckt.
    """
    print("[Gruppen-Bot] Gestartet | Integriert in Haupt-Bot (kein eigenes Polling)")
    while True:
        time.sleep(3600)


def bot_selbstlernend():
    """
    Analysiert alle Bots täglich und passt Filter an:
    - Trefferquote < 40% → Filter verschärfen (weniger Signale aber bessere)
    - Trefferquote > 65% → Filter lockern (mehr Signale)
    """
    print("[Selbstlern-Bot] Gestartet | Tägliche Filter-Optimierung")
    letzter_check = None

    while True:
        try:
            now = de_now()
            # Täglich um 06:00 Uhr analysieren
            if now.hour == 6 and letzter_check != now.date():
                letzter_check = now.date()
                print("  [Selbstlern] Starte tägliche Analyse...")
                aenderungen = []

                for typ, filter_dict in DYNAMISCHE_FILTER.items():
                    perf = analysiere_bot_performance(typ)
                    if not perf["ausreichend"]:
                        print(f"  [Selbstlern] {typ}: Zu wenig Daten ({perf['tipps']} Tipps)")
                        continue

                    quote = perf["quote"]
                    print(f"  [Selbstlern] {typ}: {round(quote*100)}% ({perf['gewonnen']}/{perf['tipps']})")

                    # Filter anpassen
                    if quote < 0.40:
                        # Zu schlecht → Filter verschärfen
                        if typ == "comeback" and filter_dict.get("COMEBACK_AB_MINUTE", 30) < 45:
                            DYNAMISCHE_FILTER[typ]["COMEBACK_AB_MINUTE"] = min(45, filter_dict["COMEBACK_AB_MINUTE"] + 5)
                            aenderungen.append(f"🔴 Comeback-Bot: Minute erhöht auf {DYNAMISCHE_FILTER[typ]['COMEBACK_AB_MINUTE']}")
                        elif typ == "druck" and filter_dict.get("DRUCK_RATIO", 2.5) < 3.5:
                            DYNAMISCHE_FILTER[typ]["DRUCK_RATIO"] = round(filter_dict["DRUCK_RATIO"] + 0.25, 2)
                            aenderungen.append(f"🔴 Druck-Bot: Ratio erhöht auf {DYNAMISCHE_FILTER[typ]['DRUCK_RATIO']}")
                        elif typ == "torwart" and filter_dict.get("MIN_SHOTS_ON_TARGET", 3) < 6:
                            DYNAMISCHE_FILTER[typ]["MIN_SHOTS_ON_TARGET"] = filter_dict["MIN_SHOTS_ON_TARGET"] + 1
                            aenderungen.append(f"🔴 Torwart-Bot: Min. Schüsse erhöht auf {DYNAMISCHE_FILTER[typ]['MIN_SHOTS_ON_TARGET']}")
                        elif typ == "karten" and filter_dict.get("KARTEN_BIS_MINUTE", 40) > 30:
                            DYNAMISCHE_FILTER[typ]["KARTEN_BIS_MINUTE"] = filter_dict["KARTEN_BIS_MINUTE"] - 5
                            aenderungen.append(f"🔴 Karten-Bot: Max. Minute reduziert auf {DYNAMISCHE_FILTER[typ]['KARTEN_BIS_MINUTE']}")

                    elif quote > 0.65:
                        # Sehr gut → Filter etwas lockern
                        if typ == "comeback" and filter_dict.get("COMEBACK_AB_MINUTE", 30) > 25:
                            DYNAMISCHE_FILTER[typ]["COMEBACK_AB_MINUTE"] = max(25, filter_dict["COMEBACK_AB_MINUTE"] - 3)
                            aenderungen.append(f"🟢 Comeback-Bot: Minute reduziert auf {DYNAMISCHE_FILTER[typ]['COMEBACK_AB_MINUTE']}")
                        elif typ == "torwart" and filter_dict.get("MIN_SHOTS_ON_TARGET", 3) > 2:
                            DYNAMISCHE_FILTER[typ]["MIN_SHOTS_ON_TARGET"] = filter_dict["MIN_SHOTS_ON_TARGET"] - 1
                            aenderungen.append(f"🟢 Torwart-Bot: Min. Schüsse reduziert auf {DYNAMISCHE_FILTER[typ]['MIN_SHOTS_ON_TARGET']}")

                dynamische_filter_speichern()
                # Konfidenz-Kalibrierung prüfen
                kalibriere_konfidenz()

                if aenderungen:
                    msg = (f"🧠 <b>Selbstlern-Update!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                           f"Der Bot hat seine Filter angepasst:\n\n"
                           + "\n".join(aenderungen) +
                           f"\n━━━━━━━━━━━━━━━━━━━━\n🕐 {jetzt()} Uhr")
                    send_telegram(msg)
                    print(f"  [Selbstlern] {len(aenderungen)} Filter angepasst")
                else:
                    print(f"  [Selbstlern] Keine Anpassungen nötig")

        except Exception as e:
            print(f"  [Selbstlern] Fehler: {e}")
        time.sleep(60)

# ============================================================
#  API FALLBACK
# ============================================================

def get_live_matches_fallback() -> list:
    """Fallback API wenn livescore-api nicht antwortet."""
    try:
        # Versuche TheSportsDB als Fallback (kostenlos)
        resp = requests.get(
            "https://www.thesportsdb.com/api/v1/json/3/liveevents.php",
            timeout=8
        )
        if resp.status_code != 200:
            return []
        events = resp.json().get("events") or []
        result = []
        for e in events:
            if e.get("strSport", "").lower() != "soccer":
                continue
            score = e.get("intHomeScore", ""), e.get("intAwayScore", "")
            result.append({
                "id":     str(e.get("idEvent", "")),
                "home":   {"name": e.get("strHomeTeam", "?")},
                "away":   {"name": e.get("strAwayTeam", "?")},
                "competition": {"name": e.get("strLeague", "?")},
                "scores": {"score": f"{score[0]} - {score[1]}"},
                "status": "IN PLAY",
                "time":   e.get("strProgress", "?"),
            })
        print(f"  [Fallback API] {len(result)} Spiele geladen")
        return result
    except Exception as e:
        print(f"  [Fallback API] Fehler: {e}")
        return []

_api_fehler_count = 0

def get_live_matches_robust() -> list:
    """Primäre API mit automatischem Fallback."""
    global _api_fehler_count
    try:
        matches = ls_get_live_matches()
        _api_fehler_count = 0
        return matches
    except Exception as e:
        _api_fehler_count += 1
        print(f"  [API] Primär-API Fehler #{_api_fehler_count}: {e}")
        if _api_fehler_count >= 3:
            print(f"  [API] Wechsel zu Fallback-API")
            send_telegram(f"⚠️ <b>API Fallback aktiv!</b>\nlivescore-api antwortet nicht ({_api_fehler_count}x)\nVerwende TheSportsDB als Backup\n🕐 {jetzt()} Uhr")
            return get_live_matches_fallback()
        return []

# ============================================================
#  xG BOT (Expected Goals)
# ============================================================

notified_xg = set()

def bot_xg():
    """
    xG Bot: Vergleicht Expected Goals mit tatsächlichen Toren.
    Signal wenn xG deutlich höher als Tore → mehr Tore wahrscheinlich.
    """
    print("[xG-Bot] Gestartet | Expected Goals Analyse")
    while True:
        try:
            matches = get_live_matches()
            laufend = [m for m in matches if m.get("status") in
                       ("IN PLAY", "ADDED TIME") and
                       _safe_int(m.get("time", 0)) >= 30]
            print(f"[{jetzt()}] [xG-Bot] {len(laufend)} Spiele geprüft")
            for game in laufend:
                match_id = str(game.get("id"))
                if match_id in notified_xg:
                    continue
                home    = game.get("home", {}).get("name", "?")
                away    = game.get("away", {}).get("name", "?")
                comp    = game.get("competition", {}).get("name", "?")
                country = (game.get("country") or {}).get("name", "?")
                score   = game.get("scores", {}).get("score", "?")
                minute  = _safe_int(game.get("time", 0))
                h_tore, a_tore = parse_score(score)
                tore_ges = h_tore + a_tore
                # Stats holen für xG-Schätzung
                stats    = get_statistiken(match_id)
                shots_h  = stats["shots_on_target_home"]
                shots_a  = stats["shots_on_target_away"]
                da_h     = stats["dangerous_attacks_home"]
                da_a     = stats["dangerous_attacks_away"]
                if shots_h + shots_a == 0:
                    continue
                # xG Schätzung: Schüsse aufs Tor × 0.33 + gef. Angriffe × 0.05
                xg_h = round(shots_h * 0.33 + da_h * 0.05, 2)
                xg_a = round(shots_a * 0.33 + da_a * 0.05, 2)
                xg_ges = round(xg_h + xg_a, 2)
                # Signal wenn xG deutlich > tatsächliche Tore
                xg_diff = round(xg_ges - tore_ges, 2)
                if xg_diff < 1.5:  # Mind. 1.5 xG mehr als Tore
                    continue
                notified_xg.add(match_id)
                notified_sets_speichern()
                msg = (f"📊 <b>xG Signal!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                       f"🏆 {comp} ({country})\n📌 {home} vs {away}\n"
                       f"📊 Stand: <b>{score}</b> | Min. <b>{minute}'</b>\n"
                       f"━━━━━━━━━━━━━━━━━━━━\n"
                       f"📈 xG Gesamt: <b>{xg_ges}</b> | Tore: <b>{tore_ges}</b>\n"
                       f"🎯 xG Differenz: <b>+{xg_diff}</b> ungenutzte Chancen\n"
                       f"🔵 {home}: xG {xg_h} | 🔴 {away}: xG {xg_a}\n"
                       f"━━━━━━━━━━━━━━━━━━━━\n"
                       f"💡 Tipp: Noch mehr Tore wahrscheinlich\n"
                       f"🕐 {jetzt()} Uhr")
                send_telegram(msg)
                embed = {
                    "title": "📊 xG Signal – Mehr Tore erwartet!",
                    "color": 0x1ABC9C,
                    "fields": [
                        {"name": "🏆 Liga",       "value": comp,                    "inline": True},
                        {"name": "🌍 Land",       "value": country,                 "inline": True},
                        {"name": "⏱️ Minute",     "value": f"**{minute}'**",        "inline": True},
                        {"name": "⚽ Spiel",      "value": f"{home} vs {away}",     "inline": False},
                        {"name": "📊 Stand",      "value": f"**{score}**",          "inline": True},
                        {"name": "📈 xG gesamt",  "value": f"**{xg_ges}**",         "inline": True},
                        {"name": "⚽ Echte Tore", "value": f"**{tore_ges}**",       "inline": True},
                        {"name": "💎 xG Diff",    "value": f"**+{xg_diff}**",       "inline": True},
                        {"name": f"🔵 {home} xG", "value": f"**{xg_h}**",           "inline": True},
                        {"name": f"🔴 {away} xG", "value": f"**{xg_a}**",           "inline": True},
                        {"name": "💡 Tipp",       "value": "Mehr Tore wahrscheinlich – Über Tore prüfen", "inline": False},
                    ],
                    "footer": {"text": f"xG-Bot • {heute()} {jetzt()}"},
                }
                send_discord_embed(DISCORD_WEBHOOK_TORWART, embed)
                print(f"  [xG-Bot] ✅ {home} vs {away} | xG {xg_ges} vs {tore_ges} Tore | Diff +{xg_diff}")
                time.sleep(0.5)
            bot_fehler_reset("xG-Bot")
        except Exception as e:
            bot_fehler_melden("xG-Bot", e)
        time.sleep(FUSSBALL_INTERVAL * 60)

# ============================================================
#  ARBITRAGE FINDER
# ============================================================

notified_arbitrage = set()

def finde_arbitrage() -> list:
    """
    Findet Arbitrage-Möglichkeiten: wenn Over + Under Quote > 1 ergeben.
    Beispiel: Over 2.5 bei Bookie A zu 2.10, Under 2.5 bei Bookie B zu 2.20
    → Arbitrage wenn 1/2.10 + 1/2.20 < 1.0
    """
    if not ODDS_API_KEY:
        return []
    try:
        url    = "https://api.the-odds-api.com/v4/sports/soccer/odds/"
        params = {"apiKey": ODDS_API_KEY, "regions": "eu",
                  "markets": "totals", "oddsFormat": "decimal"}
        resp   = requests.get(url, params=params, timeout=8)
        if resp.status_code != 200:
            return []
        arbs = []
        for game in resp.json():
            home_t = game.get("home_team", "?")
            away_t = game.get("away_team", "?")
            # Beste Over + beste Under Quote finden
            beste_over  = {}  # linie → {quote, bm}
            beste_under = {}  # linie → {quote, bm}
            for bm in game.get("bookmakers", []):
                for market in bm.get("markets", []):
                    if market.get("key") != "totals":
                        continue
                    for outcome in market.get("outcomes", []):
                        linie = str(outcome.get("point", ""))
                        q     = outcome.get("price", 0)
                        name  = outcome.get("name", "")
                        if "Over" in name:
                            if linie not in beste_over or q > beste_over[linie]["q"]:
                                beste_over[linie] = {"q": q, "bm": bm.get("title", "?")}
                        elif "Under" in name:
                            if linie not in beste_under or q > beste_under[linie]["q"]:
                                beste_under[linie] = {"q": q, "bm": bm.get("title", "?")}
            # Arbitrage prüfen
            for linie in beste_over:
                if linie not in beste_under:
                    continue
                q_over  = beste_over[linie]["q"]
                q_under = beste_under[linie]["q"]
                margin  = round(1/q_over + 1/q_under, 4)
                if margin < 0.98:  # Unter 98% = Arbitrage!
                    profit_pct = round((1 - margin) * 100, 2)
                    arbs.append({
                        "home": home_t, "away": away_t,
                        "linie": linie,
                        "q_over": q_over, "bm_over": beste_over[linie]["bm"],
                        "q_under": q_under, "bm_under": beste_under[linie]["bm"],
                        "margin": margin, "profit_pct": profit_pct,
                    })
        return sorted(arbs, key=lambda x: x["profit_pct"], reverse=True)
    except Exception as e:
        print(f"  [Arbitrage] Fehler: {e}")
        return []

def bot_arbitrage():
    """Arbitrage Finder – sucht risikolose Gewinnmöglichkeiten."""
    print("[Arbitrage-Bot] Gestartet | Suche Arbitrage-Möglichkeiten")
    while True:
        try:
            arbs = finde_arbitrage()
            print(f"[{jetzt()}] [Arbitrage-Bot] {len(arbs)} Arbitragen gefunden")
            for arb in arbs[:5]:  # Max 5 pro Durchlauf
                key = f"{arb['home']}_{arb['away']}_{arb['linie']}"
                if key in notified_arbitrage:
                    continue
                notified_arbitrage.add(key)
                # Optimale Einsatzaufteilung berechnen
                einsatz_total = 100  # Beispiel 100€
                einsatz_over  = round(einsatz_total / arb["q_over"] / (1/arb["q_over"] + 1/arb["q_under"]), 2)
                einsatz_under = round(einsatz_total - einsatz_over, 2)
                gewinn_sicher = round(einsatz_over * arb["q_over"] - einsatz_total, 2)
                msg = (f"💰 <b>ARBITRAGE GEFUNDEN!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                       f"📌 {arb['home']} vs {arb['away']}\n"
                       f"━━━━━━━━━━━━━━━━━━━━\n"
                       f"📈 Over {arb['linie']}: <b>{arb['q_over']}</b> @ {arb['bm_over']}\n"
                       f"📉 Under {arb['linie']}: <b>{arb['q_under']}</b> @ {arb['bm_under']}\n"
                       f"━━━━━━━━━━━━━━━━━━━━\n"
                       f"💎 Risikoloser Gewinn: <b>+{arb['profit_pct']}%</b>\n"
                       f"💡 Bei 100€: Over {einsatz_over}€ + Under {einsatz_under}€\n"
                       f"✅ Garantierter Gewinn: <b>+{gewinn_sicher}€</b>\n"
                       f"🕐 {jetzt()} Uhr")
                send_telegram(msg)
                embed = {
                    "title": "💰 Arbitrage – Risikoloser Gewinn!",
                    "color": 0xF1C40F,
                    "fields": [
                        {"name": "⚽ Spiel",       "value": f"**{arb['home']}** vs **{arb['away']}**", "inline": False},
                        {"name": "📈 Over Linie",  "value": f"Über {arb['linie']}", "inline": True},
                        {"name": "📈 Over Quote",  "value": f"**{arb['q_over']}** @ {arb['bm_over']}", "inline": True},
                        {"name": "📉 Under Quote", "value": f"**{arb['q_under']}** @ {arb['bm_under']}", "inline": True},
                        {"name": "💎 Profit",      "value": f"**+{arb['profit_pct']}%** garantiert", "inline": True},
                        {"name": "💡 Einsatz",     "value": f"Over: **{einsatz_over}€** | Under: **{einsatz_under}€**", "inline": False},
                        {"name": "✅ Garantierter Gewinn", "value": f"**+{gewinn_sicher}€** bei 100€ Einsatz", "inline": False},
                    ],
                    "footer": {"text": f"Arbitrage-Bot • {heute()} {jetzt()}"},
                }
                send_discord_embed(DISCORD_WEBHOOK_VALUE, embed)
                print(f"  [Arbitrage] ✅ {arb['home']} vs {arb['away']} | +{arb['profit_pct']}%")
            bot_fehler_reset("Arbitrage-Bot")
        except Exception as e:
            bot_fehler_melden("Arbitrage-Bot", e)
        time.sleep(10 * 60)  # Alle 10 Minuten

# ============================================================
#  CLOSING LINE VALUE TRACKER
# ============================================================

def clv_tracker_nach_spiel(spiel: dict) -> str:
    """
    Vergleicht Einstiegsquote mit aktueller Marktquote nach Spielende.
    Gibt formatierten CLV-Text zurück.
    """
    einstieg = spiel.get("quote")
    if not einstieg or not ODDS_API_KEY:
        return ""
    try:
        details = get_quote_details(spiel.get("home", ""), spiel.get("away", ""))
        schluss = details.get("avg_quote")
        if not schluss or schluss <= 1.0:
            return ""
        diff_pct = round((einstieg - schluss) / schluss * 100, 1)
        if diff_pct > 5:
            return f"\n📊 CLV: ✅ Guter Einstieg! {einstieg} → Schluss {schluss} (+{diff_pct}%)"
        elif diff_pct < -5:
            return f"\n📊 CLV: ⚠️ Quote gesunken: {einstieg} → Schluss {schluss} ({diff_pct}%)"
        return f"\n📊 CLV: Quote stabil: {einstieg} → {schluss}"
    except Exception:
        return ""


# ============================================================
#  PATTERN RECOGNITION
# ============================================================

_pattern_cache = {}
PATTERN_TTL    = 7200

def analysiere_team_muster(team_id, team_name):
    now = time.time()
    if team_id in _pattern_cache and now - _pattern_cache[team_id]["ts"] < PATTERN_TTL:
        return _pattern_cache[team_id]["data"]
    try:
        params  = {**LS_AUTH, "team_id": team_id, "number": 10}
        resp    = api_get_with_retry(f"{LS_BASE}/matches/history.json", params)
        matches = resp.json().get("data", {}).get("match", []) or []
        if len(matches) < 5:
            return {}
        hz1_tore, hz2_tore, comebacks, rueckstaende = [], [], 0, 0
        for m in matches:
            svz = (m.get("scores") or {}).get("score", "")
            shz = (m.get("scores") or {}).get("ht_score", "")
            if not svz or not shz:
                continue
            h_vz, a_vz = parse_score(svz)
            h_hz, a_hz = parse_score(shz)
            hz1_tore.append(h_hz + a_hz)
            hz2_tore.append((h_vz - h_hz) + (a_vz - a_hz))
            hid = str((m.get("home") or {}).get("id", ""))
            if hid == team_id:
                if h_hz < a_hz:
                    rueckstaende += 1
                    if h_vz > a_vz:
                        comebacks += 1
            else:
                if a_hz < h_hz:
                    rueckstaende += 1
                    if a_vz > h_vz:
                        comebacks += 1
        avg1 = round(sum(hz1_tore)/len(hz1_tore), 2) if hz1_tore else 0
        avg2 = round(sum(hz2_tore)/len(hz2_tore), 2) if hz2_tore else 0
        cr   = round(comebacks / max(rueckstaende, 1) * 100)
        result = {"team": team_name, "avg_hz1_tore": avg1, "avg_hz2_tore": avg2,
                  "hz2_staerker": avg2 > avg1 * 1.3, "comeback_rate": cr, "spiele": len(matches)}
        _pattern_cache[team_id] = {"data": result, "ts": now}
        return result
    except Exception as e:
        print(f"  [Pattern] Fehler: {e}")
        return {}

def pattern_signal_text(p1, p2):
    zeilen = []
    if p1.get("hz2_staerker"):
        zeilen.append(f"\U0001f525 {p1['team']}: Staerker in 2. HZ (\u00d8 {p1['avg_hz2_tore']} Tore/HZ2)")
    if p2.get("hz2_staerker"):
        zeilen.append(f"\U0001f525 {p2['team']}: Staerker in 2. HZ (\u00d8 {p2['avg_hz2_tore']} Tore/HZ2)")
    if p1.get("comeback_rate", 0) >= 60:
        zeilen.append(f"\U0001f504 {p1['team']}: Hohe Comeback-Rate ({p1['comeback_rate']}%)")
    if p2.get("comeback_rate", 0) >= 60:
        zeilen.append(f"\U0001f504 {p2['team']}: Hohe Comeback-Rate ({p2['comeback_rate']}%)")
    return "\n".join(zeilen)

# ============================================================
#  SENTIMENT-ANALYSE
# ============================================================

def hole_team_sentiment(home, away):
    if not ANTHROPIC_API_KEY or not ANTHROPIC_API_KEY.strip():
        return {}
    try:
        query    = f"{home} {away} injury lineup news today"
        news_url = f"https://html.duckduckgo.com/html/?q={query.replace(' ', '+')}"
        resp     = requests.get(news_url, timeout=8, headers={"User-Agent": "Mozilla/5.0"})
        import re as _re
        text_raw = _re.sub(r"<[^>]+>", " ", resp.text)
        text_raw = " ".join(text_raw.split())[:1500]
        if not text_raw:
            return {}
        prompt = (f"Analysiere News zu {home} vs {away}:\n{text_raw}\n\n"
                  f"Antworte so:\nSENTIMENT_HOME: positiv/negativ/neutral\n"
                  f"SENTIMENT_AWAY: positiv/negativ/neutral\nHINWEIS: [1 Satz wichtigste Info]")
        r2 = requests.post("https://api.anthropic.com/v1/messages",
            headers={"Content-Type": "application/json", "x-api-key": ANTHROPIC_API_KEY,
                     "anthropic-version": "2023-06-01"},
            json={"model": "claude-sonnet-4-20250514", "max_tokens": 120,
                  "messages": [{"role": "user", "content": prompt}]}, timeout=15)
        if r2.status_code != 200:
            return {}
        txt    = r2.json().get("content", [{}])[0].get("text", "")
        result = {"home_sentiment": "neutral", "away_sentiment": "neutral", "hinweis": ""}
        for line in txt.split("\n"):
            if line.startswith("SENTIMENT_HOME:"):
                result["home_sentiment"] = line.replace("SENTIMENT_HOME:", "").strip().lower()
            elif line.startswith("SENTIMENT_AWAY:"):
                result["away_sentiment"] = line.replace("SENTIMENT_AWAY:", "").strip().lower()
            elif line.startswith("HINWEIS:"):
                result["hinweis"] = line.replace("HINWEIS:", "").strip()
        return result
    except Exception as e:
        print(f"  [Sentiment] Fehler: {e}")
        return {}

def sentiment_emoji(s):
    return {"positiv": "\U0001f7e2", "negativ": "\U0001f534", "neutral": "\U0001f7e1"}.get(s, "\u26aa")

# ============================================================
#  MULTI-MODELL VOTING
# ============================================================

def multi_modell_vote(home, away, typ, analyse, score, minute, liga: str = ""):
    if not ANTHROPIC_API_KEY or not ANTHROPIC_API_KEY.strip():
        return {"empfohlen": True, "votes": 2, "begruendung": ""}
    # Multi-Modell nie für Live-Signale – zu viele Calls
    return {"empfohlen": True, "votes": 2, "begruendung": ""}
    typ_name    = TYP_NAMEN.get(typ, typ)
    perspektiven = [
        ("Statistik-Analyst", f"Nur Zahlen: Daten: {analyse}. Ist '{typ_name}' bei {home} vs {away} (Stand {score}, Min.{minute}) statistisch ok?"),
        ("Erfahrener Wetter",  f"10 Jahre Live-Wetten. {home} vs {away}, Stand {score}, Min.{minute}. '{typ_name}' setzen?"),
        ("Skeptiker",          f"Sei kritisch. Was spricht gegen '{typ_name}' bei {home} vs {away} (Stand {score}, Min.{minute})?"),
    ]
    votes_ja = 0
    beg = []
    for rolle, pt in perspektiven:
        try:
            r = requests.post("https://api.anthropic.com/v1/messages",
                headers={"Content-Type": "application/json", "x-api-key": ANTHROPIC_API_KEY,
                         "anthropic-version": "2023-06-01"},
                json={"model": "claude-sonnet-4-20250514", "max_tokens": 60,
                      "messages": [{"role": "user", "content": f"Du bist {rolle}. NUR JA oder NEIN dann 1 Satz:\n{pt}"}]},
                timeout=10)
            if r.status_code != 200:
                votes_ja += 1
                continue
            antwort = r.json().get("content", [{}])[0].get("text", "").strip()
            ja = antwort.upper().startswith("JA")
            if ja:
                votes_ja += 1
            teile = antwort.split(" ", 1)
            if len(teile) > 1:
                beg.append(f"{'\u2705' if ja else '\u274c'} {rolle}: {teile[1][:70]}")
        except Exception:
            votes_ja += 1
    return {"empfohlen": votes_ja >= 2, "votes": votes_ja,
            "begruendung": "\n".join(beg[:2])}





# ============================================================
#  STARTUP & HEALTH CHECK
# ============================================================

BOT_START_ZEIT = time.time()

def bot_startup_alarm():
    """Sendet Alarm wenn Bot neu startet."""
    try:
        start_str = de_now().strftime("%d.%m.%Y %H:%M")
        msg = (f"🔄 <b>Bot neu gestartet!</b>\n"
               f"━━━━━━━━━━━━━━━━━━━━\n"
               f"🕐 {start_str} Uhr\n"
               f"✅ Alle {len(bot_definitionen)} Bots werden gestartet\n"
               f"📊 Signal-Tracker geladen\n"
               f"━━━━━━━━━━━━━━━━━━━━\n"
               f"⚡ BetlabLIVE ist wieder aktiv!")
        send_telegram(msg)
    except Exception as e:
        print(f"  [Startup] Alarm Fehler: {e}")

def bot_health_check_server():
    """
    Einfacher HTTP-Server für Railway Health-Checks.
    Railway ruft /health auf – antwortet mit 200 OK wenn Bot läuft.
    """
    try:
        from http.server import HTTPServer, BaseHTTPRequestHandler
        import json

        class HealthHandler(BaseHTTPRequestHandler):
            def log_message(self, fmt, *args):
                pass

            def do_GET(self):
                if self.path == "/health":
                    uptime_min = round((time.time() - BOT_START_ZEIT) / 60)
                    gw  = sum(statistik[t]["gewonnen"] for t in statistik)
                    vl  = sum(statistik[t]["verloren"] for t in statistik)
                    data = {
                        "status": "ok",
                        "uptime_min": uptime_min,
                        "gewonnen": gw,
                        "verloren": vl,
                        "api_calls": _api_monitor.get("heute", 0),
                        "offene_signale": len(tracker_get_offene()),
                    }
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps(data).encode())
                else:
                    self.send_response(404)
                    self.end_headers()

        # Health auf Port 8081, Dashboard auf 8080
        server = HTTPServer(("0.0.0.0", 8081), HealthHandler)
        print("[Health-Check] Gestartet auf Port 8081 /health")
        server.serve_forever()
    except Exception as e:
        print(f"[Health-Check] Fehler: {e}")

# ============================================================
#  RATE-LIMIT DASHBOARD
# ============================================================

API_LIMIT_WARNUNG_PCT = 80  # Warnung bei 80% des Tages-Limits

def check_rate_limit_warnung():
    """Warnt wenn API-Calls 80% des Limits erreichen."""
    calls_heute = _api_monitor.get("heute", 0)
    limit       = 50000  # livescore-api Tages-Limit
    pct         = round(calls_heute / limit * 100, 1)
    if pct >= API_LIMIT_WARNUNG_PCT:
        msg = (f"⚠️ <b>API Rate-Limit Warnung!</b>\n"
               f"━━━━━━━━━━━━━━━━━━━━\n"
               f"📊 Heute: <b>{calls_heute:,}</b> / {limit:,} Calls\n"
               f"📈 Auslastung: <b>{pct}%</b>\n"
               f"{'🔴 KRITISCH – Bot könnte gedrosselt werden!' if pct >= 95 else '⚠️ Hohe Auslastung'}\n"
               f"━━━━━━━━━━━━━━━━━━━━\n🕐 {jetzt()} Uhr")
        send_telegram(msg)
        return True
    return False

# ============================================================
#  TIPP-CLUSTERING
# ============================================================

def analysiere_tipp_clustering() -> dict:
    """
    Findet welche Faktor-Kombinationen am häufigsten gewinnen.
    Beispiel: Typ=druck + Liga=Premier League + Stunde=20 → 78% Trefferquote
    """
    with _tracker_lock:
        alle = [s for s in _signal_tracker.values()
                if s.get("status") == "ausgewertet"]
    if len(alle) < 50:
        return {}
    import datetime
    cluster = {}
    for s in alle:
        typ  = s.get("typ", "?")
        liga = s.get("competition", s.get("liga", "?"))[:20] if s.get("competition") or s.get("liga") else "?"
        try:
            h = datetime.datetime.fromtimestamp(s["signal_zeit"]).hour
            block = "Abend" if h >= 18 else ("Nachmittag" if h >= 13 else "Morgen")
        except Exception:
            block = "?"
        key = f"{typ}+{block}"
        if key not in cluster:
            cluster[key] = {"g": 0, "total": 0}
        cluster[key]["total"] += 1
        if s.get("gewonnen"):
            cluster[key]["g"] += 1
    # Beste Cluster (min. 5 Tipps)
    beste = sorted(
        [(k, v) for k, v in cluster.items() if v["total"] >= 5],
        key=lambda x: x[1]["g"] / x[1]["total"],
        reverse=True
    )[:5]
    return {k: {"quote": round(v["g"]/v["total"]*100), "tipps": v["total"]} for k, v in beste}

# ============================================================
#  ANOMALIE-ERKENNUNG
# ============================================================

notified_anomalie = set()

def bot_anomalie_erkennung():
    """
    Erkennt statistisch außergewöhnliche Spiele.
    Beispiel: 15 Ecken in 30 Min, 8 Schüsse aufs Tor in 15 Min.
    """
    print("[Anomalie-Bot] Gestartet | Statistische Ausreißer erkennen")
    while True:
        try:
            matches = get_live_matches()
            laufend = [m for m in matches if m.get("status") == "IN PLAY"
                       and _safe_int(m.get("time", 0)) >= 15]
            for game in laufend:
                match_id = str(game.get("id"))
                if match_id in notified_anomalie:
                    continue
                home    = game.get("home", {}).get("name", "?")
                away    = game.get("away", {}).get("name", "?")
                comp    = game.get("competition", {}).get("name", "?")
                minute  = _safe_int(game.get("time", 0))
                score   = game.get("scores", {}).get("score", "?")
                stats   = get_statistiken(match_id)
                ecken_ges   = stats["corners_home"] + stats["corners_away"]
                schuesse_ges = stats["shots_on_target_home"] + stats["shots_on_target_away"]
                da_ges      = stats["dangerous_attacks_home"] + stats["dangerous_attacks_away"]
                anomalien = []
                # Ecken-Rate: normal ~4-6 in 30 Min → Anomalie wenn >12
                if minute <= 35 and ecken_ges >= 12:
                    anomalien.append(f"📐 {ecken_ges} Ecken in Minute {minute} (extrem hoch!)")
                # Schüsse-Rate: normal ~3-5 in 20 Min → Anomalie wenn >10
                if minute <= 25 and schuesse_ges >= 10:
                    anomalien.append(f"🎯 {schuesse_ges} Schüsse aufs Tor in Minute {minute}!")
                # Gefährliche Angriffe: Anomalie wenn >60 in 30 Min
                if minute <= 35 and da_ges >= 60:
                    anomalien.append(f"⚡ {da_ges} gefährliche Angriffe in Minute {minute}!")
                if not anomalien:
                    continue
                notified_anomalie.add(match_id)
                notified_sets_speichern()
                msg = (f"🚨 <b>Anomalie erkannt!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                       f"🏆 {comp}\n📌 {home} vs {away}\n"
                       f"📊 Stand: <b>{score}</b> | Min. <b>{minute}'</b>\n"
                       f"━━━━━━━━━━━━━━━━━━━━\n"
                       + "\n".join(anomalien) +
                       f"\n━━━━━━━━━━━━━━━━━━━━\n"
                       f"💡 Statistisch außergewöhnlich – lohnt Blick auf Live-Märkte!\n"
                       f"🕐 {jetzt()} Uhr")
                send_telegram(msg)
                embed = {
                    "title": "🚨 Statistische Anomalie!",
                    "color": 0xFF0000,
                    "fields": [
                        {"name": "🏆 Liga",    "value": comp,                 "inline": True},
                        {"name": "⏱️ Minute", "value": f"**{minute}'**",      "inline": True},
                        {"name": "⚽ Spiel",  "value": f"{home} vs {away}",  "inline": False},
                        {"name": "📊 Stand",  "value": f"**{score}**",       "inline": True},
                        {"name": "🚨 Anomalie","value": "\n".join(anomalien),"inline": False},
                    ],
                    "footer": {"text": f"Anomalie-Bot • {heute()} {jetzt()}"},
                }
                send_discord_embed(DISCORD_WEBHOOK_VALUE, embed)
                print(f"  [Anomalie] 🚨 {home} vs {away}: {anomalien}")
            bot_fehler_reset("Anomalie-Bot")
        except Exception as e:
            bot_fehler_melden("Anomalie-Bot", e)
        time.sleep(90)

# ============================================================
#  GEGNERMODELL
# ============================================================

_team_profile_cache = {}

def erstelle_team_profil(team_id: str, team_name: str) -> dict:
    """
    Analysiert Spielweise eines Teams:
    - Offensiv (viele Schüsse) vs Defensiv (wenige)
    - Pressingintensiv (viele gefährliche Angriffe) vs Passiv
    - Eckenstark vs Eckenschwach
    """
    if team_id in _team_profile_cache:
        return _team_profile_cache[team_id]
    try:
        params = {**LS_AUTH, "team_id": team_id, "number": 8}
        resp   = api_get_with_retry(f"{LS_BASE}/matches/history.json", params)
        spiele = resp.json().get("data", {}).get("match", []) or []
        if len(spiele) < 4:
            return {}
        shots_avg = []
        ecken_avg = []
        tore_avg  = []
        for m in spiele:
            stats_m  = (m.get("stats") or {})
            home_id  = str((m.get("home") or {}).get("id", ""))
            ist_heim = home_id == team_id
            side     = "home" if ist_heim else "away"
            s = stats_m.get(f"shots_on_target_{side}", 0) or 0
            e = stats_m.get(f"corners_{side}", 0) or 0
            score_m  = (m.get("scores") or {}).get("score", "")
            h_m, a_m = parse_score(score_m)
            t = h_m if ist_heim else a_m
            shots_avg.append(_safe_int(s))
            ecken_avg.append(_safe_int(e))
            tore_avg.append(t)
        shots_m  = round(sum(shots_avg) / len(shots_avg), 1) if shots_avg else 0
        ecken_m  = round(sum(ecken_avg) / len(ecken_avg), 1) if ecken_avg else 0
        tore_m   = round(sum(tore_avg)  / len(tore_avg),  1) if tore_avg  else 0
        stil = "offensiv" if shots_m >= 5 else ("defensiv" if shots_m <= 2 else "ausgewogen")
        profil = {
            "team":      team_name,
            "shots_avg": shots_m,
            "ecken_avg": ecken_m,
            "tore_avg":  tore_m,
            "stil":      stil,
            "eckenstark": ecken_m >= 5,
        }
        _team_profile_cache[team_id] = profil
        return profil
    except Exception as e:
        print(f"  [Gegnermodell] Fehler {team_name}: {e}")
        return {}

def profil_zu_text(p1: dict, p2: dict) -> str:
    """Kurzer Profiltext für Signale."""
    if not p1 and not p2:
        return ""
    zeilen = []
    if p1.get("stil"):
        zeilen.append(f"🎭 {p1['team']}: {p1['stil'].capitalize()} (Ø {p1['shots_avg']} Schüsse)")
    if p2.get("stil"):
        zeilen.append(f"🎭 {p2['team']}: {p2['stil'].capitalize()} (Ø {p2['shots_avg']} Schüsse)")
    return "\n".join(zeilen)

# ============================================================
#  KONFIDENZ-DECAY
# ============================================================

def wende_konfidenz_decay_an():
    """
    Reduziert Konfidenz offener Signale über Zeit.
    Nach 30 Min ohne Auswertung: -1 Konfidenz alle 15 Min.
    """
    now = time.time()
    with _tracker_lock:
        for key, sig in _signal_tracker.items():
            if sig.get("status") != "offen":
                continue
            alter_min = (now - sig.get("signal_zeit", now)) / 60
            if alter_min < 30:
                continue
            decay_stufen = int((alter_min - 30) / 15)
            orig_konfidenz = sig.get("konfidenz_original",
                                     sig.get("konfidenz", 6))
            if "konfidenz_original" not in sig:
                _signal_tracker[key]["konfidenz_original"] = orig_konfidenz
            neue_konfidenz = max(1, orig_konfidenz - decay_stufen)
            _signal_tracker[key]["konfidenz"] = neue_konfidenz

# ============================================================
#  TRANSFER LEARNING
# ============================================================

LIGA_AEHNLICHKEITEN = {
    "Bundesliga":         ["Eredivisie", "Belgian Pro League", "Austrian Bundesliga"],
    "Premier League":     ["Championship", "Scottish Premiership", "MLS"],
    "La Liga":            ["Primeira Liga", "Serie A", "Ligue 1"],
    "Serie A":            ["La Liga", "Ligue 1", "Super Lig"],
    "Ligue 1":            ["Serie A", "Eredivisie", "Belgian Pro League"],
    "Eredivisie":         ["Bundesliga", "Belgian Pro League"],
    "Primera Division":   ["La Liga", "Serie A"],
}

def transfer_konfidenz_bonus(typ: str, liga: str) -> int:
    """
    Gibt Konfidenz-Bonus wenn ähnliche Ligen gute Erfahrung mit diesem Bot-Typ haben.
    """
    with _tracker_lock:
        alle = [s for s in _signal_tracker.values()
                if s.get("typ") == typ and s.get("status") == "ausgewertet"]
    aehnliche = LIGA_AEHNLICHKEITEN.get(liga, [])
    if not aehnliche:
        return 0
    relevant = [s for s in alle
                if any(a.lower() in (s.get("competition") or "").lower()
                       for a in aehnliche)]
    if len(relevant) < 8:
        return 0
    gw  = sum(1 for s in relevant if s.get("gewonnen"))
    pct = gw / len(relevant)
    if pct >= 0.65:
        return 1
    elif pct >= 0.75:
        return 2
    return 0

# ============================================================
#  A/B TESTING
# ============================================================

AB_FILTER = {
    "ecken_unter": {
        "A": {"max_hz1_corners": 5, "label": "≤5 HZ1"},
        "B": {"max_hz1_corners": 4, "label": "≤4 HZ1"},
        "ergebnisse": {"A": {"g": 0, "total": 0}, "B": {"g": 0, "total": 0}},
    }
}
AB_DATEI = "ab_test.json"

def ab_test_laden():
    import json, os
    global AB_FILTER
    if os.path.exists(AB_DATEI):
        try:
            with open(AB_DATEI) as f:
                AB_FILTER = json.load(f)
        except Exception:
            pass

def ab_test_speichern():
    import json
    try:
        with open(AB_DATEI, "w") as f:
            json.dump(AB_FILTER, f, indent=2)
    except Exception:
        pass

def ab_test_variante(test_name: str) -> str:
    """Gibt A oder B zurück (gleichmäßige Verteilung)."""
    if test_name not in AB_FILTER:
        return "A"
    erg = AB_FILTER[test_name]["ergebnisse"]
    total_a = erg["A"]["total"]
    total_b = erg["B"]["total"]
    return "A" if total_a <= total_b else "B"

def ab_test_auswerten():
    """Wertet A/B Tests aus und behält die bessere Variante."""
    for test, data in AB_FILTER.items():
        erg = data["ergebnisse"]
        for v in ("A", "B"):
            if erg[v]["total"] < 20:
                continue
        total_a = erg["A"]["total"]
        total_b = erg["B"]["total"]
        if total_a < 20 or total_b < 20:
            continue
        q_a = erg["A"]["g"] / total_a
        q_b = erg["B"]["g"] / total_b
        gewinner = "A" if q_a >= q_b else "B"
        msg = (f"🧪 <b>A/B Test Ergebnis: {test}</b>\n"
               f"━━━━━━━━━━━━━━━━━━━━\n"
               f"A ({data['A']['label']}): {round(q_a*100)}% ({erg['A']['g']}/{total_a})\n"
               f"B ({data['B']['label']}): {round(q_b*100)}% ({erg['B']['g']}/{total_b})\n"
               f"🏆 Gewinner: Variante <b>{gewinner}</b>\n"
               f"✅ Wird ab sofort als Standard verwendet")
        send_telegram(msg)
        print(f"  [A/B] {test}: Variante {gewinner} gewinnt")

# ============================================================
#  SHARP MONEY TRACKER
# ============================================================

_sharp_history = {}
notified_sharp = set()

def bot_sharp_money():
    """
    Erkennt professionelle Wetter-Bewegungen:
    - Mehrere Bookmaker bewegen gleichzeitig dieselbe Quote
    - Bewegung gegen den intuitiven Trend (z.B. Favorit-Quote fällt obwohl mehr Geld auf Außenseiter)
    """
    print("[Sharp-Money-Bot] Gestartet | Pro-Wetter Bewegungen")
    while True:
        try:
            if not ODDS_API_KEY:
                time.sleep(10 * 60)
                continue
            url    = "https://api.the-odds-api.com/v4/sports/soccer/odds/"
            params = {"apiKey": ODDS_API_KEY, "regions": "eu",
                      "markets": "h2h", "oddsFormat": "decimal"}
            resp   = requests.get(url, params=params, timeout=10)
            if resp.status_code != 200:
                time.sleep(10 * 60)
                continue
            now = time.time()
            for game in resp.json():
                home_t = game.get("home_team", "?")
                away_t = game.get("away_team", "?")
                key    = f"{home_t}_{away_t}"
                # Alle Bookmaker Quoten sammeln
                bm_quoten = {}
                for bm in game.get("bookmakers", []):
                    for market in bm.get("markets", []):
                        if market.get("key") != "h2h":
                            continue
                        for outcome in market.get("outcomes", []):
                            name = outcome.get("name", "")
                            q    = outcome.get("price", 0)
                            if name not in bm_quoten:
                                bm_quoten[name] = []
                            bm_quoten[name].append(q)
                if key not in _sharp_history:
                    _sharp_history[key] = {"ts": now, "quoten": bm_quoten}
                    continue
                alt = _sharp_history[key]
                if now - alt["ts"] < 8 * 60:
                    continue
                # Vergleiche ob mehrere BMs gleichzeitig dieselbe Richtung bewegen
                sharp_signals = []
                for name, quoten in bm_quoten.items():
                    if name not in alt["quoten"]:
                        continue
                    q_neu = sum(quoten) / len(quoten)
                    q_alt = sum(alt["quoten"][name]) / len(alt["quoten"][name])
                    bewegung = round((q_neu - q_alt) / q_alt * 100, 1)
                    # Signifikante Bewegung: >8% bei mehreren BMs
                    if abs(bewegung) >= 8 and len(quoten) >= 3:
                        richtung = "📉 gefallen" if bewegung < 0 else "📈 gestiegen"
                        sharp_signals.append(f"{name}: {q_alt:.2f}→{q_neu:.2f} ({bewegung:+.1f}%) {richtung}")
                _sharp_history[key] = {"ts": now, "quoten": bm_quoten}
                if not sharp_signals or key in notified_sharp:
                    continue
                notified_sharp.add(key)
                msg = (f"💼 <b>Sharp Money Signal!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                       f"📌 {home_t} vs {away_t}\n"
                       f"━━━━━━━━━━━━━━━━━━━━\n"
                       + "\n".join(sharp_signals) +
                       f"\n━━━━━━━━━━━━━━━━━━━━\n"
                       f"💡 Professionelle Wetter bewegen den Markt!\n"
                       f"🕐 {jetzt()} Uhr")
                send_telegram(msg)
                embed = {
                    "title": "💼 Sharp Money – Pro-Wetter aktiv!",
                    "color": 0x8E44AD,
                    "fields": [
                        {"name": "⚽ Spiel",     "value": f"{home_t} vs {away_t}", "inline": False},
                        {"name": "📊 Bewegungen","value": "\n".join(sharp_signals),"inline": False},
                    ],
                    "footer": {"text": f"Sharp-Money-Bot • {heute()} {jetzt()}"},
                }
                send_discord_embed(DISCORD_WEBHOOK_VALUE, embed)
                print(f"  [Sharp] 💼 {home_t} vs {away_t}")
            bot_fehler_reset("Sharp-Money-Bot")
        except Exception as e:
            bot_fehler_melden("Sharp-Money-Bot", e)
        time.sleep(8 * 60)

# ============================================================
#  DATEN-EXPORT & SIGNAL-ARCHIV
# ============================================================

def erstelle_excel_export() -> str | None:
    """Erstellt Excel-Export der letzten 30 Tage."""
    try:
        import openpyxl
        from openpyxl.styles import PatternFill, Font, Alignment
        import datetime

        wb  = openpyxl.Workbook()
        ws  = wb.active
        ws.title = "BetlabLIVE Tipps"
        # Header
        header = ["Datum", "Uhrzeit", "Liga", "Heim", "Gast", "Typ",
                  "Tipp", "Quote", "Konfidenz", "EV%", "Status", "Ergebnis"]
        for i, h in enumerate(header, 1):
            cell = ws.cell(row=1, column=i, value=h)
            cell.font = PatternFill("solid", fgColor="1F3A5F")
            cell.font = Font(bold=True, color="FFFFFF")
        # Daten
        grenze_ts = time.time() - 30 * 24 * 3600
        with _tracker_lock:
            signale = [s for s in _signal_tracker.values()
                       if s.get("signal_zeit", 0) >= grenze_ts]
        signale_sort = sorted(signale, key=lambda x: x.get("signal_zeit", 0), reverse=True)
        for row, s in enumerate(signale_sort, 2):
            ts = s.get("signal_zeit", 0)
            try:
                dt = datetime.datetime.fromtimestamp(ts)
                datum = dt.strftime("%d.%m.%Y")
                uhrzeit = dt.strftime("%H:%M")
            except Exception:
                datum = uhrzeit = "?"
            ev = berechne_ev_score(s.get("konfidenz", 6), s.get("quote") or 1.85)
            status  = s.get("status", "offen")
            ergebnis = ("✅ GEWONNEN" if s.get("gewonnen") else
                        ("❌ VERLOREN" if s.get("status") == "ausgewertet" else "⏳ Offen"))
            werte = [datum, uhrzeit,
                     s.get("competition", s.get("liga", "?")),
                     s.get("home", "?"), s.get("away", "?"),
                     s.get("typ", "?"), s.get("tipp", s.get("typ", "?")),
                     s.get("quote", ""),
                     s.get("konfidenz", 6),
                     ev["ev_pct"],
                     status, ergebnis]
            for col, val in enumerate(werte, 1):
                ws.cell(row=row, column=col, value=val)
            # Farbe nach Ergebnis
            fill_color = ("C8E6C9" if "GEWONNEN" in ergebnis else
                          ("FFCDD2" if "VERLOREN" in ergebnis else "FFF9C4"))
            for col in range(1, len(header) + 1):
                ws.cell(row=row, column=col).fill = PatternFill("solid", fgColor=fill_color)
        # Spaltenbreite
        for col in ws.columns:
            ws.column_dimensions[col[0].column_letter].width = 16
        pfad = "/tmp/betlab_export.xlsx"
        wb.save(pfad)
        return pfad
    except ImportError:
        print("  [Export] openpyxl nicht installiert")
        return None
    except Exception as e:
        print(f"  [Export] Fehler: {e}")
        return None

def suche_signale(suchbegriff: str) -> str:
    """Durchsucht Signal-Archiv nach Team oder Liga."""
    suchbegriff = suchbegriff.lower().strip()
    with _tracker_lock:
        treffer = [s for s in _signal_tracker.values()
                   if (suchbegriff in (s.get("home", "") or "").lower() or
                       suchbegriff in (s.get("away", "") or "").lower() or
                       suchbegriff in (s.get("competition", "") or "").lower() or
                       suchbegriff in (s.get("liga", "") or "").lower())]
    if not treffer:
        return f"❌ Keine Signale gefunden für: {suchbegriff}"
    treffer_sort = sorted(treffer, key=lambda x: x.get("signal_zeit", 0), reverse=True)[:10]
    zeilen = [f"🔍 <b>Suchergebnisse: '{suchbegriff}'</b> ({len(treffer)} gesamt)\n━━━━━━━━━━━━━━━━━━━━"]
    for s in treffer_sort:
        import datetime
        ts = s.get("signal_zeit", 0)
        try:
            dt   = datetime.datetime.fromtimestamp(ts).strftime("%d.%m %H:%M")
        except Exception:
            dt   = "?"
        ergebnis = ("✅" if s.get("gewonnen") else
                    ("❌" if s.get("status") == "ausgewertet" else "⏳"))
        zeilen.append(f"{ergebnis} {dt} | {s.get('home','?')} vs {s.get('away','?')} | {s.get('typ','?')}")
    return "\n".join(zeilen)

# ============================================================
#  TELEGRAM INLINE-BUTTONS
# ============================================================

def send_telegram_mit_buttons(msg: str, match_id: str, typ: str) -> bool:
    """
    Sendet Telegram-Nachricht mit ✅/❌ Inline-Buttons für schnelle Auswertung.
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        keyboard = {
            "inline_keyboard": [[
                {"text": "✅ Gewonnen", "callback_data": f"won_{match_id}_{typ}"},
                {"text": "❌ Verloren", "callback_data": f"lost_{match_id}_{typ}"},
            ]]
        }
        import json
        resp = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg,
                  "parse_mode": "HTML", "reply_markup": json.dumps(keyboard)},
            timeout=10
        )
        return resp.status_code == 200
    except Exception as e:
        print(f"  [Buttons] Fehler: {e}")
        return False

def verarbeite_callback_query(callback_data: str, callback_query_id: str):
    """Verarbeitet Button-Drücke (Gewonnen/Verloren)."""
    try:
        teile = callback_data.split("_", 2)
        if len(teile) < 3:
            return
        aktion, match_id, typ = teile
        gewonnen = aktion == "won"
        key      = f"{match_id}_{typ}"
        if key in _signal_tracker:
            tracker_ausgewertet_markieren(key, gewonnen)
            update_statistik(typ, gewonnen, _signal_tracker[key].get("quote"))
            check_streak_alarm()
            antwort = "✅ Als Gewonnen markiert!" if gewonnen else "❌ Als Verloren markiert!"
        else:
            antwort = "⚠️ Signal nicht gefunden"
        # Callback bestätigen
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery",
            json={"callback_query_id": callback_query_id, "text": antwort},
            timeout=5
        )
    except Exception as e:
        print(f"  [Callback] Fehler: {e}")


# ============================================================
#  VERSCHLÜSSELUNG API KEYS
# ============================================================

def _xor_cipher(text: str, key: str = "BetlabLIVE2025") -> str:
    """Einfache XOR-Verschlüsselung für API Keys."""
    return "".join(chr(ord(c) ^ ord(key[i % len(key)])) for i, c in enumerate(text))

def verschluessel_key(key: str) -> str:
    import base64
    return base64.b64encode(_xor_cipher(key).encode("latin-1")).decode()

def entschluessel_key(enc: str) -> str:
    import base64
    return _xor_cipher(base64.b64decode(enc).decode("latin-1"))

def api_keys_sichern():
    """Speichert API Keys verschlüsselt in keys.enc."""
    import json
    keys = {
        "ls_key":     verschluessel_key(API_KEY),
        "odds_key":   verschluessel_key(ODDS_API_KEY or ""),
        "panda_key":  verschluessel_key(PANDASCORE_API_KEY or ""),
        "tg_token":   verschluessel_key(TELEGRAM_BOT_TOKEN),
    }
    try:
        with open("keys.enc", "w") as f:
            json.dump(keys, f)
        print("  [Keys] Verschlüsselt gespeichert in keys.enc")
    except Exception as e:
        print(f"  [Keys] Fehler: {e}")

# ============================================================
#  MEHRSPRACHIGER BOT
# ============================================================

_user_sprache = {}  # user_id → "de" oder "en"

TEXTE = {
    "signal_ecken_unter": {
        "de": "📐 <b>Ecken Signal!</b>",
        "en": "📐 <b>Corners Signal!</b>",
    },
    "gewonnen": {"de": "✅ GEWONNEN", "en": "✅ WON"},
    "verloren": {"de": "❌ VERLOREN", "en": "❌ LOST"},
    "onboarding": {
        "de": "🚀 <b>Willkommen bei BetlabLIVE!</b>",
        "en": "🚀 <b>Welcome to BetlabLIVE!</b>",
    },
}

def erkenne_sprache(text: str) -> str:
    """Erkennt Sprache anhand häufiger Wörter."""
    de_words = {"wie", "was", "der", "die", "das", "ich", "du", "ist", "hallo", "danke"}
    en_words = {"the", "what", "how", "is", "are", "hello", "thanks", "hi", "can", "you"}
    text_lower = text.lower()
    de_count = sum(1 for w in de_words if w in text_lower.split())
    en_count = sum(1 for w in en_words if w in text_lower.split())
    return "en" if en_count > de_count else "de"

def t(key: str, user_id: str = "") -> str:
    """Gibt Text in der Sprache des Users zurück."""
    sprache = _user_sprache.get(str(user_id), "de")
    return TEXTE.get(key, {}).get(sprache, TEXTE.get(key, {}).get("de", key))

# ============================================================
#  TELEGRAM INLINE-MENÜ
# ============================================================

HAUPT_MENU = {
    "inline_keyboard": [
        [
            {"text": "📊 Statistik", "callback_data": "menu_statistik"},
            {"text": "🔴 Live Signale", "callback_data": "menu_live"},
        ],
        [
            {"text": "📈 Chart", "callback_data": "menu_chart"},
            {"text": "💰 Bankroll", "callback_data": "menu_bankroll"},
        ],
        [
            {"text": "🔍 Auswertung", "callback_data": "menu_auswertung"},
            {"text": "🏆 Rangliste", "callback_data": "menu_rangliste"},
        ],
        [
            {"text": "🎯 Tipp abgeben", "callback_data": "menu_tipp_help"},
            {"text": "📋 Whitelist", "callback_data": "menu_whitelist"},
        ],
        [
            {"text": "🔎 Suche", "callback_data": "menu_suche_help"},
            {"text": "📤 Export", "callback_data": "menu_export"},
        ],
        [
            {"text": "🧩 Clustering", "callback_data": "menu_clustering"},
            {"text": "⚙️ System", "callback_data": "menu_system"},
        ],
    ]
}

MENU_ANTWORTEN = {
    "menu_statistik":    "/statistik",
    "menu_live":         "/live",
    "menu_chart":        "/chart",
    "menu_bankroll":     "/bankroll",
    "menu_auswertung":   "/auswertung",
    "menu_rangliste":    "/rangliste",
    "menu_clustering":   "/clustering",
    "menu_export":       "/export",
    "menu_tipp_help":    "📝 Tipp abgeben:\n/tipp [Spiel] [Bet] [Quote]\nBeispiel:\n/tipp ManCity Über2.5 1.85",
    "menu_whitelist":    "📋 Whitelist Commands:\n/whitelist on/off\n/whitelist liga [Name]\n/whitelist team [Name]\n/whitelist reset",
    "menu_suche_help":   "🔎 Signal-Archiv durchsuchen:\n/suche [Teamname]\n/suche [Liga]\nBeispiel:\n/suche Manchester",
    "menu_system":       "⚙️ System Commands:\n/status – Bot-Status\n/api – API-Monitor\n/pause – Bot pausieren\n/kalibrierung – Konfidenz-Check\n/gegner – Liga-Analyse",
}

def sende_hauptmenu(chat_id: str):
    import json
    msg = ("🤖 <b>BetlabLIVE Menü</b>\n━━━━━━━━━━━━━━━━━━━━\n"
           "Wähle eine Option:")
    requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
        json={"chat_id": chat_id, "text": msg,
              "parse_mode": "HTML",
              "reply_markup": json.dumps(HAUPT_MENU)},
        timeout=10
    )

# ============================================================
#  WETTMARKT-EFFIZIENZ
# ============================================================

def berechne_markt_effizienz() -> dict:
    """
    Misst wie oft der Value-Bot echte Fehler findet die aufgehen.
    Vergleicht EV-Score mit tatsächlichem Ergebnis.
    """
    with _tracker_lock:
        value_tipps = [s for s in _signal_tracker.values()
                       if s.get("typ") in ("value", "arbitrage")
                       and s.get("status") == "ausgewertet"
                       and s.get("ev") is not None]
    if len(value_tipps) < 10:
        return {"ausreichend": False}
    # Gruppiere nach EV-Kategorien
    kategorien = {
        "sehr_gut":    [s for s in value_tipps if s.get("ev", 0) >= 0.15],
        "gut":         [s for s in value_tipps if 0.08 <= s.get("ev", 0) < 0.15],
        "grenzwertig": [s for s in value_tipps if 0 <= s.get("ev", 0) < 0.08],
    }
    ergebnis = {}
    for kat, tipps in kategorien.items():
        if not tipps:
            continue
        gw   = sum(1 for t in tipps if t.get("gewonnen"))
        ges  = len(tipps)
        pct  = round(gw / ges * 100)
        roi  = round(sum(
            (t.get("quote", 1) - 1) if t.get("gewonnen") else -1
            for t in tipps
        ) / ges * 100, 1)
        ergebnis[kat] = {"pct": pct, "roi": roi, "tipps": ges}
    return {"ausreichend": True, "kategorien": ergebnis}

# ============================================================
#  GEGNERMODELL V2 – HOME/AWAY SPLIT
# ============================================================

def erstelle_team_profil_v2(team_id: str, team_name: str) -> dict:
    """
    Erweitertes Teamprofil mit Home/Away Split.
    Erkennt Teams die Auswärts deutlich schwächer sind.
    """
    basis = erstelle_team_profil(team_id, team_name)
    try:
        params = {**LS_AUTH, "team_id": team_id, "number": 12}
        resp   = api_get_with_retry(f"{LS_BASE}/matches/history.json", params)
        spiele = resp.json().get("data", {}).get("match", []) or []
        heim_siege  = auswarts_siege  = 0
        heim_spiele = auswarts_spiele = 0
        heim_tore   = auswarts_tore   = []
        for m in spiele:
            home_id = str((m.get("home") or {}).get("id", ""))
            ist_heim = home_id == team_id
            score    = (m.get("scores") or {}).get("score", "")
            h, a     = parse_score(score)
            winner   = str((m.get("winner") or {}).get("id", ""))
            gewonnen = winner == team_id
            tore     = h if ist_heim else a
            if ist_heim:
                heim_spiele += 1
                heim_tore.append(tore)
                if gewonnen:
                    heim_siege += 1
            else:
                auswarts_spiele += 1
                auswarts_tore.append(tore)
                if gewonnen:
                    auswarts_siege += 1
        heim_rate  = round(heim_siege  / max(heim_spiele,  1) * 100)
        ausw_rate  = round(auswarts_siege / max(auswarts_spiele, 1) * 100)
        heim_tore_avg = round(sum(heim_tore) / max(len(heim_tore), 1), 1)
        ausw_tore_avg = round(sum(auswarts_tore) / max(len(auswarts_tore), 1), 1)
        heim_schwäche = ausw_rate < heim_rate - 25  # Auswärts deutlich schwächer
        return {
            **basis,
            "heim_siegrate":  heim_rate,
            "ausw_siegrate":  ausw_rate,
            "heim_tore_avg":  heim_tore_avg,
            "ausw_tore_avg":  ausw_tore_avg,
            "reisemüdigkeit": heim_schwäche,
            "split_text": (f"🏠 Heim: {heim_rate}% | ✈️ Auswärts: {ausw_rate}%"
                           + (" ⚠️ Auswärtsschwäche!" if heim_schwäche else "")),
        }
    except Exception as e:
        print(f"  [GegnermodellV2] Fehler: {e}")
        return basis

# ============================================================
#  HEDGE-ALARM
# ============================================================

def bot_hedge_alarm():
    """
    Überwacht offene Tipps und empfiehlt Hedge wenn:
    - Gegentipp-Quote stark gefallen ist (Gewinn sichern)
    - Oder Signal-Quote stark gestiegen ist (Verlust minimieren)
    """
    print("[Hedge-Alarm-Bot] Gestartet | Hedging-Empfehlungen")
    while True:
        try:
            offene = tracker_get_offene()
            if not ODDS_API_KEY or not offene:
                time.sleep(5 * 60)
                continue
            for key, sig in offene:
                home      = sig.get("home", "")
                away      = sig.get("away", "")
                orig_q    = sig.get("quote")
                if not orig_q or orig_q <= 1.0:
                    continue
                # Aktuelle Quoten holen
                details   = get_quote_details(home, away)
                aktuelle_q = details.get("avg_quote", 0)
                if not aktuelle_q or aktuelle_q <= 1.0:
                    continue
                # Quote stark gestiegen → Tipp schlechter geworden (Gegentipp gesunken)
                anstieg = round((aktuelle_q - orig_q) / orig_q * 100, 1)
                if anstieg >= 25:
                    gegentipp_q = round(1 / (1 - 1/aktuelle_q), 2) if aktuelle_q > 1 else 0
                    einsatz     = EINSATZ
                    hedge_eins  = round(einsatz / aktuelle_q * gegentipp_q, 2)
                    gewinn_wenn = round(einsatz * orig_q - einsatz - hedge_eins, 2)
                    msg = (f"🛡️ <b>Hedge-Empfehlung!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                           f"📌 {home} vs {away}\n"
                           f"🎯 Dein Tipp: {sig.get('typ','?')}\n"
                           f"━━━━━━━━━━━━━━━━━━━━\n"
                           f"💶 Orig. Quote: <b>{orig_q}</b>\n"
                           f"📈 Aktuelle Quote: <b>{aktuelle_q}</b> (+{anstieg}%)\n"
                           f"🛡️ Hedge-Einsatz: <b>{hedge_eins}€</b> auf Gegentipp\n"
                           f"✅ Gesicherter Gewinn: <b>+{gewinn_wenn}€</b>\n"
                           f"━━━━━━━━━━━━━━━━━━━━\n🕐 {jetzt()} Uhr")
                    send_telegram(msg)
                    print(f"  [Hedge] ✅ {home} vs {away} | Quote +{anstieg}%")
            bot_fehler_reset("Hedge-Alarm-Bot")
        except Exception as e:
            bot_fehler_melden("Hedge-Alarm-Bot", e)
        time.sleep(10 * 60)

# ============================================================
#  QUOTENVERGLEICH-BOT (täglich)
# ============================================================

def bot_quotenvergleich():
    """
    Vergleicht täglich um 08:30 Uhr die besten Quoten über 10+ Bookmaker
    für die Top-Spiele des Tages.
    """
    print("[Quotenvergleich-Bot] Gestartet | Täglich 08:30 Uhr")
    gesendet_qv = set()
    while True:
        try:
            now = de_now()
            key = now.strftime("%Y-%m-%d")
            if now.hour == 8 and now.minute >= 30 and now.minute < 35 and key not in gesendet_qv:
                gesendet_qv.add(key)
                if not ODDS_API_KEY:
                    time.sleep(60)
                    continue
                url    = "https://api.the-odds-api.com/v4/sports/soccer/odds/"
                params = {"apiKey": ODDS_API_KEY, "regions": "eu,uk",
                          "markets": "h2h,totals", "oddsFormat": "decimal"}
                resp   = requests.get(url, params=params, timeout=10)
                if resp.status_code != 200:
                    continue
                top_werte = []
                for game in resp.json()[:8]:
                    home_t = game.get("home_team", "?")
                    away_t = game.get("away_team", "?")
                    # Beste Gesamtquote pro Spiel
                    beste_bm   = None
                    beste_q    = 0
                    markt_name = ""
                    for bm in game.get("bookmakers", []):
                        for market in bm.get("markets", []):
                            for outcome in market.get("outcomes", []):
                                q = outcome.get("price", 0)
                                if q > beste_q:
                                    beste_q    = q
                                    beste_bm   = bm.get("title", "?")
                                    markt_name = f"{outcome.get('name','')} {outcome.get('point','')}"
                    if beste_q >= 1.8:
                        top_werte.append({
                            "spiel": f"{home_t} vs {away_t}",
                            "q": beste_q, "bm": beste_bm, "markt": markt_name
                        })
                if not top_werte:
                    continue
                top_werte.sort(key=lambda x: x["q"], reverse=True)
                zeilen = "\n".join([
                    f"⭐ {v['spiel']}\n   💶 {v['q']} @ {v['bm']} ({v['markt']})"
                    for v in top_werte[:5]
                ])
                msg = (f"💹 <b>Beste Quoten heute – {now.strftime('%d.%m.%Y')}</b>\n"
                       f"━━━━━━━━━━━━━━━━━━━━\n"
                       f"{zeilen}\n"
                       f"━━━━━━━━━━━━━━━━━━━━\n"
                       f"🕐 {jetzt()} Uhr | Quelle: The Odds API")
                send_telegram(msg)
                print(f"  [Quotenvergleich] ✅ {len(top_werte)} Spiele gesendet")
            bot_fehler_reset("Quotenvergleich-Bot")
        except Exception as e:
            bot_fehler_melden("Quotenvergleich-Bot", e)
        time.sleep(60)

# ============================================================
#  BONUS-TRACKER
# ============================================================

BOOKIE_AKTIONEN = {
    "Leonbet":    {"typ": "keine Steuer",  "info": "Keine Wettsteuer, bis zu 5% Cashback"},
    "Bet365":     {"typ": "Freiwetten",    "info": "Regelmäßige Freiwetten für aktive Kunden"},
    "Bwin":       {"typ": "Boost",         "info": "Quoten-Boosts auf ausgewählte Spiele"},
    "Unibet":     {"typ": "Cashback",      "info": "Cashback auf erste verlorene Wette"},
    "Betway":     {"typ": "Freiwette",     "info": "Freiwetten für Neukunden"},
    "20Bet":      {"typ": "Reload",        "info": "Wöchentlicher Reload Bonus"},
    "Pinnacle":   {"typ": "Keine Boni",    "info": "Kein Bonus aber beste Quoten weltweit"},
    "Winamax":    {"typ": "Steuerfrei",    "info": "97% Quotenschlüssel, steuerfrei in DE"},
    "PlayZilla":  {"typ": "Willkommen",    "info": "25-32 Ecken-Märkte, Willkommensbonus"},
}

BONUS_ERINNERUNG_TAG = 3  # Montag, Mittwoch, Freitag

def bot_bonus_tracker():
    """Erinnert an Bookmaker-Aktionen montags, mittwochs und freitags."""
    print("[Bonus-Tracker] Gestartet | Erinnerungen Mo/Mi/Fr")
    gesendet_bonus = set()
    while True:
        try:
            now = de_now()
            key = now.strftime("%Y-%m-%d")
            # Montag=0, Mittwoch=2, Freitag=4
            if now.weekday() in (0, 2, 4) and now.hour == 9 and now.minute < 5 and key not in gesendet_bonus:
                gesendet_bonus.add(key)
                zeilen = []
                for bm, data in BOOKIE_AKTIONEN.items():
                    zeilen.append(f"📌 <b>{bm}</b> [{data['typ']}]\n   {data['info']}")
                msg = (f"🎁 <b>Bookmaker Aktionen – {now.strftime('%d.%m.%Y')}</b>\n"
                       f"━━━━━━━━━━━━━━━━━━━━\n"
                       + "\n".join(zeilen) +
                       f"\n━━━━━━━━━━━━━━━━━━━━\n"
                       f"💡 Immer die besten Konditionen nutzen!\n"
                       f"⚠️ 18+ | Verantwortungsvoll spielen")
                send_telegram(msg)
                print(f"  [Bonus-Tracker] ✅ Aktionen gesendet")
            bot_fehler_reset("Bonus-Tracker")
        except Exception as e:
            bot_fehler_melden("Bonus-Tracker", e)
        time.sleep(60)

# ============================================================
#  COMPOUND BANKROLL (Kelly-Simulation)
# ============================================================

def simuliere_compound_bankroll(startkapital: float = None,
                                 wochen: int = 20) -> dict:
    """
    Simuliert Bankroll-Wachstum bei automatischem Reinvestieren (Compound).
    Nutzt Kelly-Kriterium und historische Trefferquote.
    """
    if startkapital is None:
        startkapital = bankroll_laden()
    gw  = sum(statistik[t]["gewonnen"] for t in statistik)
    vl  = sum(statistik[t]["verloren"] for t in statistik)
    ges = gw + vl
    if ges < 10:
        return {"ausreichend": False}
    p      = gw / ges
    q_avg  = 1.85  # Durchschnittsquote
    kelly  = max(0, (p * q_avg - 1) / (q_avg - 1))
    kelly  = min(kelly, 0.05)  # Max 5% pro Wette
    tipps_pro_woche = max(1, ges // max(1, (de_now().isocalendar()[1])))
    bankroll = startkapital
    verlauf  = [round(bankroll, 2)]
    for woche in range(wochen):
        for _ in range(tipps_pro_woche):
            einsatz  = bankroll * kelly
            gewonnen = (gw / ges) > 0.5
            if gw / ges > 0.5:
                bankroll += einsatz * (q_avg - 1)
            else:
                bankroll -= einsatz
            bankroll = max(1, bankroll)
        verlauf.append(round(bankroll, 2))
    rendite = round((bankroll - startkapital) / startkapital * 100, 1)
    return {
        "ausreichend":   True,
        "start":         round(startkapital, 2),
        "end":           round(bankroll, 2),
        "rendite_pct":   rendite,
        "kelly_pct":     round(kelly * 100, 1),
        "wochen":        wochen,
        "verlauf":       verlauf,
        "trefferquote":  round(p * 100, 1),
    }

# ============================================================
#  KONFIDENZ-KALIBRIERUNG
# ============================================================

def kalibriere_konfidenz():
    """
    Prüft ob Konfidenz-Levels zur echten Trefferquote passen.
    Konfidenz 8/10 sollte ~80% Trefferquote haben.
    Passt KONFIDENZ_KALIBRIERUNG dict an wenn nötig.
    """
    with _tracker_lock:
        alle = [s for s in _signal_tracker.values()
                if s.get("status") == "ausgewertet" and s.get("konfidenz")]
    if len(alle) < 100:
        return {}
    # Gruppiere nach Konfidenz-Level
    nach_konfidenz = {}
    for s in alle:
        k = s.get("konfidenz", 6)
        if k not in nach_konfidenz:
            nach_konfidenz[k] = {"gewonnen": 0, "total": 0}
        nach_konfidenz[k]["total"] += 1
        if s.get("gewonnen"):
            nach_konfidenz[k]["gewonnen"] += 1
    kalibrierung = {}
    meldungen    = []
    for k, data in sorted(nach_konfidenz.items()):
        if data["total"] < 10:
            continue
        echte_quote = round(data["gewonnen"] / data["total"] * 100)
        erwartet    = k * 10
        abweichung  = echte_quote - erwartet
        kalibrierung[k] = {"echt": echte_quote, "erwartet": erwartet, "abweichung": abweichung}
        if abs(abweichung) >= 15:
            if abweichung < 0:
                meldungen.append(f"⚠️ Konfidenz {k}/10: erwartet {erwartet}%, echt nur {echte_quote}% → zu optimistisch")
            else:
                meldungen.append(f"✅ Konfidenz {k}/10: erwartet {erwartet}%, echt {echte_quote}% → unterschätzt")
    if meldungen:
        msg = ("🧠 <b>Konfidenz-Kalibrierung</b>\n━━━━━━━━━━━━━━━━━━━━\n"
               + "\n".join(meldungen) +
               f"\n━━━━━━━━━━━━━━━━━━━━\n🕐 {jetzt()} Uhr")
        send_telegram(msg)
    return kalibrierung

# ============================================================
#  TIPP-QUALITÄT-SCORE (Expected Value)
# ============================================================

def berechne_ev_score(konfidenz: int, quote: float) -> dict:
    """
    Berechnet Expected Value Score: EV = (P × Quote) - 1
    P = geschätzte Wahrscheinlichkeit aus Konfidenz-Level.
    EV > 0 = positiver Erwartungswert = gutes Signal.
    """
    if not quote or quote <= 1.0:
        return {"ev": 0, "label": "–", "empfohlen": False}
    p         = konfidenz / 10
    ev        = round((p * quote) - 1, 3)
    ev_pct    = round(ev * 100, 1)
    if ev >= 0.15:
        label = f"💎 Sehr gut ({ev_pct}%)"
        empfohlen = True
    elif ev >= 0.08:
        label = f"✅ Gut ({ev_pct}%)"
        empfohlen = True
    elif ev >= 0.0:
        label = f"🟡 Grenzwertig ({ev_pct}%)"
        empfohlen = False
    else:
        label = f"❌ Negativ ({ev_pct}%)"
        empfohlen = False
    return {"ev": ev, "ev_pct": ev_pct, "label": label, "empfohlen": empfohlen}

# ============================================================
#  TAGESZEIT-ANALYSE & GEGNER-ANALYSE
# ============================================================

def analysiere_tageszeit() -> str:
    """Analysiert zu welchen Uhrzeiten der Bot am erfolgreichsten ist."""
    with _tracker_lock:
        alle = [s for s in _signal_tracker.values()
                if s.get("status") == "ausgewertet" and s.get("signal_zeit")]
    if len(alle) < 20:
        return "Zu wenig Daten (mind. 20 Tipps nötig)"
    stunden = {}
    for s in alle:
        try:
            import datetime
            h = datetime.datetime.fromtimestamp(s["signal_zeit"]).hour
            block = f"{h:02d}:00"
            if block not in stunden:
                stunden[block] = {"g": 0, "total": 0}
            stunden[block]["total"] += 1
            if s.get("gewonnen"):
                stunden[block]["g"] += 1
        except Exception:
            continue
    if not stunden:
        return ""
    zeilen = []
    for h, d in sorted(stunden.items()):
        if d["total"] < 3:
            continue
        pct = round(d["g"] / d["total"] * 100)
        bar = "🟢" if pct >= 60 else ("🟡" if pct >= 45 else "🔴")
        zeilen.append(f"{bar} {h}: {pct}% ({d['g']}/{d['total']})")
    beste = max(stunden.items(), key=lambda x: x[1]["g"]/max(x[1]["total"],1) if x[1]["total"]>=3 else 0)
    return ("⏰ <b>Tageszeit-Analyse</b>\n" +
            "\n".join(zeilen[:8]) +
            f"\n⭐ Beste Zeit: <b>{beste[0]}</b>")

def analysiere_gegner() -> str:
    """Analysiert gegen welche Team-Typen der Bot am besten performt."""
    with _tracker_lock:
        alle = [s for s in _signal_tracker.values()
                if s.get("status") == "ausgewertet"]
    if len(alle) < 30:
        return "Zu wenig Daten"
    # Liga-Analyse
    ligen = {}
    for s in alle:
        liga = s.get("competition", s.get("liga", "Unbekannt"))
        if not liga or liga == "?":
            continue
        if liga not in ligen:
            ligen[liga] = {"g": 0, "total": 0}
        ligen[liga]["total"] += 1
        if s.get("gewonnen"):
            ligen[liga]["g"] += 1
    if not ligen:
        return ""
    # Top 5 Ligen nach Trefferquote
    sortiert = sorted(
        [(l, d) for l, d in ligen.items() if d["total"] >= 5],
        key=lambda x: x[1]["g"] / x[1]["total"],
        reverse=True
    )
    beste  = sortiert[:3]
    schlechteste = sortiert[-3:] if len(sortiert) >= 6 else []
    zeilen = ["🏆 <b>Top-Ligen:</b>"]
    for liga, d in beste:
        pct = round(d["g"] / d["total"] * 100)
        zeilen.append(f"  ✅ {liga}: {pct}% ({d['g']}/{d['total']})")
    if schlechteste:
        zeilen.append("⚠️ <b>Schwache Ligen:</b>")
        for liga, d in reversed(schlechteste):
            pct = round(d["g"] / d["total"] * 100)
            zeilen.append(f"  ❌ {liga}: {pct}% ({d['g']}/{d['total']})")
    return "\n".join(zeilen)

# ============================================================
#  ONBOARDING & ERKLÄRUNGEN
# ============================================================

BEKANNTE_USER = set()
BEKANNTE_DATEI = "bekannte_user.json"

def bekannte_user_laden():
    import json, os
    global BEKANNTE_USER
    if not os.path.exists(BEKANNTE_DATEI):
        return
    try:
        with open(BEKANNTE_DATEI) as f:
            BEKANNTE_USER = set(json.load(f))
    except Exception:
        pass

def bekannte_user_speichern():
    import json
    try:
        with open(BEKANNTE_DATEI, "w") as f:
            json.dump(list(BEKANNTE_USER), f)
    except Exception:
        pass

SIGNAL_ERKLAERUNGEN = {
    "ecken":    ("📐 <b>Ecken-Unter Bot</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                 "Formel: HZ1 Ecken × 2 + 1 als Gesamt-Grenze.\n"
                 "Beispiel: 5 Ecken in HZ1 → Tipp Unter 11 Ecken gesamt.\n"
                 "Warum? Spiele mit ruhiger HZ1 bleiben oft ruhig."),
    "karten":   ("🃏 <b>Karten-Bot</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                 "Signal wenn bis zur 40. Minute bereits 2+ Karten gezeigt wurden.\n"
                 "Tipp: Über 5 Karten im Gesamtspiel.\n"
                 "Warum? Frühes hitziges Spiel eskaliert meist weiter."),
    "druck":    ("🔥 <b>Druck-Bot</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                 "Signal wenn ein Team mind. 6 Ecken und 2.5× mehr als der Gegner hat.\n"
                 "Tipp: Nächste Ecke / Tor für das dominierende Team.\n"
                 "Warum? Anhaltender Druck führt statistisch zu Toren."),
    "comeback": ("🔄 <b>Comeback-Bot</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                 "Signal wenn ein Team hinten liegt aber die Statistiken dominiert.\n"
                 "Tipp: Beide Teams treffen (Rückstand wird aufgeholt).\n"
                 "Warum? Dominante Teams holen Rückstände häufig auf."),
    "torwart":  ("🧤 <b>Torwart-Bot</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                 "Signal bei 0:0 ab Minute 20 mit mind. 3 Schüssen aufs Tor.\n"
                 "Tipp: Mind. 1 Tor fällt noch.\n"
                 "Warum? Viele Schüsse ohne Tor sind statistisch untypisch."),
    "value":    ("💎 <b>Value Bet Finder</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                 "Vergleicht Quoten verschiedener Bookmaker – findet Ausreißer >12%.\n"
                 "Warum? Quotenfehler sind kurzfristig lukrativ zu nutzen."),
    "xg":       ("📊 <b>xG-Bot</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                 "Signal wenn Expected Goals um 1.5+ höher liegt als echte Tore.\n"
                 "Warum? Das Spiel hat Tore verdient die noch kommen können."),
}
ONBOARDING_NACHRICHT = """🚀 <b>Willkommen bei BetlabLIVE!</b>
━━━━━━━━━━━━━━━━━━━━
Ich bin dein persönlicher Wett-Assistent. Hier eine kurze Übersicht:

🤖 <b>Was ich tue:</b>
Ich analysiere Live-Spiele und sende dir Signale wenn ich gute Wettchancen erkenne – vollautomatisch 24/7.

📊 <b>Signal-Typen:</b>
📐 Ecken-Bot | 🃏 Karten-Bot | 🧤 Torwart-Bot
🔥 Druck-Bot | 🔄 Comeback-Bot | 💎 Value-Bot
⚡ Early Goal | 📊 xG-Bot | und mehr...

📱 <b>Nützliche Commands:</b>
/statistik – deine aktuelle Bilanz
/live – aktive Signale jetzt
/chart – Performance-Diagramm
/erklaer [Bot] – z.B. /erklaer ecken
/tipp [Spiel] [Bet] [Quote] – manuell tippen

━━━━━━━━━━━━━━━━━━━━
⚠️ 18+ | Verantwortungsvoll spielen
Fragen? Schreib einfach! 🎯"""

# ============================================================
#  MORGEN-ÜBERSICHT (08:00 Uhr)
# ============================================================

def bot_morgen_uebersicht():
    """Sendet täglich um 08:00 eine Übersicht des Tages."""
    print("[Morgen-Bot] Gestartet | Täglich 08:00 Uhr")
    gesendet_morgen = set()
    while True:
        try:
            now = de_now()
            key = now.strftime("%Y-%m-%d")
            if now.hour == 8 and now.minute < 5 and key not in gesendet_morgen:
                gesendet_morgen.add(key)
                datum    = now.strftime("%Y-%m-%d")
                fixtures = ls_get_fixtures(datum)
                top      = filtere_top_spiele(fixtures)
                if not top:
                    time.sleep(60)
                    continue
                # Ligen gruppieren
                ligen = {}
                for f in top:
                    liga    = f.get("competition", {}).get("name", "?")
                    anstoß  = f.get("time", "?")
                    home    = f.get("home_name") or (f.get("home") or {}).get("name", "?")
                    away    = f.get("away_name") or (f.get("away") or {}).get("name", "?")
                    if liga not in ligen:
                        ligen[liga] = []
                    ligen[liga].append(f"  {anstoß}: {home} vs {away}")
                liga_text = ""
                for liga, spiele in list(ligen.items())[:6]:
                    liga_text += f"\n🏆 <b>{liga}</b>\n" + "\n".join(spiele[:3]) + "\n"
                # Wetter-Check für erste Liga
                erste_liga_land = (top[0].get("country") or {}).get("name", "") if top else ""
                wetter_text = ""
                if erste_liga_land:
                    wa = wetter_analyse(erste_liga_land)
                    if wa.get("info"):
                        wetter_text = "\n" + "\n".join(wa["info"])
                # Gestern Bilanz
                gw = sum(statistik[t]["gewonnen"] for t in statistik)
                vl = sum(statistik[t]["verloren"] for t in statistik)
                msg = (f"🌅 <b>Guten Morgen! Tages-Übersicht {now.strftime('%d.%m.%Y')}</b>\n"
                       f"━━━━━━━━━━━━━━━━━━━━\n"
                       f"📅 <b>Heutige Top-Spiele:</b>{liga_text}"
                       f"━━━━━━━━━━━━━━━━━━━━\n"
                       f"📊 Gestern: <b>{gw}✅ {vl}❌</b>{wetter_text}\n"
                       f"━━━━━━━━━━━━━━━━━━━━\n"
                       f"🤖 Der Bot ist aktiv und analysiert alle Spiele!\n"
                       f"⭐ Tipp des Tages kommt um 09:00 Uhr")
                send_telegram(msg)
                send_telegram_gruppe(msg)
                print(f"  [Morgen-Bot] ✅ Übersicht gesendet – {len(top)} Spiele heute")
        except Exception as e:
            print(f"  [Morgen-Bot] Fehler: {e}")
        time.sleep(60)




def discord_vote_auswerten(signal_key: str, gewonnen: bool):
    """Wertet alle Votes für ein Signal aus."""
    votes = _discord_votes.get(signal_key, {})
    for uid, vote in votes.items():
        richtig = (vote == "ja" and gewonnen) or (vote == "nein" and not gewonnen)
        if uid not in _discord_punkte:
            _discord_punkte[uid] = {"punkte": 0, "gewinne": 0, "verluste": 0, "name": "?"}
        d = _discord_punkte[uid]
        if richtig:
            d["punkte"] += 10; d["gewinne"] += 1
        else:
            d["punkte"] = max(0, d["punkte"] - 3); d["verluste"] += 1
    discord_votes_speichern()

_discord_votes  = {}
_discord_punkte = {}
DISCORD_VOTE_DATEI = "discord_votes.json"

def discord_votes_laden():
    import json, os
    global _discord_votes, _discord_punkte
    for datei, var in [(DISCORD_VOTE_DATEI, "_discord_votes"), ("discord_punkte.json", "_discord_punkte")]:
        if os.path.exists(datei):
            try:
                with open(datei) as f:
                    d = json.load(f)
                if var == "_discord_votes": _discord_votes = d
                else: _discord_punkte = d
            except Exception: pass

def discord_votes_speichern():
    import json
    for datei, daten in [(DISCORD_VOTE_DATEI, _discord_votes), ("discord_punkte.json", _discord_punkte)]:
        try:
            with open(datei, "w") as f: json.dump(daten, f)
        except Exception: pass

def sende_discord_rangliste():
    if not _discord_punkte: return
    sortiert = sorted(_discord_punkte.items(), key=lambda x: x[1].get("punkte",0), reverse=True)[:10]
    medals = ["🥇","🥈","🥉"]
    felder = []
    for i, (uid, d) in enumerate(sortiert):
        m = medals[i] if i < 3 else f"{i+1}."
        felder.append({"name": f"{m} {d.get('name','User')}", "value": f"{d['punkte']} Pkt | {d['gewinne']}✅ {d['verluste']}❌", "inline": True})
    embed = {"title": "🏆 Discord Rangliste", "color": 0xFFD700, "fields": felder, "description": "Stimme auf Signale ab!\n✅ Richtig = +10 | ❌ Falsch = -3", "footer": {"text": f"BetlabLIVE • {heute()}"}}
    send_discord_embed(DISCORD_WEBHOOK_BILANZ, embed)

# ============================================================
#  🏆 MEGA RANG & BELOHNUNGS-SYSTEM v51
# ============================================================

# ── XP-Quellen ─────────────────────────────────────────────
XP_QUELLEN = {
    "tipp_gewonnen":     50,
    "tipp_verloren":     10,   # Teilnahme belohnen
    "daily_checkin":     20,
    "einladung":        200,
    "streak_5":          50,
    "streak_10":        150,
    "streak_20":        500,
    "challenge_gewon":  500,
    "challenge_teiln":   50,
    "woche_gewonnen":   100,
    "monat_top3":       300,
    "erster_tipp":       25,
    "100_tipps":        250,
    "sniper_unlock":    200,
}

# ── Level-Stufen ────────────────────────────────────────────
LEVEL_STUFEN = [
    (0,     1,  "🆕 Newcomer"),
    (100,   2,  "🎯 Einsteiger"),
    (300,   3,  "📊 Tipper"),
    (600,   4,  "🔍 Analyst"),
    (1000,  5,  "🥉 Bronze Tipster"),
    (1500,  6,  "📈 Fortgeschrittener"),
    (2200,  7,  "🎲 Tipp-Profi"),
    (3000,  8,  "🥈 Silber Tipster"),
    (4000,  9,  "🔥 Heißer Draht"),
    (5500,  10, "🥇 Gold Tipster"),
    (7500,  11, "💡 Stratege"),
    (10000, 12, "⚡ Streak Hunter"),
    (13000, 13, "🎯 Sniper"),
    (17000, 14, "💎 Diamond Tipster"),
    (22000, 15, "🌟 Superstar"),
    (28000, 16, "🏅 Elite Analyst"),
    (35000, 17, "💠 Platinum Tipster"),
    (45000, 18, "🔮 Oracle"),
    (60000, 19, "👑 Legend"),
    (80000, 20, "🌍 Hall of Fame"),
]

# ── Discord Rollen (basierend auf Gewinnen) ─────────────────
DISCORD_ROLLEN = [
    (200, "🌍 Hall of Fame",    0xFFD700),
    (100, "👑 Legend",          0xFF6B00),
    (75,  "💠 Platinum",        0x7DF9FF),
    (50,  "💎 Diamond Tipster", 0x00BFFF),
    (30,  "🥇 Gold Tipster",    0xFFD700),
    (15,  "🥈 Silber Tipster",  0xC0C0C0),
    (5,   "🥉 Bronze Tipster",  0xCD7F32),
    (0,   "🆕 Newcomer",        0x808080),
]

SPEZIAL_ROLLEN = {
    "sniper":       ("🎯 Sniper",        "75%+ Trefferquote bei 20+ Tipps"),
    "streak_5":     ("🔥 On Fire",       "5 Tipps in Folge gewonnen"),
    "streak_10":    ("⚡ Streak Master", "10 Tipps in Folge gewonnen"),
    "streak_20":    ("💥 Unstoppable",   "20 Tipps in Folge gewonnen"),
    "whale":        ("🐋 Whale",         "50+ Tipps mit 60%+ Quote"),
    "ambassador":   ("🤝 Ambassador",    "10+ Einladungen"),
    "champion":     ("🏆 Champion",      "Monatliche Challenge gewonnen"),
    "veteran":      ("⚔️ Veteran",       "100+ Tipps insgesamt"),
    "analyst":      ("📊 Analyst",       "50+ Tipps insgesamt"),
    "early_bird":   ("🐦 Early Bird",    "Einer der ersten 100 Mitglieder"),
}

# ── Invite Feature-Unlock ───────────────────────────────────
INVITE_FEATURES = {
    0:  {"name": "🔓 Basis-Signale",        "beschreibung": "Alle Live-Signale im Discord"},
    1:  {"name": "⭐ VIP Channel",           "beschreibung": "Zugang zum exklusiven VIP-Channel"},
    2:  {"name": "📊 Wöchentlicher Report",  "beschreibung": "Persönlicher Tipp-Report jeden Montag"},
    3:  {"name": "🤖 Bot-Analyse",           "beschreibung": "Persönliche Analyse deiner Tipps vom Bot"},
    5:  {"name": "💎 Premium (1 Monat)",     "beschreibung": "Alle Premium-Features für 1 Monat"},
    8:  {"name": "🔒 Insider-Channel",       "beschreibung": "Exklusive Vorschau auf kommende Signale"},
    10: {"name": "♾️ Lifetime-Mitglied",     "beschreibung": "Dauerhafter kostenloser Zugang"},
    15: {"name": "📢 Bot-Shoutout",          "beschreibung": "Bot nennt deinen Namen bei Signalen"},
    20: {"name": "🛡️ Co-Admin",              "beschreibung": "Admin-Rechte im Discord"},
    25: {"name": "🎯 Custom Alerts",         "beschreibung": "Persönliche Alert-Einstellungen"},
    30: {"name": "💎 Partner Badge",         "beschreibung": "Offizieller Partner-Status"},
    40: {"name": "🎙️ Eigener Channel",       "beschreibung": "Dein eigener Channel im Discord"},
    50: {"name": "🏆 Hall of Fame",          "beschreibung": "Platz in der ewigen Bestenliste"},
    75: {"name": "🤝 Revenue Share",         "beschreibung": "Anteil an Affiliate-Einnahmen"},
    100:{"name": "👑 Co-Founder Badge",      "beschreibung": "Offizieller Mitgründer-Status"},
}

# ── Daily Check-in ──────────────────────────────────────────
_checkin_heute = {}  # user_id → datum

def mache_daily_checkin(user_id: str, username: str) -> str:
    """Täglicher Check-in für XP-Bonus."""
    heute = de_now().strftime("%Y-%m-%d")
    if _checkin_heute.get(user_id) == heute:
        return f"⏰ Du hast heute bereits eingecheckt! Komm morgen wieder für +{XP_QUELLEN['daily_checkin']} XP."
    _checkin_heute[user_id] = heute
    xp   = XP_QUELLEN["daily_checkin"]
    xp_s = gib_xp(user_id, username, xp, "daily_checkin")
    streak_d = xp_s.get("checkin_streak", 1)
    bonus = ""
    if streak_d == 7:
        gib_xp(user_id, username, 100, "woche_7tage")
        bonus = "\n🎉 7-Tage-Streak Bonus: +100 XP!"
    elif streak_d == 30:
        gib_xp(user_id, username, 500, "monat_30tage")
        bonus = "\n🏆 30-Tage-Streak Bonus: +500 XP!"
    return (f"✅ <b>Check-in erfolgreich!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"👤 {username}\n"
            f"⭐ +{xp} XP verdient{bonus}\n"
            f"📅 Tag-Streak: {streak_d}\n"
            f"📊 Gesamt-XP: {xp_s.get('xp', 0):,}\n"
            f"🏅 Level: {xp_s.get('level_name', '?')}")

# ── XP System ───────────────────────────────────────────────
RANG_DATEI = "rang_system.json"
_rang_daten = {}  # user_id → {xp, level, gewinne, checkins...}

def rang_laden():
    import json, os
    global _rang_daten
    if os.path.exists(RANG_DATEI):
        try:
            with open(RANG_DATEI) as f:
                _rang_daten = json.load(f)
            print(f"  [Rang] {len(_rang_daten)} User geladen")
        except Exception as e:
            print(f"  [Rang] Ladefehler: {e}")

def rang_speichern():
    import json
    try:
        with open(RANG_DATEI, "w") as f:
            json.dump(_rang_daten, f, indent=2)
    except Exception as e:
        print(f"  [Rang] Speicherfehler: {e}")

def berechne_level(xp: int) -> tuple:
    """Gibt (level, name, xp_bis_nächstes) zurück."""
    aktuell = LEVEL_STUFEN[0]
    for min_xp, level, name in LEVEL_STUFEN:
        if xp >= min_xp:
            aktuell = (min_xp, level, name)
    # Nächstes Level
    idx = next((i for i, s in enumerate(LEVEL_STUFEN)
                if s[1] == aktuell[1]), 0)
    if idx < len(LEVEL_STUFEN) - 1:
        naechstes_xp = LEVEL_STUFEN[idx + 1][0]
        bis_naechstes = naechstes_xp - xp
    else:
        bis_naechstes = 0
    return aktuell[1], aktuell[2], bis_naechstes

def gib_xp(user_id: str, username: str, xp: int, grund: str) -> dict:
    """Gibt einem User XP und prüft Level-Up."""
    if user_id not in _rang_daten:
        _rang_daten[user_id] = {
            "name": username, "xp": 0, "level": 1,
            "level_name": "🆕 Newcomer", "gewinne": 0,
            "verluste": 0, "streak": 0, "checkin_streak": 0,
            "letzte_checkin": "", "spezial_rollen": [],
            "xp_history": []
        }
    d = _rang_daten[user_id]
    d["name"] = username
    altes_level = d["level"]
    d["xp"] += xp
    d["xp_history"].append({"xp": xp, "grund": grund, "ts": jetzt()})
    d["xp_history"] = d["xp_history"][-50:]  # Max 50 Einträge
    # Level berechnen
    neues_level, level_name, bis_naechstes = berechne_level(d["xp"])
    d["level"]      = neues_level
    d["level_name"] = level_name
    rang_speichern()
    # Level-Up Benachrichtigung
    if neues_level > altes_level:
        msg = (f"🎉 <b>LEVEL UP! {username}</b>\n"
               f"━━━━━━━━━━━━━━━━━━━━\n"
               f"Level {altes_level} → <b>Level {neues_level}</b>\n"
               f"🏅 <b>{level_name}</b>\n"
               f"⭐ Gesamt-XP: {d['xp']:,}\n"
               f"━━━━━━━━━━━━━━━━━━━━\n"
               f"{'🔓 Neue Features freigeschaltet!' if neues_level % 5 == 0 else 'Weiter so!'}")
        send_telegram_gruppe(msg)
        sende_discord_level_up(username, altes_level, neues_level, level_name, d["xp"])
    return d

def sende_discord_level_up(username, alt, neu, name, xp_ges):
    """Discord Embed bei Level-Up."""
    farben = {5: 0xCD7F32, 8: 0xC0C0C0, 10: 0xFFD700,
              14: 0x00BFFF, 19: 0xFF6B00, 20: 0xFFD700}
    farbe = farben.get(neu, 0x5865F2)
    embed = {
        "title": f"🎉 LEVEL UP – {username}!",
        "color": farbe,
        "fields": [
            {"name": "📈 Neues Level", "value": f"**Level {neu}** – {name}", "inline": False},
            {"name": "⭐ Gesamt-XP",  "value": f"**{xp_ges:,} XP**",        "inline": True},
            {"name": "📊 Vorher",     "value": f"Level {alt}",               "inline": True},
        ],
        "footer": {"text": f"BetlabLIVE Rang-System • {heute()}"},
        "thumbnail": {"url": "https://i.imgur.com/placeholder.png"}
    }
    send_discord_embed(DISCORD_WEBHOOK_BILANZ, embed)

def xp_rangliste() -> str:
    """Top 10 XP Rangliste."""
    sortiert = sorted(_rang_daten.items(),
                      key=lambda x: x[1].get("xp", 0), reverse=True)[:10]
    medals = ["🥇","🥈","🥉"]
    zeilen = []
    for i, (uid, d) in enumerate(sortiert):
        m = medals[i] if i < 3 else f"{i+1}."
        zeilen.append(f"{m} <b>{d.get('name','?')}</b> – "
                      f"Lv.{d.get('level',1)} | {d.get('xp',0):,} XP | {d.get('level_name','?')}")
    return "\n".join(zeilen) if zeilen else "Noch keine Einträge"

def invite_features_text(count: int) -> str:
    """Zeigt freigeschaltete Features basierend auf Einladungen."""
    zeilen = []
    for inv, feat in sorted(INVITE_FEATURES.items()):
        if inv <= count:
            zeilen.append(f"✅ {feat['name']}")
        else:
            zeilen.append(f"🔒 {feat['name']} <i>(ab {inv} Einladungen)</i>")
    return "\n".join(zeilen)

def wöchentliche_xp_auswertung():
    """Gibt dem Wochen-Gewinner XP-Bonus."""
    if not _rang_daten:
        return
    # Bester Gewinner diese Woche (aus community_tipps)
    beste_uid = None
    beste_pct  = 0
    for uid, data in _community_system.get("rollen", {}).items():
        ges = data.get("tipps", 0)
        gw  = data.get("gewinne", 0)
        if ges < 3:
            continue
        pct = gw / ges * 100
        if pct > beste_pct:
            beste_pct  = pct
            beste_uid  = uid
    if beste_uid and beste_uid in _rang_daten:
        username = _rang_daten[beste_uid].get("name", "?")
        gib_xp(beste_uid, username, XP_QUELLEN["woche_gewonnen"], "woche_top")
        msg = (f"🏆 <b>Wochen-Champion: {username}!</b>\n"
               f"📊 {round(beste_pct)}% Trefferquote\n"
               f"⭐ +{XP_QUELLEN['woche_gewonnen']} Bonus-XP!")
        send_telegram_gruppe(msg)

# ============================================================
#  COMMUNITY SYSTEM – Rollen, Einladungen, Challenges
# ============================================================

COMMUNITY_SYSTEM_DATEI = "community_system.json"
_community_system = {
    "einladungen":  {},   # user_id → {name, count, eingeladene: [], belohnungen: []}
    "challenges":   {},   # monat → {teilnehmer: {user_id: {start, aktuell}}}
    "vote_heute":   {},   # datum → {ja: [], nein: [], tipp: str}
    "rollen":       {},   # user_id → {rolle, gewinne, streak}
}

# ── Rollen-Definitionen ─────────────────────────────────
ROLLEN = [
    (100, "👑 Legend",          "legend"),
    (50,  "💎 Diamond Tipster", "diamond"),
    (30,  "🥇 Gold Tipster",    "gold"),
    (15,  "🥈 Silber Tipster",  "silber"),
    (5,   "🥉 Bronze Tipster",  "bronze"),
    (0,   "🆕 Neuling",         "neuling"),
]

STREAK_ROLLEN = [
    (10, "⚡ Streak Legend"),
    (5,  "⚡ Streak Master"),
    (3,  "🔥 On Fire"),
]

SNIPER_MIN_TIPPS  = 20
SNIPER_MIN_QUOTE  = 75  # %

# ── Einladungs-Belohnungen ──────────────────────────────
INVITE_BELOHNUNGEN = {
    1:  "🔓 VIP Channel Zugang",
    2:  "📊 Wöchentlicher persönlicher Report",
    3:  "🤖 Persönliche Tipp-Analyse",
    5:  "⭐ 1 Monat Premium kostenlos",
    8:  "🔒 Exklusiver Insider-Channel",
    10: "💰 Lifetime Mitgliedschaft",
    15: "📢 Bot nennt deinen Namen bei Signalen",
    20: "👤 Co-Admin Rechte",
    25: "🎯 Persönlicher Custom Alert",
    30: "💎 Lifetime VIP + Partner Badge",
    50: "🏆 Hall of Fame + eigener Channel",
}

def community_system_laden():
    import json, os
    global _community_system
    if not os.path.exists(COMMUNITY_SYSTEM_DATEI):
        return
    try:
        with open(COMMUNITY_SYSTEM_DATEI) as f:
            data = json.load(f)
        _community_system.update(data)
        print(f"  [Community] System geladen: {len(_community_system['einladungen'])} User")
    except Exception as e:
        print(f"  [Community] Ladefehler: {e}")

def community_system_speichern():
    import json
    try:
        with open(COMMUNITY_SYSTEM_DATEI, "w") as f:
            json.dump(_community_system, f, indent=2)
    except Exception as e:
        print(f"  [Community] Speicherfehler: {e}")

def berechne_rolle(gewinne: int, trefferquote: float, streak: int) -> str:
    """Berechnet die Rolle eines Users."""
    spezial = []
    # Streak-Rolle
    for min_s, name in STREAK_ROLLEN:
        if streak >= min_s:
            spezial.append(name)
            break
    # Sniper-Rolle
    if trefferquote >= SNIPER_MIN_QUOTE:
        spezial.append("🎯 Sniper")
    # Basis-Rolle
    basis = ROLLEN[-1][1]
    for min_g, name, _ in ROLLEN:
        if gewinne >= min_g:
            basis = name
            break
    alle = [basis] + spezial
    return " | ".join(alle)

def update_user_rolle(user_id: str, username: str, gewonnen: bool):
    """Aktualisiert Rolle nach einem Tipp-Ergebnis."""
    if user_id not in _community_system["rollen"]:
        _community_system["rollen"][user_id] = {
            "name": username, "gewinne": 0, "verluste": 0,
            "streak": 0, "beste_streak": 0, "tipps": 0
        }
    data = _community_system["rollen"][user_id]
    data["tipps"]  += 1
    data["name"]    = username
    if gewonnen:
        data["gewinne"]      += 1
        data["streak"]        = data.get("streak", 0) + 1
        data["beste_streak"]  = max(data.get("beste_streak", 0), data["streak"])
    else:
        data["verluste"] = data.get("verluste", 0) + 1
        data["streak"]   = 0
    pct   = round(data["gewinne"] / max(data["tipps"], 1) * 100)
    rolle = berechne_rolle(data["gewinne"], pct, data["streak"])
    alte_rolle = data.get("rolle", "")
    data["rolle"] = rolle
    community_system_speichern()
    # Benachrichtigung bei neuer Rolle
    if alte_rolle != rolle and alte_rolle:
        msg = (f"🎉 <b>{username} hat eine neue Rolle!</b>\n"
               f"━━━━━━━━━━━━━━━━━━━━\n"
               f"Vorher: {alte_rolle}\n"
               f"Jetzt:  <b>{rolle}</b>\n"
               f"📊 {data['gewinne']}W | {data.get('verluste',0)}L | {pct}% Quote")
        send_telegram_gruppe(msg)
    return rolle

def registriere_einladung(einlader_id: str, einlader_name: str,
                           neuer_id: str, neuer_name: str):
    """Registriert eine neue Einladung."""
    if einlader_id not in _community_system["einladungen"]:
        _community_system["einladungen"][einlader_id] = {
            "name": einlader_name, "count": 0,
            "eingeladene": [], "belohnungen": []
        }
    data = _community_system["einladungen"][einlader_id]
    if neuer_id in data["eingeladene"]:
        return  # Bereits registriert
    data["eingeladene"].append(neuer_id)
    data["count"] += 1
    data["name"]   = einlader_name
    anzahl = data["count"]
    community_system_speichern()
    # Belohnung prüfen
    belohnung = INVITE_BELOHNUNGEN.get(anzahl)
    if belohnung and belohnung not in data["belohnungen"]:
        data["belohnungen"].append(belohnung)
        community_system_speichern()
        msg = (f"🎁 <b>{einlader_name} hat eine Belohnung verdient!</b>\n"
               f"━━━━━━━━━━━━━━━━━━━━\n"
               f"✅ {anzahl}. Einladung – {neuer_name} beigetreten\n"
               f"🎁 Belohnung: <b>{belohnung}</b>\n"
               f"━━━━━━━━━━━━━━━━━━━━\n"
               f"Kontaktiere einen Admin zur Einlösung!")
        send_telegram(msg)

def freundes_bonus_prüfen(user_id: str, username: str):
    """Prüft ob jemand einen Einlader hat und benachrichtigt ihn beim ersten Gewinn."""
    for einlader_id, data in _community_system["einladungen"].items():
        if user_id in data.get("eingeladene", []):
            bonus_key = f"bonus_{user_id}"
            if data.get(bonus_key):
                return  # Bereits benachrichtigt
            data[bonus_key] = True
            community_system_speichern()
            msg = (f"🎉 <b>Freundes-Bonus!</b>\n"
                   f"━━━━━━━━━━━━━━━━━━━━\n"
                   f"Dein eingeladener Freund <b>{username}</b>\n"
                   f"hat seinen ersten Tipp gewonnen! 🥳\n"
                   f"Danke dass du die Community wachsen lässt! 💪")
            # An Einlader schicken
            try:
                requests.post(
                    f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                    json={"chat_id": einlader_id, "text": msg, "parse_mode": "HTML"},
                    timeout=10
                )
            except Exception:
                pass
            break

def sende_tipp_vote(tipp: str, spiel: str, liga: str):
    """Sendet den Tipp des Tages mit Abstimmungs-Buttons."""
    heute = de_now().strftime("%Y-%m-%d")
    _community_system["vote_heute"][heute] = {"ja": [], "nein": [], "tipp": tipp, "spiel": spiel}
    community_system_speichern()
    import json as _json
    keyboard = {
        "inline_keyboard": [[
            {"text": "✅ Ich nehme den Tipp!", "callback_data": f"vote_ja_{heute}"},
            {"text": "❌ Ich passe aus",        "callback_data": f"vote_nein_{heute}"},
        ]]
    }
    msg = (f"🗳️ <b>Tipp des Tages – Abstimmung!</b>\n"
           f"━━━━━━━━━━━━━━━━━━━━\n"
           f"🏆 {liga}\n📌 {spiel}\n"
           f"━━━━━━━━━━━━━━━━━━━━\n"
           f"🎯 Tipp: <b>{tipp}</b>\n"
           f"━━━━━━━━━━━━━━━━━━━━\n"
           f"👇 Nimmst du den Tipp mit?")
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg,
                  "parse_mode": "HTML",
                  "reply_markup": _json.dumps(keyboard)},
            timeout=10
        )
    except Exception as e:
        print(f"  [Vote] Fehler: {e}")

def verarbeite_vote_callback(data: str, user_id: str, username: str):
    """Verarbeitet Vote-Button-Drücke."""
    heute = de_now().strftime("%Y-%m-%d")
    if heute not in _community_system["vote_heute"]:
        return "Keine aktive Abstimmung heute"
    vote_data = _community_system["vote_heute"][heute]
    # Alten Vote entfernen
    vote_data["ja"]   = [u for u in vote_data["ja"]   if u != user_id]
    vote_data["nein"] = [u for u in vote_data["nein"] if u != user_id]
    if "vote_ja_" in data:
        vote_data["ja"].append(user_id)
        antwort = f"✅ Du nimmst den Tipp mit! ({len(vote_data['ja'])} Stimmen)"
    else:
        vote_data["nein"].append(user_id)
        antwort = f"❌ Du passt aus! ({len(vote_data['nein'])} Stimmen)"
    community_system_speichern()
    return antwort

def sende_einladungs_leaderboard():
    """Wöchentliches Einladungs-Leaderboard."""
    einladungen = _community_system["einladungen"]
    if not einladungen:
        return
    sortiert = sorted(einladungen.items(),
                      key=lambda x: x[1]["count"], reverse=True)[:10]
    medals  = ["🥇","🥈","🥉"]
    zeilen  = []
    for i, (uid, data) in enumerate(sortiert):
        m = medals[i] if i < 3 else f"{i+1}."
        naechste = min([k for k in INVITE_BELOHNUNGEN if k > data["count"]] or [999])
        noch = naechste - data["count"] if naechste < 999 else 0
        zeilen.append(
            f"{m} <b>{data['name']}</b>: {data['count']} Einladungen"
            + (f" (noch {noch} bis nächste Belohnung)" if noch > 0 else " 🏆")
        )
    msg = (f"📊 <b>Einladungs-Leaderboard diese Woche</b>\n"
           f"━━━━━━━━━━━━━━━━━━━━\n"
           + "\n".join(zeilen) +
           f"\n━━━━━━━━━━━━━━━━━━━━\n"
           f"🎁 Lade Freunde ein und gewinne Belohnungen!\n"
           f"Dein Link: t.me/frums72bot")
    send_telegram(msg)
    send_telegram_gruppe(msg)

def sende_monatliche_challenge():
    """Startet monatliche Wett-Challenge."""
    monat = de_now().strftime("%Y-%m")
    if monat in _community_system["challenges"]:
        return
    _community_system["challenges"][monat] = {"teilnehmer": {}, "aktiv": True}
    community_system_speichern()
    msg = (f"🏆 <b>Monatliche Challenge startet!</b>\n"
           f"━━━━━━━━━━━━━━━━━━━━\n"
           f"📅 {de_now().strftime('%B %Y')}\n\n"
           f"💰 <b>Challenge: Wer macht aus 10€ am meisten?</b>\n\n"
           f"📋 Regeln:\n"
           f"• Startkapital: 10€\n"
           f"• Nur Bot-Signale verwenden\n"
           f"• Monatlich beste Trefferquote gewinnt\n"
           f"• Einsätze selbst dokumentieren mit /tipp\n\n"
           f"🎁 Preise:\n"
           f"🥇 Platz 1: 1 Monat Premium gratis\n"
           f"🥈 Platz 2: Persönliche Bot-Analyse\n"
           f"🥉 Platz 3: VIP Channel 2 Wochen\n"
           f"━━━━━━━━━━━━━━━━━━━━\n"
           f"Mitmachen mit: <code>/challenge join</code>")
    send_telegram(msg)
    send_telegram_gruppe(msg)

# ============================================================
#  MULTI-ADMIN SUPPORT
# ============================================================

ADMIN_IDS = [TELEGRAM_CHAT_ID]  # Weitere IDs per /addadmin hinzufügen
ADMIN_DATEI = "admins.json"

def admins_laden():
    import json, os
    global ADMIN_IDS
    if not os.path.exists(ADMIN_DATEI):
        return
    try:
        with open(ADMIN_DATEI) as f:
            data = json.load(f)
        ADMIN_IDS = data.get("ids", [TELEGRAM_CHAT_ID])
        print(f"  [Admin] {len(ADMIN_IDS)} Admins geladen")
    except Exception as e:
        print(f"  [Admin] Ladefehler: {e}")

def admins_speichern():
    import json
    try:
        with open(ADMIN_DATEI, "w") as f:
            json.dump({"ids": ADMIN_IDS}, f)
    except Exception as e:
        print(f"  [Admin] Speicherfehler: {e}")

def ist_admin(chat_id: str) -> bool:
    return str(chat_id) in [str(a) for a in ADMIN_IDS]

# ============================================================
#  HEAD-TO-HEAD ANALYSE
# ============================================================

def hole_h2h(home_id: str, away_id: str, home: str, away: str) -> str:
    """Holt letzte 3 Direktduelle und gibt formatierten Text zurück."""
    try:
        params = {**LS_AUTH, "team_id": home_id, "number": 20}
        resp   = api_get_with_retry(f"{LS_BASE}/matches/history.json", params)
        alle   = resp.json().get("data", {}).get("match", []) or []
        duelle = []
        for m in alle:
            h_id = str((m.get("home") or {}).get("id", ""))
            a_id = str((m.get("away") or {}).get("id", ""))
            if away_id in (h_id, a_id):
                score  = (m.get("scores") or {}).get("score", "?")
                datum  = (m.get("date") or "")[:10]
                h_name = (m.get("home") or {}).get("name", "?")
                a_name = (m.get("away") or {}).get("name", "?")
                duelle.append(f"{datum}: {h_name} {score} {a_name}")
                if len(duelle) >= 3:
                    break
        if not duelle:
            return ""
        return "🔄 <b>H2H (letzte 3):</b>\n" + "\n".join(duelle)
    except Exception as e:
        print(f"  [H2H] Fehler: {e}")
        return ""

# ============================================================
#  HEIMSTÄRKE-INDEX
# ============================================================

def heimstaerke_index(home_id: str, home: str) -> tuple:
    """
    Berechnet Heimsieg-Rate aus letzten 10 Heimspielen.
    Gibt (rate: float, bonus: int) zurück.
    Bonus: +1 Konfidenz wenn >= 70%, +2 wenn >= 80%.
    """
    try:
        params = {**LS_AUTH, "team_id": home_id, "number": 10}
        resp   = api_get_with_retry(f"{LS_BASE}/matches/history.json", params)
        alle   = resp.json().get("data", {}).get("match", []) or []
        heim   = [m for m in alle
                  if str((m.get("home") or {}).get("id", "")) == home_id][:10]
        if len(heim) < 5:
            return 0.0, 0
        siege = sum(1 for m in heim
                    if str((m.get("winner") or {}).get("id", "")) == home_id)
        rate  = round(siege / len(heim) * 100)
        bonus = 2 if rate >= 80 else (1 if rate >= 70 else 0)
        return rate, bonus
    except Exception:
        return 0.0, 0

# ============================================================
#  FORMKURVEN-VERGLEICH
# ============================================================

def formkurve(team_id: str, team_name: str) -> str:
    """
    Letzte 5 Spiele als Emoji-Trendlinie.
    W=✅ D=🟡 L=❌ → z.B. ✅✅❌✅✅ (↑ aufsteigend)
    """
    try:
        params = {**LS_AUTH, "team_id": team_id, "number": 5}
        resp   = api_get_with_retry(f"{LS_BASE}/matches/history.json", params)
        alle   = resp.json().get("data", {}).get("match", []) or []
        if not alle:
            return ""
        emojis = []
        for m in reversed(alle[:5]):
            winner_id = str((m.get("winner") or {}).get("id", ""))
            if winner_id == team_id:
                emojis.append("✅")
            elif winner_id == "0" or not winner_id:
                emojis.append("🟡")
            else:
                emojis.append("❌")
        # Trend berechnen
        letzte2   = emojis[-2:]
        trend     = "↑" if letzte2.count("✅") >= 2 else ("↓" if letzte2.count("❌") >= 2 else "→")
        form_str  = "".join(emojis)
        return f"{team_name}: {form_str} {trend}"
    except Exception:
        return ""

# ============================================================
#  ÜBER 0.5 HZ2 TORE BOT
# ============================================================

notified_hz2 = set()

def bot_hz2_tore():
    """
    Signal zur Halbzeit wenn:
    - HZ1 torlos (0:0)
    - Aber beide Teams viel Druck (Schüsse + Ecken)
    → Über 0.5 Tore in HZ2 sehr wahrscheinlich (>75%)
    """
    print("[HZ2-Tore-Bot] Gestartet | 0:0 HZ + Druck → Tor in HZ2")
    while True:
        try:
            matches = get_live_matches()
            pausen  = [m for m in matches if m.get("status") in
                       ("HALF TIME", "HT", "Half Time", "half time",
                        "HALFTIME", "Half-Time")]
            print(f"[{jetzt()}] [HZ2-Tore-Bot] {len(pausen)} Halbzeit-Spiele")
            for game in pausen:
                match_id = str(game.get("id"))
                if match_id in notified_hz2:
                    continue
                score_str = game.get("scores", {}).get("score", "")
                h, a      = parse_score(score_str)
                if h + a != 0:
                    continue  # Nur bei 0:0
                home    = game.get("home", {}).get("name", "?")
                away    = game.get("away", {}).get("name", "?")
                comp    = game.get("competition", {}).get("name", "?")
                country = (game.get("country") or {}).get("name", "?")
                if not whitelist_check(comp, home, away):
                    continue
                stats     = get_statistiken(match_id)
                shots_h   = stats["shots_on_target_home"]
                shots_a   = stats["shots_on_target_away"]
                corners_h = stats["corners_home"]
                corners_a = stats["corners_away"]
                da_h      = stats["dangerous_attacks_home"]
                da_a      = stats["dangerous_attacks_away"]
                druck_ges = shots_h + shots_a + corners_h + corners_a
                if druck_ges < 8:
                    continue  # Zu wenig Druck → kein Signal
                notified_hz2.add(match_id)
                notified_sets_speichern()
                beobachtung_hinzufuegen(match_id, {
                    "typ": "hz1tore", "match_id": match_id,
                    "home": home, "away": away,
                    "richtung": "ueber", "linie": 0.5,
                    "score_signal": "0 - 0",
                    "quote": get_quote(home, away, "hz2"),
                    "webhook": DISCORD_WEBHOOK_TORE,
                    "signal_zeit": time.time(), "bot": "HZ2-Tore-Bot"
                })
                konfidenz = min(10, 6 + (1 if druck_ges >= 12 else 0) + (1 if druck_ges >= 16 else 0))
                ke        = konfidenz_emoji(konfidenz)
                msg = (f"⚡ <b>Über 0.5 HZ2 Tore!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                       f"🏆 {comp} ({country})\n📌 {home} vs {away}\n"
                       f"📊 HZ1: <b>0:0</b> – Halbzeit!\n"
                       f"━━━━━━━━━━━━━━━━━━━━\n"
                       f"🎯 Schüsse: {shots_h}|{shots_a} | Ecken: {corners_h}|{corners_a}\n"
                       f"⚡ Druck-Score: <b>{druck_ges}</b> (min. 8 nötig)\n"
                       f"📈 Statistik: >75% Chance auf mind. 1 Tor in HZ2\n"
                       f"🎯 Tipp: <b>Über 0.5 HZ2 Tore</b>\n"
                       f"{ke} Konfidenz: <b>{konfidenz}/10</b>\n"
                       f"━━━━━━━━━━━━━━━━━━━━\n🕐 {jetzt()} Uhr")
                send_telegram(msg)
                embed = {
                    "title": "⚡ 0:0 HZ + Hoher Druck → Tor in HZ2!",
                    "color": 0x9B59B6,
                    "fields": [
                        {"name": "🏆 Liga",     "value": comp,                 "inline": True},
                        {"name": "🌍 Land",     "value": country,              "inline": True},
                        {"name": "⚽ Spiel",    "value": f"{home} vs {away}", "inline": False},
                        {"name": "📊 HZ1",      "value": "**0:0**",           "inline": True},
                        {"name": "🎯 Schüsse",  "value": f"{shots_h}|{shots_a}","inline": True},
                        {"name": "📐 Ecken",    "value": f"{corners_h}|{corners_a}","inline": True},
                        {"name": "⚡ Druck",    "value": f"**{druck_ges}**",  "inline": True},
                        {"name": ke+" Konfidenz","value": f"**{konfidenz}/10**","inline": True},
                        {"name": "🎯 Tipp",     "value": "**Über 0.5 HZ2 Tore**","inline": False},
                    ],
                    "footer": {"text": f"HZ2-Tore-Bot • {heute()} {jetzt()}"},
                }
                send_discord_embed(DISCORD_WEBHOOK_TORE, embed)
                print(f"  [HZ2-Tore-Bot] ✅ {home} vs {away} | Druck {druck_ges}")
                time.sleep(0.5)
            bot_fehler_reset("HZ2-Tore-Bot")
        except Exception as e:
            bot_fehler_melden("HZ2-Tore-Bot", e)
        time.sleep(60)

# ============================================================
#  CORNER RUSH BOT
# ============================================================

notified_corner_rush = set()
_corner_history = {}  # match_id → [(timestamp, corners_total)]

def bot_corner_rush():
    """
    Signal wenn ein Team in 10 Minuten 4+ Ecken erzwingt.
    → Eskalation: weiterer Druck und mögliches Tor oder mehr Ecken.
    """
    print("[CornerRush-Bot] Gestartet | 4+ Ecken in 10 Min")
    while True:
        try:
            matches = get_live_matches()
            laufend = [m for m in matches if m.get("status") in
                       ("IN PLAY", "ADDED TIME") and
                       _safe_int(m.get("time", 0)) >= 20]
            for game in laufend:
                match_id = str(game.get("id"))
                if match_id in notified_corner_rush:
                    continue
                home    = game.get("home", {}).get("name", "?")
                away    = game.get("away", {}).get("name", "?")
                comp    = game.get("competition", {}).get("name", "?")
                country = (game.get("country") or {}).get("name", "?")
                score   = game.get("scores", {}).get("score", "?")
                minute  = _safe_int(game.get("time", 0))
                if not whitelist_check(comp, home, away):
                    continue
                stats     = get_statistiken(match_id)
                corners_h = stats["corners_home"]
                corners_a = stats["corners_away"]
                now_ts    = time.time()
                if match_id not in _corner_history:
                    _corner_history[match_id] = []
                _corner_history[match_id].append({
                    "ts": now_ts, "ch": corners_h, "ca": corners_a
                })
                # Nur letzte 12 Min behalten
                hist = [e for e in _corner_history[match_id]
                        if now_ts - e["ts"] <= 12 * 60]
                _corner_history[match_id] = hist
                if len(hist) < 2:
                    continue
                alt    = hist[0]
                diff_h = corners_h - alt["ch"]
                diff_a = corners_a - alt["ca"]
                rush_team  = home if diff_h >= 4 else (away if diff_a >= 4 else None)
                rush_ecken = diff_h if diff_h >= 4 else (diff_a if diff_a >= 4 else 0)
                if not rush_team:
                    continue
                notified_corner_rush.add(match_id)
                notified_sets_speichern()
                zeit_diff = round((now_ts - alt["ts"]) / 60, 1)
                msg = (f"📐 <b>Corner Rush!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                       f"🏆 {comp} ({country})\n📌 {home} vs {away}\n"
                       f"📊 Stand: <b>{score}</b> | Min. <b>{minute}'</b>\n"
                       f"━━━━━━━━━━━━━━━━━━━━\n"
                       f"⚡ <b>{rush_team}</b>: {rush_ecken} Ecken in {zeit_diff} Min!\n"
                       f"📐 Ecken gesamt: {corners_h}|{corners_a}\n"
                       f"💡 Extremer Druck → Tor oder weitere Ecken sehr wahrscheinlich\n"
                       f"🎯 Tipp: <b>Nächste Ecke / Tor für {rush_team}</b>\n"
                       f"━━━━━━━━━━━━━━━━━━━━\n🕐 {jetzt()} Uhr")
                send_telegram(msg)
                embed = {
                    "title": f"📐 Corner Rush – {rush_team} eskaliert!",
                    "color": 0xE67E22,
                    "fields": [
                        {"name": "🏆 Liga",       "value": comp,                    "inline": True},
                        {"name": "🌍 Land",       "value": country,                 "inline": True},
                        {"name": "⏱️ Minute",     "value": f"**{minute}'**",        "inline": True},
                        {"name": "⚽ Spiel",      "value": f"{home} vs {away}",     "inline": False},
                        {"name": "📊 Stand",      "value": f"**{score}**",          "inline": True},
                        {"name": "⚡ Rush-Team",  "value": f"**{rush_team}**",      "inline": True},
                        {"name": "📐 Ecken Rush", "value": f"**{rush_ecken}** in {zeit_diff} Min","inline": True},
                        {"name": "📐 Gesamt",     "value": f"{corners_h}|{corners_a}","inline": True},
                        {"name": "🎯 Tipp",       "value": f"**Nächste Ecke / Tor {rush_team}**","inline": False},
                    ],
                    "footer": {"text": f"CornerRush-Bot • {heute()} {jetzt()}"},
                }
                send_discord_embed(DISCORD_WEBHOOK_ECKEN, embed)
                print(f"  [CornerRush] ✅ {rush_team}: {rush_ecken} Ecken in {zeit_diff} Min")
                time.sleep(0.5)
            bot_fehler_reset("CornerRush-Bot")
        except Exception as e:
            bot_fehler_melden("CornerRush-Bot", e)
        time.sleep(90)

# ============================================================
#  TIPP DES TAGES
# ============================================================

def bot_tipp_des_tages():
    """
    Sendet täglich um 09:00 Uhr den besten Tipp des Tages
    basierend auf Claude-Analyse der Top-Liga Spiele.
    """
    print("[TippDesTages-Bot] Gestartet | Täglich 09:00 Uhr")
    gesendet_tdt = set()
    while True:
        try:
            now = de_now()
            key = now.strftime("%Y-%m-%d")
            if now.hour == 9 and now.minute < 5 and key not in gesendet_tdt:
                gesendet_tdt.add(key)
                datum    = now.strftime("%Y-%m-%d")
                fixtures = ls_get_fixtures(datum)
                top      = filtere_top_spiele(fixtures)
                if not top:
                    time.sleep(60)
                    continue
                bester_tipp = None
                beste_konfidenz = 0
                for spiel in top[:10]:
                    home   = spiel.get("home_name") or spiel.get("home", {}).get("name", "?")
                    away   = spiel.get("away_name") or spiel.get("away", {}).get("name", "?")
                    liga   = spiel.get("competition", {}).get("name", "?")
                    ansatz = spiel.get("time", "?")
                    result = claude_prematch_analyse(home, away, liga, ansatz, [])
                    if result and result.get("konfidenz", 0) > beste_konfidenz:
                        beste_konfidenz = result["konfidenz"]
                        bester_tipp     = {**result, "home": home, "away": away,
                                           "liga": liga, "anstoß": ansatz}
                    time.sleep(1)
                if not bester_tipp:
                    continue
                ke  = konfidenz_emoji(bester_tipp["konfidenz"])
                msg = (f"⭐ <b>Tipp des Tages – {now.strftime('%d.%m.%Y')}</b>\n"
                       f"━━━━━━━━━━━━━━━━━━━━\n"
                       f"🏆 {bester_tipp['liga']}\n"
                       f"📌 {bester_tipp['home']} vs {bester_tipp['away']}\n"
                       f"🕐 Anstoß: <b>{bester_tipp['anstoß']} Uhr</b>\n"
                       f"━━━━━━━━━━━━━━━━━━━━\n"
                       f"🎯 Tipp: <b>{bester_tipp['tipp']}</b>\n"
                       f"{ke} Konfidenz: <b>{bester_tipp['konfidenz']}/10</b>\n"
                       f"📊 {bester_tipp['analyse']}\n"
                       f"━━━━━━━━━━━━━━━━━━━━\n"
                       f"⭐ Unser bester Tipp heute!\n"
                       f"⚠️ 18+ | Verantwortungsvoll spielen")
                send_telegram(msg)
                send_telegram_gruppe(msg)
                embed = {
                    "title": f"⭐ Tipp des Tages – {now.strftime('%d.%m.%Y')}",
                    "color": 0xF1C40F,
                    "fields": [
                        {"name": "🏆 Liga",     "value": bester_tipp["liga"],      "inline": True},
                        {"name": "🕐 Anstoß",   "value": bester_tipp["anstoß"],    "inline": True},
                        {"name": "⚽ Spiel",    "value": f"**{bester_tipp['home']}** vs **{bester_tipp['away']}**", "inline": False},
                        {"name": "🎯 Tipp",     "value": f"**{bester_tipp['tipp']}**", "inline": False},
                        {"name": ke+" Konfidenz","value": f"**{bester_tipp['konfidenz']}/10**", "inline": True},
                        {"name": "📊 Analyse",  "value": bester_tipp["analyse"],   "inline": False},
                    ],
                    "footer": {"text": f"BetlabLIVE • Tipp des Tages • {heute()}"},
                }
                send_discord_embed(DISCORD_WEBHOOK_BILANZ, embed)
                print(f"  [TippDesTages] ✅ {bester_tipp['home']} vs {bester_tipp['away']} | {bester_tipp['tipp']}")
        except Exception as e:
            print(f"  [TippDesTages] Fehler: {e}")
        time.sleep(60)

# ============================================================
#  STREAK-ALARM
# ============================================================

_streak_alarm_gesendet = {"positiv": 0, "negativ": 0}

def check_streak_alarm():
    """Prüft nach jeder Auswertung ob Streak-Alarm ausgelöst werden soll."""
    global _streak_alarm_gesendet
    if abs(streak_aktuell) >= 5:
        typ = "positiv" if streak_aktuell > 0 else "negativ"
        if _streak_alarm_gesendet[typ] == streak_aktuell:
            return
        _streak_alarm_gesendet[typ] = streak_aktuell
        if streak_aktuell >= 5:
            emoji = "🔥"
            titel = f"Hot Streak! {streak_aktuell} Tipps in Folge GEWONNEN!"
            farbe = 0x2ECC71
        else:
            emoji = "❄️"
            titel = f"Cold Streak! {abs(streak_aktuell)} Tipps in Folge VERLOREN!"
            farbe = 0xE74C3C
        msg = (f"{emoji} <b>{titel}</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                       f"{'🎉 Der Bot ist gerade in Top-Form!' if streak_aktuell > 0 else '⚠️ Vorsicht – eventuell Einsätze reduzieren!'}\n"
               f"Streak: <b>{streak_aktuell}</b> | Bester Streak: <b>{streak_beste}</b>\n"
               f"━━━━━━━━━━━━━━━━━━━━\n🕐 {jetzt()} Uhr")
        send_telegram(msg)
        embed = {"title": f"{emoji} {titel}", "color": farbe,
                 "fields": [
                     {"name": "🔥 Aktueller Streak","value": f"**{streak_aktuell}**","inline": True},
                     {"name": "🏆 Bester Streak",   "value": f"**{streak_beste}**",  "inline": True},
                 ],
                 "footer": {"text": f"BetlabLIVE • {heute()} {jetzt()}"}}
        send_discord_embed(DISCORD_WEBHOOK_BILANZ, embed)
        print(f"  [Streak-Alarm] {emoji} Streak {streak_aktuell}")

# ============================================================
#  WHITELIST & MANUELLE TIPPS (Konfiguration)
# ============================================================

WHITELIST_DATEI   = "whitelist.json"
MANUELL_DATEI     = "manuell_tipps.json"
_whitelist        = {"ligen": [], "teams": [], "aktiv": False}
_manuell_tipps    = []

def whitelist_laden():
    import json, os
    global _whitelist
    if not os.path.exists(WHITELIST_DATEI):
        return
    try:
        with open(WHITELIST_DATEI) as f:
            _whitelist = json.load(f)
        if _whitelist.get("aktiv"):
            print(f"  [Whitelist] Aktiv: {len(_whitelist.get('ligen',[]))} Ligen, {len(_whitelist.get('teams',[]))} Teams")
    except Exception as e:
        print(f"  [Whitelist] Fehler: {e}")

def whitelist_speichern():
    import json
    try:
        with open(WHITELIST_DATEI, "w") as f:
            json.dump(_whitelist, f, indent=2)
    except Exception as e:
        print(f"  [Whitelist] Speicherfehler: {e}")

def whitelist_check(liga: str, home: str = "", away: str = "") -> bool:
    """Gibt False zurück wenn Whitelist aktiv und Spiel nicht auf der Liste."""
    if not _whitelist.get("aktiv"):
        return True
    ligen  = [l.lower() for l in _whitelist.get("ligen", [])]
    teams  = [t.lower() for t in _whitelist.get("teams", [])]
    if ligen and liga.lower() not in ligen:
        return False
    if teams and home.lower() not in teams and away.lower() not in teams:
        return False
    return True

def manuell_tipps_laden():
    import json, os
    global _manuell_tipps
    if not os.path.exists(MANUELL_DATEI):
        return
    try:
        with open(MANUELL_DATEI) as f:
            _manuell_tipps = json.load(f)
    except Exception as e:
        print(f"  [Manuell] Fehler: {e}")

def manuell_tipps_speichern():
    import json
    try:
        with open(MANUELL_DATEI, "w") as f:
            json.dump(_manuell_tipps, f, indent=2)
    except Exception as e:
        print(f"  [Manuell] Fehler: {e}")

# ============================================================
#  LIVE-ODDS TRACKER
# ============================================================

_odds_history  = {}  # "home_away_linie" → [{quote, ts}]
ODDS_TRACKER_INTERVALL = 5 * 60  # alle 5 Min
notified_odds_drop = set()

def bot_odds_tracker():
    """
    Verfolgt Quoten-Bewegungen in Echtzeit.
    Wenn eine Quote in 15 Min um >12% fällt → Insider-Signal.
    """
    print("[Odds-Tracker] Gestartet | Quoten-Bewegungen überwachen")
    while True:
        try:
            if not ODDS_API_KEY:
                time.sleep(ODDS_TRACKER_INTERVALL)
                continue
            url    = "https://api.the-odds-api.com/v4/sports/soccer/odds/"
            params = {"apiKey": ODDS_API_KEY, "regions": "eu",
                      "markets": "totals,h2h", "oddsFormat": "decimal"}
            resp   = requests.get(url, params=params, timeout=10)
            if resp.status_code != 200:
                time.sleep(ODDS_TRACKER_INTERVALL)
                continue
            now = time.time()
            for game in resp.json():
                home_t = game.get("home_team", "?")
                away_t = game.get("away_team", "?")
                for bm in game.get("bookmakers", [])[:3]:
                    for market in bm.get("markets", []):
                        for outcome in market.get("outcomes", []):
                            q    = outcome.get("price", 0)
                            name = outcome.get("name", "")
                            pt   = outcome.get("point", "")
                            key  = f"{home_t}_{away_t}_{name}_{pt}"
                            if key not in _odds_history:
                                _odds_history[key] = []
                            _odds_history[key].append({"q": q, "ts": now})
                            # Nur letzte 15 Min behalten
                            _odds_history[key] = [
                                e for e in _odds_history[key]
                                if now - e["ts"] <= 15 * 60
                            ]
                            hist = _odds_history[key]
                            if len(hist) < 2:
                                continue
                            q_alt = hist[0]["q"]
                            q_neu = hist[-1]["q"]
                            if q_alt <= 1.1 or q_neu <= 1.1:
                                continue
                            drop = round((q_alt - q_neu) / q_alt * 100, 1)
                            if drop >= 12 and key not in notified_odds_drop:
                                notified_odds_drop.add(key)
                                msg = (f"📉 <b>Quote stark gefallen!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                                       f"📌 {home_t} vs {away_t}\n"
                                       f"🎯 Markt: <b>{name} {pt}</b>\n"
                                       f"📉 {q_alt} → <b>{q_neu}</b> ({bm.get('title','?')})\n"
                                       f"🔻 Rückgang: <b>-{drop}%</b> in 15 Min\n"
                                       f"💡 Mögliches Insider-Signal!\n"
                                       f"━━━━━━━━━━━━━━━━━━━━\n🕐 {jetzt()} Uhr")
                                send_telegram(msg)
                                embed = {
                                    "title": "📉 Quote stark gefallen – Insider-Signal?",
                                    "color": 0xE74C3C,
                                    "fields": [
                                        {"name": "⚽ Spiel",    "value": f"{home_t} vs {away_t}", "inline": False},
                                        {"name": "🎯 Markt",    "value": f"**{name} {pt}**",       "inline": True},
                                        {"name": "📉 Vorher",   "value": f"**{q_alt}**",           "inline": True},
                                        {"name": "📉 Jetzt",    "value": f"**{q_neu}**",           "inline": True},
                                        {"name": "🔻 Rückgang", "value": f"**-{drop}%**",          "inline": True},
                                        {"name": "📊 Quelle",   "value": bm.get("title","?"),      "inline": True},
                                    ],
                                    "footer": {"text": f"Odds-Tracker • {heute()} {jetzt()}"},
                                }
                                send_discord_embed(DISCORD_WEBHOOK_VALUE, embed)
                                print(f"  [Odds-Tracker] 📉 {home_t} vs {away_t} | {name} {pt}: {q_alt}→{q_neu} (-{drop}%)")
            bot_fehler_reset("Odds-Tracker")
        except Exception as e:
            bot_fehler_melden("Odds-Tracker", e)
        time.sleep(ODDS_TRACKER_INTERVALL)

# ============================================================
#  EARLY GOAL BOT
# ============================================================

notified_early_goal = set()

def bot_early_goal():
    """
    Signal wenn in Minute 1-10 ein Tor fällt.
    Statistisch: Spiele mit Frühtor enden häufig mit 3+ Toren gesamt.
    """
    print("[EarlyGoal-Bot] Gestartet | Frühtore Min. 1-10")
    while True:
        try:
            matches = get_live_matches()
            frueh   = [m for m in matches if m.get("status") == "IN PLAY"
                       and 1 <= _safe_int(m.get("time", 0)) <= 15]
            for game in frueh:
                match_id = str(game.get("id"))
                if match_id in notified_early_goal:
                    continue
                score_str = game.get("scores", {}).get("score", "")
                h, a = parse_score(score_str)
                if h + a == 0:
                    continue
                # Tor gefallen – via Events Minute prüfen
                try:
                    events = ls_get_events(match_id)
                    tore   = [e for e in events
                              if e.get("event") in ("Goal", "goal", "GOAL")
                              and _safe_int(e.get("time", 99)) <= 10]
                    if not tore:
                        continue
                except Exception:
                    if h + a == 0:
                        continue
                    tore = [{"time": "?"}]
                home    = game.get("home", {}).get("name", "?")
                away    = game.get("away", {}).get("name", "?")
                comp    = game.get("competition", {}).get("name", "?")
                country = (game.get("country") or {}).get("name", "?")
                minute  = game.get("time", "?")
                if not whitelist_check(comp, home, away):
                    continue
                notified_early_goal.add(match_id)
                notified_sets_speichern()
                tor_min = tore[0].get("time", "?") if tore else "?"
                quote   = get_quote(home, away, "tore")
                ql      = f"\n💶 Quote: <b>{quote}</b>" if quote else ""
                msg = (f"⚡ <b>Early Goal!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                       f"🏆 {comp} ({country})\n📌 {home} vs {away}\n"
                       f"📊 Stand: <b>{score_str}</b> | Min. <b>{minute}'</b>\n"
                       f"━━━━━━━━━━━━━━━━━━━━\n"
                       f"⚡ Tor in Minute <b>{tor_min}'</b>!\n"
                       f"📈 Statistik: Frühtore → 70%+ Chance auf 3+ Tore gesamt\n"
                       f"🎯 Tipp: <b>Über 2.5 Tore</b>{ql}\n"
                       f"━━━━━━━━━━━━━━━━━━━━\n🕐 {jetzt()} Uhr")
                send_telegram(msg)
                embed = {
                    "title": "⚡ Early Goal – Mehr Tore erwartet!",
                    "color": 0xF39C12,
                    "fields": [
                        {"name": "🏆 Liga",    "value": comp,                    "inline": True},
                        {"name": "🌍 Land",    "value": country,                 "inline": True},
                        {"name": "⏱️ Minute",  "value": f"**{minute}'**",        "inline": True},
                        {"name": "⚽ Spiel",   "value": f"{home} vs {away}",     "inline": False},
                        {"name": "📊 Stand",   "value": f"**{score_str}**",      "inline": True},
                        {"name": "⚡ Tor in",  "value": f"Min. **{tor_min}'**",  "inline": True},
                        {"name": "📈 Statistik","value": "Frühtore → 70%+ für Über 2.5 Tore", "inline": False},
                        {"name": "🎯 Tipp",    "value": "**Über 2.5 Tore**",    "inline": False},
                    ],
                    "footer": {"text": f"EarlyGoal-Bot • {heute()} {jetzt()}"},
                }
                send_discord_embed(DISCORD_WEBHOOK_TORFLUT, embed)
                print(f"  [EarlyGoal-Bot] ✅ {home} vs {away} | Tor in Min. {tor_min}")
                time.sleep(0.5)
            bot_fehler_reset("EarlyGoal-Bot")
        except Exception as e:
            bot_fehler_melden("EarlyGoal-Bot", e)
        time.sleep(60)  # Jede Minute prüfen – sehr zeitkritisch

# ============================================================
#  ROTE KARTE ECKEN-BOT
# ============================================================

notified_rk_ecken = set()

def bot_rotkarte_ecken():
    """
    Nach Roter Karte: das geschwächte Team kann kaum noch Ecken erzwingen.
    → Unter Ecken für das geschwächte Team sehr wahrscheinlich.
    """
    print("[RotkarteEcken-Bot] Gestartet | Ecken nach Roter Karte")
    while True:
        try:
            matches = get_live_matches()
            laufend = [m for m in matches if m.get("status") in
                       ("IN PLAY", "ADDED TIME") and
                       _safe_int(m.get("time", 0)) <= 75]
            for game in laufend:
                match_id = str(game.get("id"))
                if match_id in notified_rk_ecken:
                    continue
                try:
                    events = ls_get_events(match_id)
                    rote   = [e for e in events if e.get("event") in ROTKARTE_TYPEN]
                except Exception:
                    continue
                if not rote:
                    continue
                minute = _safe_int(game.get("time", 0))
                letzte = rote[-1]
                karte_min = _safe_int(letzte.get("time", 99))
                if minute - karte_min > 8:
                    continue
                home    = game.get("home", {}).get("name", "?")
                away    = game.get("away", {}).get("name", "?")
                comp    = game.get("competition", {}).get("name", "?")
                country = (game.get("country") or {}).get("name", "?")
                score   = game.get("scores", {}).get("score", "?")
                if not whitelist_check(comp, home, away):
                    continue
                karte_fuer    = letzte.get("home_away", "")
                geschwaeches  = home if karte_fuer == "home" else away
                staerkeres    = away if karte_fuer == "home" else home
                stats = get_statistiken(match_id)
                ecken_schwach = stats["corners_home"] if karte_fuer == "home" else stats["corners_away"]
                ecken_stark   = stats["corners_away"] if karte_fuer == "home" else stats["corners_home"]
                restminuten   = 90 - minute
                # Grenze: bisherige Ecken des schwachen Teams + max. 2 weitere
                grenze_ecken  = ecken_schwach + 2
                notified_rk_ecken.add(match_id)
                notified_sets_speichern()
                spieler = (letzte.get("player") or {}).get("name", "?")
                msg = (f"📐 <b>Rotkarte Ecken-Signal!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                       f"🏆 {comp} ({country})\n📌 {home} vs {away}\n"
                       f"📊 Stand: <b>{score}</b> | Min. <b>{minute}'</b>\n"
                       f"━━━━━━━━━━━━━━━━━━━━\n"
                       f"🟥 {spieler} ({geschwaeches}) Rote Karte Min. {karte_min}'\n"
                       f"📐 Ecken bisher: {staerkeres}: <b>{ecken_stark}</b> | {geschwaeches}: <b>{ecken_schwach}</b>\n"
                       f"⏱️ Noch <b>{restminuten} Min</b> – {geschwaeches} in Unterzahl\n"
                       f"🎯 Tipp: <b>{geschwaeches} unter {grenze_ecken} Ecken gesamt</b>\n"
                       f"━━━━━━━━━━━━━━━━━━━━\n🕐 {jetzt()} Uhr")
                send_telegram(msg)
                embed = {
                    "title": "📐 Rotkarte → Unter Ecken!",
                    "color": 0xC0392B,
                    "fields": [
                        {"name": "🏆 Liga",         "value": comp,                              "inline": True},
                        {"name": "🌍 Land",         "value": country,                           "inline": True},
                        {"name": "📊 Stand",        "value": f"**{score}** | Min. {minute}'",   "inline": True},
                        {"name": "⚽ Spiel",        "value": f"{home} vs {away}",               "inline": False},
                        {"name": "🟥 Rote Karte",   "value": f"**{spieler}** ({geschwaeches})", "inline": False},
                        {"name": "📐 Ecken stark",  "value": f"**{ecken_stark}** ({staerkeres})","inline": True},
                        {"name": "📐 Ecken schwach","value": f"**{ecken_schwach}** ({geschwaeches})","inline": True},
                        {"name": "⏱️ Rest",         "value": f"**{restminuten} Min**",          "inline": True},
                        {"name": "🎯 Tipp",         "value": f"**{geschwaeches} unter {grenze_ecken} Ecken**", "inline": False},
                    ],
                    "footer": {"text": f"RotkarteEcken-Bot • {heute()} {jetzt()}"},
                }
                send_discord_embed(DISCORD_WEBHOOK_ECKEN, embed)
                print(f"  [RotkarteEcken] ✅ {geschwaeches} in Unterzahl – Unter Ecken")
                time.sleep(0.5)
            bot_fehler_reset("RotkarteEcken-Bot")
        except Exception as e:
            bot_fehler_melden("RotkarteEcken-Bot", e)
        time.sleep(FUSSBALL_INTERVAL * 60)

# ============================================================
#  TABELLEN-KONTEXT für PreMatch
# ============================================================

def hole_tabellen_kontext(home: str, away: str,
                           home_id: str, away_id: str, league_id: str) -> str:
    """Gibt kurzen Tabellen-Kontext zurück z.B. Abstiegskampf vs Meisterschaft."""
    try:
        h_stand = get_team_standing(league_id, home_id)
        a_stand = get_team_standing(league_id, away_id)
        if not h_stand or not a_stand:
            return ""
        gesamt  = max(h_stand["position"], a_stand["position"]) + 2
        zeilen  = []
        # Meisterschaftskandidat?
        if h_stand["position"] <= 3:
            zeilen.append(f"🏆 {home} kämpft um die Meisterschaft (Platz {h_stand['position']})")
        if a_stand["position"] <= 3:
            zeilen.append(f"🏆 {away} kämpft um die Meisterschaft (Platz {a_stand['position']})")
        # Abstiegskampf?
        if h_stand["position"] >= gesamt - 3:
            zeilen.append(f"🔴 {home} im Abstiegskampf (Platz {h_stand['position']})")
        if a_stand["position"] >= gesamt - 3:
            zeilen.append(f"🔴 {away} im Abstiegskampf (Platz {a_stand['position']})")
        # Direktes Duell (nahe beieinander)?
        if abs(h_stand["position"] - a_stand["position"]) <= 2:
            zeilen.append(f"⚔️ Direktes Duell: Platz {h_stand['position']} vs Platz {a_stand['position']}")
        return "\n".join(zeilen)
    except Exception:
        return ""

# ============================================================
#  CHART GENERATOR – Performance Bild
# ============================================================

def erstelle_performance_chart() -> str | None:
    """
    Erstellt ein Performance-Liniendiagramm als PNG.
    Gibt Dateipfad zurück oder None bei Fehler.
    """
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import numpy as np

        # Daten aus Signal-Tracker
        with _tracker_lock:
            alle = list(_signal_tracker.values())

        if len(alle) < 5:
            return None

        # Chronologisch sortieren
        alle_sort = sorted(alle, key=lambda x: x.get("signal_zeit", 0))
        labels, quoten_kum = [], []
        gw_ges = 0
        for i, sig in enumerate(alle_sort):
            if sig.get("status") != "ausgewertet":
                continue
            gw_ges += 1 if sig.get("gewonnen") else 0
            ges     = i + 1
            quoten_kum.append(round(gw_ges / ges * 100, 1))
            labels.append(sig.get("typ", "?")[:3].upper())

        if len(quoten_kum) < 3:
            return None

        fig, ax = plt.subplots(figsize=(10, 4), facecolor="#0d1117")
        ax.set_facecolor("#161b22")
        x = range(len(quoten_kum))
        ax.plot(x, quoten_kum, color="#58a6ff", linewidth=2.5, marker="o", markersize=4)
        ax.axhline(y=50, color="#f85149", linestyle="--", linewidth=1, alpha=0.7, label="50%")
        ax.axhline(y=55, color="#3fb950", linestyle="--", linewidth=1, alpha=0.5, label="55% Ziel")
        ax.fill_between(x, quoten_kum, 50,
                         where=[q >= 50 for q in quoten_kum],
                         alpha=0.15, color="#3fb950")
        ax.fill_between(x, quoten_kum, 50,
                         where=[q < 50 for q in quoten_kum],
                         alpha=0.15, color="#f85149")
        ax.set_title("BetlabLIVE – Trefferquote Verlauf", color="#e6edf3",
                      fontsize=14, pad=15)
        ax.set_ylabel("Trefferquote %", color="#8b949e")
        ax.set_xlabel("Tipps", color="#8b949e")
        ax.tick_params(colors="#8b949e")
        ax.spines[:].set_color("#30363d")
        ax.set_ylim(0, 100)
        ax.legend(facecolor="#21262d", labelcolor="#e6edf3", fontsize=9)
        # Aktuellen Wert annotieren
        if quoten_kum:
            ax.annotate(f"{quoten_kum[-1]}%",
                        xy=(len(quoten_kum)-1, quoten_kum[-1]),
                        xytext=(10, 10), textcoords="offset points",
                        color="#58a6ff", fontsize=11, fontweight="bold")
        plt.tight_layout()
        pfad = "/tmp/betlab_chart.png"
        plt.savefig(pfad, dpi=120, bbox_inches="tight", facecolor="#0d1117")
        plt.close()
        return pfad
    except ImportError:
        print("  [Chart] matplotlib nicht installiert")
        return None
    except Exception as e:
        print(f"  [Chart] Fehler: {e}")
        return None

def sende_chart_telegram(pfad: str):
    """Sendet ein Bild via Telegram."""
    try:
        with open(pfad, "rb") as f:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
            requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID,
                                      "caption": f"📊 BetlabLIVE Performance Chart – {heute()}"},
                          files={"photo": f}, timeout=20)
        print("  [Chart] Bild gesendet")
    except Exception as e:
        print(f"  [Chart] Senden-Fehler: {e}")

# ============================================================
#  PDF MONATSBERICHT
# ============================================================

def erstelle_monatsbericht_pdf() -> str | None:
    """Erstellt einen PDF-Monatsbericht."""
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.lib.colors import HexColor
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
        from reportlab.lib.units import cm
        import calendar
        lm = de_now().replace(day=1) - timedelta(days=1)
        monat_name = lm.strftime("%B %Y")
        pfad = f"/tmp/betlab_{lm.strftime('%Y_%m')}.pdf"
        doc  = SimpleDocTemplate(pfad, pagesize=A4,
                                  topMargin=2*cm, bottomMargin=2*cm,
                                  leftMargin=2*cm, rightMargin=2*cm)
        styles = getSampleStyleSheet()
        elems  = []
        bg     = HexColor("#0d1117")
        blue   = HexColor("#58a6ff")
        green  = HexColor("#3fb950")
        red    = HexColor("#f85149")
        grey   = HexColor("#8b949e")

        # Titel
        title_style = styles["Title"]
        title_style.textColor = blue
        elems.append(Paragraph(f"BetlabLIVE – Monatsbericht {monat_name}", title_style))
        elems.append(Spacer(1, 0.5*cm))

        # Zusammenfassung
        gw  = sum(statistik[t]["gewonnen"] for t in statistik)
        vl  = sum(statistik[t]["verloren"] for t in statistik)
        ges = gw + vl
        pct = round(gw / ges * 100) if ges else 0
        br  = bankroll_laden()
        diff = round(br - BANKROLL, 2)

        body = styles["Normal"]
        elems.append(Paragraph(f"<b>Gewonnen:</b> {gw} | <b>Verloren:</b> {vl} | <b>Quote:</b> {pct}%", body))
        elems.append(Paragraph(f"<b>Bankroll:</b> {br}€ ({'+' if diff>=0 else ''}{diff}€ seit Start)", body))
        elems.append(Spacer(1, 0.5*cm))

        # Tabelle nach Bot
        table_data = [["Bot", "Gewonnen", "Verloren", "Quote", "ROI"]]
        bot_namen  = {
            "ecken": "Ecken Unter", "ecken_over": "Ecken Über",
            "karten": "Karten", "torwart": "Torwart", "druck": "Druck",
            "comeback": "Comeback", "torflut": "Torflut",
            "rotkarte": "Rotkarte", "hz1tore": "HZ1 Tore", "vztore": "VZ Tore",
        }
        for typ, s in statistik.items():
            g = s["gewonnen"]; v = s["verloren"]; ges_t = g + v
            q_t = f"{round(g/ges_t*100)}%" if ges_t > 0 else "–"
            roi = f"{'+' if s.get('roi',0)>=0 else ''}{round(s.get('roi',0),2)}€"
            table_data.append([bot_namen.get(typ, typ), str(g), str(v), q_t, roi])

        t = Table(table_data, colWidths=[4.5*cm, 2.5*cm, 2.5*cm, 2.5*cm, 2.5*cm])
        t.setStyle(TableStyle([
            ("BACKGROUND", (0,0), (-1,0), blue),
            ("TEXTCOLOR",  (0,0), (-1,0), HexColor("#ffffff")),
            ("FONTNAME",   (0,0), (-1,0), "Helvetica-Bold"),
            ("GRID",       (0,0), (-1,-1), 0.5, grey),
            ("ROWBACKGROUNDS", (0,1), (-1,-1), [HexColor("#161b22"), HexColor("#21262d")]),
            ("TEXTCOLOR",  (0,1), (-1,-1), HexColor("#e6edf3")),
            ("ALIGN",      (1,0), (-1,-1), "CENTER"),
        ]))
        elems.append(t)
        elems.append(Spacer(1, 0.5*cm))
        elems.append(Paragraph(f"Erstellt: {de_now().strftime('%d.%m.%Y %H:%M')} Uhr", styles["Normal"]))

        doc.build(elems)
        return pfad
    except ImportError:
        print("  [PDF] reportlab nicht installiert – sende Text-Version")
        return None
    except Exception as e:
        print(f"  [PDF] Fehler: {e}")
        return None

def sende_pdf_telegram(pfad: str, monat: str):
    """Sendet PDF via Telegram."""
    try:
        with open(pfad, "rb") as f:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
            requests.post(url,
                data={"chat_id": TELEGRAM_CHAT_ID,
                      "caption": f"📄 Monatsbericht {monat} – BetlabLIVE"},
                files={"document": f}, timeout=30)
        print(f"  [PDF] Monatsbericht gesendet")
    except Exception as e:
        print(f"  [PDF] Senden-Fehler: {e}")

# ============================================================
#  SIGNAL TRACKER – Robustes Auswertungs-System
# ============================================================

_signal_tracker = {}  # match_id+typ → signal dict
_tracker_lock   = threading.Lock()

def tracker_laden():
    """Lädt alle offenen Signale beim Start."""
    import json, os
    global _signal_tracker
    if not os.path.exists(SIGNAL_TRACKER_DATEI):
        return
    try:
        with open(SIGNAL_TRACKER_DATEI, "r") as f:
            data = json.load(f)
        with _tracker_lock:
            _signal_tracker = data
        offen = sum(1 for s in data.values() if s.get("status") == "offen")
        print(f"  [Tracker] {len(data)} Signale geladen, {offen} noch offen")
    except Exception as e:
        print(f"  [Tracker] Ladefehler: {e}")

def tracker_speichern():
    """Speichert Signal-Tracker."""
    import json
    try:
        with _tracker_lock:
            data = dict(_signal_tracker)
        with open(SIGNAL_TRACKER_DATEI, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        print(f"  [Tracker] Speicherfehler: {e}")

def tracker_signal_hinzufuegen(match_id: str, spiel: dict):
    """Registriert ein neues Signal im Tracker."""
    key = f"{match_id}_{spiel.get('typ', '')}"
    with _tracker_lock:
        _signal_tracker[key] = {
            **spiel,
            "match_id":    match_id,
            "status":      "offen",
            "signal_zeit": time.time(),
            "versuche":    0,
            "letzter_versuch": 0,
        }
    tracker_speichern()
    print(f"  [Tracker] ✅ Registriert: {spiel.get('home','?')} vs {spiel.get('away','?')} ({spiel.get('typ','')})")

def tracker_ausgewertet_markieren(key: str, gewonnen: bool):
    """Markiert ein Signal als ausgewertet."""
    with _tracker_lock:
        if key in _signal_tracker:
            _signal_tracker[key]["status"]    = "ausgewertet"
            _signal_tracker[key]["gewonnen"]  = gewonnen
            _signal_tracker[key]["ausgewertet_um"] = de_now().strftime("%Y-%m-%d %H:%M")
    tracker_speichern()

def tracker_get_offene() -> list:
    """Gibt alle noch offenen Signale zurück."""
    jetzt_ts = time.time()
    with _tracker_lock:
        offene = []
        for key, sig in _signal_tracker.items():
            if sig.get("status") != "offen":
                continue
            # Nach 5 Stunden aufgeben
            alter_h = (jetzt_ts - sig.get("signal_zeit", jetzt_ts)) / 3600
            if alter_h > 5:
                _signal_tracker[key]["status"] = "abgelaufen"
                continue
            offene.append((key, sig))
    return offene

# ── Football-Data.org Hilfsfunktionen ─────────────────────────
FD_BASE = "https://api.football-data.org/v4"

# Mapping livescore-api Liga-Namen → football-data.org Competition IDs
FD_LIGA_IDS = {
    "Premier League": 2021,
    "Bundesliga": 2002,
    "La Liga": 2014,
    "Serie A": 2019,
    "Ligue 1": 2015,
    "Primeira Liga": 2017,
    "Eredivisie": 2003,
    "Championship": 2016,
    "Champions League": 2001,
    "Europa League": 2146,
    "World Cup": 2000,
    "European Championship": 2018,
}

def fd_suche_spiel(home: str, away: str, liga: str = "") -> dict | None:
    """Sucht ein Spiel auf football-data.org anhand Teamnamen."""
    if not FOOTBALLDATA_KEY:
        return None
    try:
        headers = {"X-Auth-Token": FOOTBALLDATA_KEY}
        # Versuche über Competition ID
        liga_id = FD_LIGA_IDS.get(liga)
        urls_zu_prüfen = []
        if liga_id:
            urls_zu_prüfen.append(f"{FD_BASE}/competitions/{liga_id}/matches?status=FINISHED")
        urls_zu_prüfen.append(f"{FD_BASE}/matches?status=FINISHED")
        for url in urls_zu_prüfen:
            try:
                resp = requests.get(url, headers=headers, timeout=8)
                if resp.status_code != 200:
                    continue
                for match in resp.json().get("matches", []):
                    h = (match.get("homeTeam") or {}).get("shortName", "") or                         (match.get("homeTeam") or {}).get("name", "")
                    a = (match.get("awayTeam") or {}).get("shortName", "") or                         (match.get("awayTeam") or {}).get("name", "")
                    # Unscharfer Vergleich: mind. 4 Buchstaben übereinstimmend
                    home_short = home[:5].lower()
                    away_short  = away[:5].lower()
                    if (home_short in h.lower() or h.lower()[:5] in home.lower()) and                        (away_short in a.lower() or a.lower()[:5] in away.lower()):
                        score = match.get("score", {})
                        full  = score.get("fullTime", {})
                        half  = score.get("halfTime", {})
                        h_ft  = full.get("home")
                        a_ft  = full.get("away")
                        h_ht  = half.get("home")
                        a_ht  = half.get("away")
                        status = match.get("status", "")
                        if status == "FINISHED" and h_ft is not None and a_ft is not None:
                            return {
                                "status": "FT",
                                "score": f"{h_ft} - {a_ft}",
                                "ht_score": f"{h_ht} - {a_ht}" if h_ht is not None else "",
                                "quelle": "football-data.org",
                                "home": h, "away": a,
                            }
            except Exception as e:
                print(f"  [FD] Fehler bei {url}: {e}")
                continue
    except Exception as e:
        print(f"  [FD] Suche Fehler: {e}")
    return None

def thesportsdb_suche_spiel(home: str, away: str) -> dict | None:
    """Sucht Spielergebnis auf TheSportsDB (kostenlos, kein Key)."""
    try:
        # TheSportsDB Live Events
        resp = requests.get(
            "https://www.thesportsdb.com/api/v1/json/3/liveevents.php",
            timeout=8
        )
        if resp.status_code != 200:
            return None
        for ev in (resp.json().get("events") or []):
            if ev.get("strSport", "").lower() != "soccer":
                continue
            h_name = ev.get("strHomeTeam", "").lower()
            a_name = ev.get("strAwayTeam", "").lower()
            if home[:5].lower() in h_name and away[:5].lower() in a_name:
                h_score = ev.get("intHomeScore")
                a_score = ev.get("intAwayScore")
                status  = ev.get("strStatus", "").lower()
                if status in ("ft", "finished", "aet") and h_score is not None:
                    return {
                        "status": "FT",
                        "score": f"{h_score} - {a_score}",
                        "ht_score": "",
                        "quelle": "thesportsdb",
                    }
    except Exception as e:
        print(f"  [TSDB] Fehler: {e}")
    return None

def ls_get_match_result(match_id: str, home: str = "",
                         away: str = "", liga: str = "") -> dict | None:
    """
    TRIPLE-VERIFIKATION: Prüft Spielergebnis über 3 unabhängige Quellen.
    Erst wenn 2 von 3 übereinstimmen → sicheres Ergebnis.
    """
    ergebnisse = {}  # quelle → score

    # ── Quelle 1: livescore-api ──────────────────────────────────
    try:
        match    = ls_get_single_match(match_id)
        status   = str(match.get("status", "") or "")
        time_val = str(match.get("time", "") or "")
        score    = (match.get("scores") or {}).get("score", "")
        ht_score = (match.get("scores") or {}).get("ht_score", "")
        if time_val.upper() in ("FT", "FULL TIME", "AET"):
            status = "FT"
        if status in FT_STATI and score and score != "0 - 0":
            ergebnisse["livescore"] = score
            print(f"  [Triple] Quelle 1 livescore-api: {score} ✅")
        elif status in FT_STATI:
            ergebnisse["livescore_ft"] = "0 - 0"  # FT aber 0-0 → unsicher
    except Exception as e:
        print(f"  [Triple] Livescore Fehler: {e}")

    # ── Quelle 2: football-data.org ──────────────────────────────
    if home and away:
        try:
            fd_result = fd_suche_spiel(home, away, liga)
            if fd_result:
                ergebnisse["football_data"] = fd_result["score"]
                ht_score = fd_result.get("ht_score", ht_score)
                print(f"  [Triple] Quelle 2 football-data.org: {fd_result['score']} ✅")
        except Exception as e:
            print(f"  [Triple] football-data Fehler: {e}")

    # ── Quelle 3: TheSportsDB ────────────────────────────────────
    if home and away and len(ergebnisse) < 2:
        try:
            tsdb = thesportsdb_suche_spiel(home, away)
            if tsdb:
                ergebnisse["thesportsdb"] = tsdb["score"]
                print(f"  [Triple] Quelle 3 TheSportsDB: {tsdb['score']} ✅")
        except Exception as e:
            print(f"  [Triple] TheSportsDB Fehler: {e}")

    # ── Quelle 4: Events Fallback ────────────────────────────────
    if not ergebnisse:
        try:
            live     = get_live_matches()
            live_ids = {str(m.get("id")) for m in live}
            if match_id not in live_ids:
                events   = ls_get_events(match_id)
                tore_h   = len([e for e in events if e.get("event") in ("Goal","goal")
                                 and e.get("home_away") == "home"])
                tore_a   = len([e for e in events if e.get("event") in ("Goal","goal")
                                 and e.get("home_away") == "away"])
                if events:
                    ev_score = f"{tore_h} - {tore_a}"
                    ergebnisse["events"] = ev_score
                    print(f"  [Triple] Quelle 4 Events: {ev_score} ✅")
        except Exception as e:
            print(f"  [Triple] Events Fehler: {e}")

    # ── Entscheidung: 2 von 3 müssen übereinstimmen ──────────────
    if not ergebnisse:
        print(f"  [Triple] ❌ Keine Quelle hat Ergebnis geliefert")
        return None

    # Scores zählen (ohne unsichere 0-0 FT-Markierung)
    sichere = {k: v for k, v in ergebnisse.items() if k != "livescore_ft"}

    if not sichere:
        print(f"  [Triple] ⚠️ Nur unsichere 0-0 Meldung – warte auf Bestätigung")
        return None

    # Wenn mindestens 2 Quellen dasselbe Ergebnis liefern
    from collections import Counter
    zaehler = Counter(sichere.values())
    bestes  = zaehler.most_common(1)[0]
    bester_score, anzahl = bestes

    if anzahl >= 2:
        print(f"  [Triple] ✅ BESTÄTIGT ({anzahl}/3 Quellen): {bester_score}")
        return {"status": "FT", "score": bester_score,
                "ht_score": ht_score, "quelle": f"triple_verified ({anzahl}/3)",
                "alle_quellen": ergebnisse}
    elif len(sichere) == 1:
        # Nur eine Quelle – trotzdem verwenden wenn nicht 0-0
        einziger_score = list(sichere.values())[0]
        if einziger_score != "0 - 0":
            print(f"  [Triple] ⚠️ Nur 1 Quelle: {einziger_score} – verwende trotzdem")
            return {"status": "FT", "score": einziger_score,
                    "ht_score": ht_score, "quelle": "single_source",
                    "alle_quellen": ergebnisse}
    else:
        # Quellen widersprechen sich
        print(f"  [Triple] ⚠️ Quellen uneinig: {sichere} – warte auf mehr Daten")

    return None

def bot_nachschau():
    """
    Dedizierter Nachschau-Bot: prüft alle offenen Signale
    alle 3 Minuten und wertet ab sobald das Spiel fertig ist.
    Viel aggressiver als der normale Auswertungs-Bot.
    """
    print("[Nachschau-Bot] Gestartet | Prüft offene Signale alle 3 Min")

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

    # MIN_WARTE auf 0 – sofort auswerten wenn FT erkannt
    # Keine Zeitabhängigkeit mehr – nur Status zählt
    MIN_WARTE = {
        "ecken": 0, "torflut": 0, "hz1tore": 0,
        "vztore": 0, "karten": 0, "torwart": 0,
        "comeback": 0, "druck": 0, "rotkarte": 0, "ecken_over": 0,
    }

    while True:
        try:
            wende_konfidenz_decay_an()
            offene = tracker_get_offene()
            if offene:
                print(f"[{jetzt()}] [Nachschau-Bot] {len(offene)} offene Signale prüfen")

            for key, sig in offene:
                match_id  = sig.get("match_id", "")
                typ       = sig.get("typ", "")
                home      = sig.get("home", "?")
                away      = sig.get("away", "?")

                # Mindest-Wartezeit einhalten
                signal_zeit = sig.get("signal_zeit", 0)
                min_seit    = (time.time() - signal_zeit) / 60
                min_warte   = MIN_WARTE.get(typ, 25)
                if min_seit < min_warte:
                    print(f"  [Nachschau] {home} vs {away} | Warte noch {min_warte-min_seit:.0f} Min")
                    continue

                # Nicht öfter als alle 4 Minuten versuchen
                letzter = sig.get("letzter_versuch", 0)
                if time.time() - letzter < 240:
                    continue

                # Versuchszähler erhöhen
                with _tracker_lock:
                    if key in _signal_tracker:
                        _signal_tracker[key]["versuche"] += 1
                        _signal_tracker[key]["letzter_versuch"] = time.time()

                print(f"  [Nachschau] Prüfe: {home} vs {away} ({typ}) | Versuch #{sig.get('versuche',0)+1}")

                # Ergebnis holen
                result = ls_get_match_result(
                    match_id,
                    home=sig.get("home", ""),
                    away=sig.get("away", ""),
                    liga=sig.get("competition", sig.get("liga", ""))
                )
                if not result:
                    print(f"  [Nachschau] {home} vs {away} | Noch kein Ergebnis")
                    continue

                # Kurz warten für stabile Daten
                time.sleep(8)

                # Auswertung durchführen
                auswert_fn = AUSWERTUNG_FNS.get(typ)
                if not auswert_fn:
                    tracker_ausgewertet_markieren(key, False)
                    continue

                msg = None
                try:
                    msg = auswert_fn(sig)
                except Exception as e:
                    print(f"  [Nachschau] Auswertungs-Fehler {home} vs {away}: {e}")

                if not msg:
                    # Nach 5 Versuchen aufgeben
                    if sig.get("versuche", 0) >= 5:
                        print(f"  [Nachschau] ❌ Aufgegeben nach 5 Versuchen: {home} vs {away}")
                        tracker_ausgewertet_markieren(key, False)
                    continue

                # Ergebnis senden
                gewonnen = "GEWONNEN" in msg
                send_telegram(msg)

                # Discord Embed
                webhook = sig.get("webhook", "")
                emoji   = "✅" if gewonnen else "❌"
                details = {"📊 Typ": f"**{typ.upper()}**",
                           "⚽ Spiel": f"**{home} vs {away}**"}
                embed = discord_auswertung(typ, home, away, gewonnen, details)
                send_discord_embed(webhook, embed)

                tracker_ausgewertet_markieren(key, gewonnen)
                check_streak_alarm()
                discord_vote_auswerten(key, gewonnen)
                print(f"  [Nachschau] ✅ Ausgewertet: {home} vs {away} ({typ}) → {'GEWONNEN' if gewonnen else 'VERLOREN'}")

                # Claude Verlust-Analyse
                if not gewonnen:
                    threading.Thread(target=claude_verloren_analyse,
                        args=(home, away, typ, msg), daemon=True).start()

                time.sleep(1)

            tracker_speichern()
            bot_fehler_reset("Nachschau-Bot")
        except Exception as e:
            bot_fehler_melden("Nachschau-Bot", e)
        time.sleep(90)  # Alle 90 Sekunden – aggressiver für schnelle Auswertung

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
    print("  ⚽ FUSSBALL BOTS v52")
    print("  Value Bets · CS2 · Telegram Befehle · Bankroll · Multi-Signal · Persistenz")
    print("=" * 50 + "\n")

    # Daten von GitHub wiederherstellen (falls Railway neu gestartet hat)
    print("[Startup] Prüfe ob Daten von GitHub wiederhergestellt werden müssen...")
    github_restore()

    statistik_laden()
    beobachtete_spiele_laden()
    tracker_laden()
    notified_sets_laden()
    whitelist_laden()
    admins_laden()
    bekannte_user_laden()
    ab_test_laden()
    manuell_tipps_laden()
    community_laden()
    telegram_filter_laden()
    community_system_laden()
    rang_laden()
    discord_votes_laden()

    # Dynamische Filter laden (Funktion weiter unten definiert)
    try:
        dynamische_filter_laden()
    except Exception as e:
        print(f"  [Startup] dynamische_filter_laden: {e}")

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
        ("Backup-Bot",       bot_github_backup),
        ("Auswertung-Bot",   bot_auswertung_und_berichte),
        ("Nachschau-Bot",    bot_nachschau),
        ("Value-Bot",        bot_value_bet),
        ("xG-Bot",           bot_xg),
        ("Arbitrage-Bot",    bot_arbitrage),
        ("EarlyGoal-Bot",    bot_early_goal),
        ("RotkarteEcken-Bot",bot_rotkarte_ecken),
        ("Odds-Tracker",     bot_odds_tracker),
        ("Anomalie-Bot",     bot_anomalie_erkennung),
        ("Sharp-Money-Bot",  bot_sharp_money),
        ("Hedge-Alarm-Bot",  bot_hedge_alarm),
        ("Quotenvergleich",  bot_quotenvergleich),
        ("Bonus-Tracker",    bot_bonus_tracker),
        ("HZ2-Tore-Bot",     bot_hz2_tore),
        ("CornerRush-Bot",    bot_corner_rush),
        ("TippDesTages-Bot",  bot_tipp_des_tages),
        ("Morgen-Bot",        bot_morgen_uebersicht),
        ("Selbstlern-Bot",   bot_selbstlernend),
        ("Wetter-Bot",       bot_wetter_tipp),
        ("Gruppen-Bot",      bot_telegram_gruppe),
        ("CS2-Bot",          bot_cs2),
    ]

    # Startup Alarm NACH bot_definitionen
    try:
        bot_startup_alarm()
    except Exception as e:
        print(f"  [Startup] Alarm: {e}")

    # Targets für Watchdog merken
    for name, target in bot_definitionen:
        _bot_targets[name] = target

    threads = []
    for name, target in bot_definitionen:
        t = threading.Thread(target=target, daemon=True, name=name)
        threads.append(t)
        t.start()
        time.sleep(2)

    # Health-Check starten (Port 8081)
    health_thread = threading.Thread(target=bot_health_check_server, daemon=True, name="HealthCheck")
    health_thread.start()

    # Dashboard starten (Port 8080)
    dashboard = threading.Thread(target=bot_web_dashboard, daemon=True, name="Dashboard")
    dashboard.start()

    # Watchdog starten
    watchdog = threading.Thread(target=bot_watchdog, daemon=True, name="Watchdog")
    watchdog.start()

    print("Alle Bots laufen!\n")
    while True:
        time.sleep(60)
