# v3 - nur Fussball Bots
import requests
import time
import threading
from datetime import datetime

# ============================================================
#  KONFIGURATION – hier deine Daten eintragen
# ============================================================
API_KEY            = "E2xvBtoCuexKFTUt"
API_SECRET         = "FiAUHwmqoVBQqo64rDA26ZFBddlT6gmM"

TELEGRAM_BOT_TOKEN = "8706066107:AAFAQhT3k0jhTZ7ep-VWHPlskOKJVvsfucQ"
TELEGRAM_CHAT_ID   = "7272001004"

MAX_CORNERS         = 5
MIN_KARTEN          = 2
KARTEN_BIS_MINUTE   = 30
MIN_SHOTS_ON_TARGET = 5
FUSSBALL_INTERVAL   = 2
# ============================================================

BASE_URL_FB  = "https://livescore-api.com/api-client"
AUTH_FB      = {"key": API_KEY, "secret": API_SECRET}
KARTEN_TYPEN = {"YELLOW_CARD", "RED_CARD", "YELLOW_RED_CARD"}

notified_ecken   = set()
notified_karten  = set()
notified_torwart = set()

def jetzt():
    return datetime.now().strftime("%H:%M")

def send_telegram(message: str):
    url     = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    resp    = requests.post(url, json=payload, timeout=10)
    if resp.status_code != 200:
        print(f"  [Telegram Fehler] {resp.text}")

def get_live_matches():
    resp = requests.get(f"{BASE_URL_FB}/matches/live.json", params=AUTH_FB, timeout=10)
    resp.raise_for_status()
    return resp.json().get("data", {}).get("match", [])

def get_statistiken(match_id):
    params = {**AUTH_FB, "match_id": match_id}
    resp   = requests.get(f"{BASE_URL_FB}/statistics/matches.json", params=params, timeout=10)
    resp.raise_for_status()
    result = {}
    for s in resp.json().get("data", []):
        result[s.get("type")] = {"home": s.get("home") or 0, "away": s.get("away") or 0}
    return result

def get_events(match_id):
    params = {**AUTH_FB, "id": match_id}
    resp   = requests.get(f"{BASE_URL_FB}/matches/events.json", params=params, timeout=10)
    resp.raise_for_status()
    return resp.json().get("data", {}).get("event", []) or []

def karten_emoji(typ):
    if typ == "YELLOW_CARD":     return "🟨"
    if typ == "RED_CARD":        return "🟥"
    if typ == "YELLOW_RED_CARD": return "🟨🟥"
    return "🃏"

def bot_ecken():
    print(f"[Ecken-Bot] Gestartet | max. {MAX_CORNERS} Ecken in HZ1")
    while True:
        try:
            matches  = get_live_matches()
            halftime = [m for m in matches if m.get("status") == "HALF TIME BREAK"]
            print(f"[{jetzt()}] [Ecken-Bot] {len(halftime)} Halbzeit-Spiele")
            for game in halftime:
                match_id = game.get("id")
                if match_id in notified_ecken:
                    continue
                stats   = get_statistiken(match_id)
                corners = int(stats.get("corners", {}).get("home", 0)) + int(stats.get("corners", {}).get("away", 0))
                home    = game.get("home", {}).get("name", "?")
                away    = game.get("away", {}).get("name", "?")
                comp    = game.get("competition", {}).get("name", "?")
                country = (game.get("country") or {}).get("name", "International")
                score   = game.get("scores", {}).get("score", "?")
                if corners <= MAX_CORNERS:
                    msg = (f"⚽ <b>HALBZEIT-TIPP!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                           f"🏆 {comp} ({country})\n📌 {home} vs {away}\n"
                           f"📊 Stand: <b>{score}</b>\n📐 Ecken HZ1: <b>{corners}</b> (≤{MAX_CORNERS})\n"
                           f"━━━━━━━━━━━━━━━━━━━━\n🕐 {jetzt()} Uhr")
                    send_telegram(msg)
                    notified_ecken.add(match_id)
                    print(f"  [Ecken-Bot] OK: {home} vs {away} ({corners} Ecken)")
                time.sleep(0.5)
        except Exception as e:
            print(f"  [Ecken-Bot] Fehler: {e}")
        time.sleep(FUSSBALL_INTERVAL * 60)

def bot_karten():
    print(f"[Karten-Bot] Gestartet | mind. {MIN_KARTEN} Karten bis Minute {KARTEN_BIS_MINUTE}")
    while True:
        try:
            matches = get_live_matches()
            laufend = [m for m in matches if m.get("status") in ("IN PLAY", "ADDED TIME")]
            print(f"[{jetzt()}] [Karten-Bot] {len(laufend)} laufende Spiele")
            for game in laufend:
                match_id = game.get("id")
                if match_id in notified_karten:
                    continue
                try:
                    minute = int(game.get("time", 0))
                except:
                    continue
                if minute > KARTEN_BIS_MINUTE + 5:
                    continue
                events = get_events(match_id)
                karten = [e for e in events if e.get("event") in KARTEN_TYPEN and e.get("time", 999) <= KARTEN_BIS_MINUTE]
                home   = game.get("home", {}).get("name", "?")
                away   = game.get("away", {}).get("name", "?")
                comp   = game.get("competition", {}).get("name", "?")
                country= (game.get("country") or {}).get("name", "International")
                score  = game.get("scores", {}).get("score", "?")
                if len(karten) >= MIN_KARTEN:
                    zeilen = ""
                    for k in karten:
                        spieler = (k.get("player") or {}).get("name", "?")
                        team    = home if k.get("is_home") else away
                        zeilen += f"  {karten_emoji(k.get('event'))} {k.get('time')}' {spieler} ({team})\n"
                    msg = (f"🃏 <b>KARTEN-ALARM!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                           f"🏆 {comp} ({country})\n📌 {home} vs {away}\n"
                           f"📊 Stand: <b>{score}</b> | Minute: <b>{minute}'</b>\n"
                           f"━━━━━━━━━━━━━━━━━━━━\n<b>{len(karten)} Karten bis Min. {KARTEN_BIS_MINUTE}:</b>\n{zeilen}"
                           f"━━━━━━━━━━━━━━━━━━━━\n🕐 {jetzt()} Uhr")
                    send_telegram(msg)
                    notified_karten.add(match_id)
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
                match_id = game.get("id")
                if match_id in notified_torwart:
                    continue
                score = game.get("scores", {}).get("score", "")
                if "0 - 0" not in score and "0-0" not in score:
                    continue
                stats      = get_statistiken(match_id)
                shots_home = int(stats.get("shots_on_target", {}).get("home", 0))
                shots_away = int(stats.get("shots_on_target", {}).get("away", 0))
                shots_ges  = shots_home + shots_away
                saves_home = int(stats.get("saves", {}).get("home", 0))
                saves_away = int(stats.get("saves", {}).get("away", 0))
                poss_home  = stats.get("possesion", {}).get("home", "?")
                poss_away  = stats.get("possesion", {}).get("away", "?")
                home       = game.get("home", {}).get("name", "?")
                away       = game.get("away", {}).get("name", "?")
                comp       = game.get("competition", {}).get("name", "?")
                country    = (game.get("country") or {}).get("name", "International")
                minute     = game.get("time", "?")
                status     = game.get("status", "")
                min_text   = "Halbzeit" if status == "HALF TIME BREAK" else f"{minute}'"
                if shots_ges >= MIN_SHOTS_ON_TARGET:
                    msg = (f"🧤 <b>TORWART-ALARM!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                           f"🏆 {comp} ({country})\n📌 {home} vs {away}\n"
                           f"📊 Stand: <b>0:0</b> | {min_text}\n━━━━━━━━━━━━━━━━━━━━\n"
                           f"🎯 Schüsse: <b>{shots_ges}</b> ({shots_home}|{shots_away})\n"
                           f"🧤 Paraden: <b>{saves_home+saves_away}</b> ({saves_home}|{saves_away})\n"
                           f"⚽ Ballbesitz: {poss_home}%|{poss_away}%\n"
                           f"━━━━━━━━━━━━━━━━━━━━\n🕐 {jetzt()} Uhr")
                    send_telegram(msg)
                    notified_torwart.add(match_id)
                    print(f"  [Torwart-Bot] OK: {home} vs {away} | {shots_ges} Schüsse")
                time.sleep(0.5)
        except Exception as e:
            print(f"  [Torwart-Bot] Fehler: {e}")
        time.sleep(FUSSBALL_INTERVAL * 60)

if __name__ == "__main__":
    print("=" * 50)
    print("  ⚽ FUSSBALL BOTS GESTARTET")
    print("  Ecken + Karten + Torwart")
    print("=" * 50 + "\n")

    threads = [
        threading.Thread(target=bot_ecken,   daemon=True, name="Ecken-Bot"),
        threading.Thread(target=bot_karten,  daemon=True, name="Karten-Bot"),
        threading.Thread(target=bot_torwart, daemon=True, name="Torwart-Bot"),
    ]

    for t in threads:
        t.start()
        time.sleep(2)

    print("Alle Bots laufen!\n")
    while True:
        time.sleep(60)
