import requests
import time
import threading
import xml.etree.ElementTree as ET
import re
from datetime import datetime

# ============================================================
#  KONFIGURATION – hier deine Daten eintragen
# ============================================================
API_KEY            = "E2xvBtoCuexKFTUt"
API_SECRET         = "FiAUHwmqoVBQqo64rDA26ZFBddlT6gmM"

TELEGRAM_BOT_TOKEN = "8706066107:AAFAQhT3k0jhTZ7ep-VWHPlskOKJVvsfucQ"
TELEGRAM_CHAT_ID   = "7272001004"

# ── Fussball Bot Einstellungen ────────────────────────────
MAX_CORNERS         = 5
MIN_KARTEN          = 2
KARTEN_BIS_MINUTE   = 30
MIN_SHOTS_ON_TARGET = 5
FUSSBALL_INTERVAL   = 2

# ── Kleinanzeigen Bot Einstellungen ──────────────────────
KA_INTERVAL         = 3
# ============================================================

KATEGORIEN = [
    # Smartphones
    {"name": "📱 iPhone 14",          "suche": "iphone 14",          "max_preis": 220},
    {"name": "📱 iPhone 15",          "suche": "iphone 15",          "max_preis": 300},
    {"name": "📱 iPhone 14 Pro",      "suche": "iphone 14 pro",      "max_preis": 360},
    {"name": "📱 Samsung S24",        "suche": "samsung s24",        "max_preis": 300},
    # Gaming
    {"name": "🎮 PS5 Slim",           "suche": "ps5 slim",           "max_preis": 260},
    {"name": "🎮 PS5 Pro",            "suche": "ps5 pro",            "max_preis": 500},
    {"name": "🎮 Nintendo Switch 2",  "suche": "nintendo switch 2",  "max_preis": 280},
    {"name": "🎮 Xbox Series X",      "suche": "xbox series x",      "max_preis": 250},
    # Laptop / Mac
    {"name": "💻 MacBook Air M2",     "suche": "macbook air m2",     "max_preis": 520},
    {"name": "💻 MacBook Pro M3",     "suche": "macbook pro m3",     "max_preis": 950},
    # Grafikkarten
    {"name": "🖥️ RTX 4070",          "suche": "rtx 4070",           "max_preis": 300},
    {"name": "🖥️ RTX 4080",          "suche": "rtx 4080",           "max_preis": 620},
    # Sneaker
    {"name": "👟 Jordan 1 Retro",     "suche": "air jordan 1",       "max_preis": 120},
    {"name": "👟 Yeezy 350",          "suche": "yeezy 350",          "max_preis": 130},
    {"name": "👟 Nike Dunk",          "suche": "nike dunk low",      "max_preis": 100},
    # Sammelkarten
    {"name": "🃏 Pokemon Booster",    "suche": "pokemon booster",    "max_preis": 20},
    {"name": "🃏 Pokemon Display",    "suche": "pokemon display",    "max_preis": 80},
    {"name": "🃏 Pokemon Karten",     "suche": "pokemon karten",     "max_preis": 50},
    {"name": "🃏 Magic Gathering",    "suche": "magic gathering",    "max_preis": 30},
    {"name": "🃏 One Piece Karten",   "suche": "one piece karten",   "max_preis": 30},
    # Lego
    {"name": "🧱 Lego Star Wars",     "suche": "lego star wars",     "max_preis": 60},
    {"name": "🧱 Lego Technic",       "suche": "lego technic",       "max_preis": 50},
    # Sonstiges
    {"name": "⌚ Apple Watch",        "suche": "apple watch",        "max_preis": 150},
    {"name": "📷 Sony Alpha",         "suche": "sony alpha",         "max_preis": 400},
    {"name": "🎸 Gitarre",            "suche": "gitarre",            "max_preis": 100},
]

# ============================================================
#  GEMEINSAME HILFSFUNKTIONEN
# ============================================================

def jetzt():
    return datetime.now().strftime("%H:%M")

def send_telegram(message: str):
    url     = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML", "disable_web_page_preview": True}
    resp    = requests.post(url, json=payload, timeout=10)
    if resp.status_code != 200:
        print(f"  [Telegram Fehler] {resp.text}")

# ============================================================
#  FUSSBALL API
# ============================================================

BASE_URL_FB  = "https://livescore-api.com/api-client"
AUTH_FB      = {"key": API_KEY, "secret": API_SECRET}
KARTEN_TYPEN = {"YELLOW_CARD", "RED_CARD", "YELLOW_RED_CARD"}

notified_ecken   = set()
notified_karten  = set()
notified_torwart = set()

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

# ============================================================
#  KLEINANZEIGEN BOT – korrigierte URL
# ============================================================

ka_seen = set()

def get_rss(suchbegriff):
    """Holt RSS ohne maxPrice – Preisfilter machen wir selbst."""
    query = suchbegriff.replace(" ", "-")
    url   = f"https://www.kleinanzeigen.de/s-anzeigen/{query}/k0.rss"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/rss+xml, application/xml, text/xml"
    }
    resp = requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    return resp.text

def preis_aus_text(text):
    """Extrahiert Preis aus Titel/Beschreibung."""
    match = re.search(r'(\d+[\.,]?\d*)\s*[€E]', text)
    if match:
        preis_str = match.group(1).replace(".", "").replace(",", ".")
        try:
            return float(preis_str)
        except:
            return None
    return None

def parse_rss(xml_text, max_preis):
    """Liest Anzeigen und filtert nach Preis."""
    try:
        root  = ET.fromstring(xml_text)
    except ET.ParseError:
        return []
    
    items = []
    for item in root.findall(".//item"):
        titel = item.findtext("title", "").strip()
        link  = item.findtext("link", "").strip()
        desc  = item.findtext("description", "")
        guid  = item.findtext("guid", link)

        # Preis aus Titel oder Beschreibung lesen
        preis_num = preis_aus_text(titel) or preis_aus_text(desc)
        
        # Preisfilter anwenden
        if max_preis > 0 and preis_num is not None:
            if preis_num > max_preis:
                continue
        
        preis_text = f"{int(preis_num)}€" if preis_num else "VB"
        
        items.append({
            "titel": titel,
            "link":  link,
            "preis": preis_text,
            "guid":  guid
        })
    return items

def bot_kleinanzeigen():
    print(f"[KA-Bot] Gestartet | {len(KATEGORIEN)} Kategorien")
    print("[KA-Bot] Initialisierung – lese bestehende Anzeigen...")
    for kat in KATEGORIEN:
        try:
            xml   = get_rss(kat["suche"])
            items = parse_rss(xml, 0)  # Beim Init keinen Preisfilter
            for item in items:
                ka_seen.add(item["guid"])
            time.sleep(2)
        except Exception as e:
            print(f"  [KA-Bot] Init Fehler {kat['name']}: {e}")
    print(f"[KA-Bot] Fertig – {len(ka_seen)} bestehende Anzeigen ignoriert\n")

    while True:
        neu = 0
        try:
            for kat in KATEGORIEN:
                try:
                    xml   = get_rss(kat["suche"])
                    items = parse_rss(xml, kat["max_preis"])
                    for item in items:
                        if item["guid"] not in ka_seen:
                            msg = (f"{kat['name']} <b>SCHNÄPPCHEN!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                                   f"📌 {item['titel']}\n"
                                   f"💰 Preis: <b>{item['preis']}</b>\n"
                                   f"🔗 <a href=\"{item['link']}\">Zur Anzeige</a>\n"
                                   f"━━━━━━━━━━━━━━━━━━━━\n🕐 {jetzt()} Uhr")
                            send_telegram(msg)
                            ka_seen.add(item["guid"])
                            neu += 1
                            print(f"  [KA-Bot] NEU [{kat['name']}]: {item['titel']} – {item['preis']}")
                    time.sleep(2)
                except Exception as e:
                    print(f"  [KA-Bot] Fehler {kat['name']}: {e}")
                    time.sleep(3)
            print(f"[{jetzt()}] [KA-Bot] {len(KATEGORIEN)} Kategorien geprüft | {neu} neu")
        except Exception as e:
            print(f"[KA-Bot] Fehler: {e}")
        time.sleep(KA_INTERVAL * 60)

# ============================================================
#  ALLE BOTS PARALLEL STARTEN
# ============================================================
if __name__ == "__main__":
    print("=" * 55)
    print("  ALLE BOTS WERDEN GESTARTET")
    print("  ⚽ Fussball: Ecken + Karten + Torwart")
    print(f"  🛒 Kleinanzeigen: {len(KATEGORIEN)} Kategorien")
    print("=" * 55 + "\n")

    threads = [
        threading.Thread(target=bot_ecken,        daemon=True, name="Ecken-Bot"),
        threading.Thread(target=bot_karten,        daemon=True, name="Karten-Bot"),
        threading.Thread(target=bot_torwart,       daemon=True, name="Torwart-Bot"),
        threading.Thread(target=bot_kleinanzeigen, daemon=True, name="KA-Bot"),
    ]

    for t in threads:
        t.start()
        time.sleep(2)

    print("\nAlle Bots laufen!\n")
    while True:
        time.sleep(60)
