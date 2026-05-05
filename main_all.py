# v4 - mit Auswertung nach Spielende
import requests
import time
import threading
from datetime import datetime, timezone, timedelta

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

# Spiele die wir beobachten – für Auswertung nach Spielende
# Format: match_id -> {"typ": "ecken"/"karten"/"torwart", "daten": {...}}
beobachtete_spiele = {}
auswertung_done    = set()

def jetzt():
    de_time = datetime.now(timezone.utc) + timedelta(hours=2)
    return de_time.strftime("%H:%M")

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

def get_finished_matches():
    resp = requests.get(f"{BASE_URL_FB}/matches/live.json", params={**AUTH_FB, "status": "FINISHED"}, timeout=10)
    resp.raise_for_status()
    return resp.json().get("data", {}).get("match", []) or []

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

# ============================================================
#  AUSWERTUNG nach Spielende
# ============================================================

def auswertung_ecken(spiel):
    """Ecken-Tipp: HZ1-Ecken * 2 + 1 = Grenze für Unter-Wette"""
    match_id   = spiel["match_id"]
    hz1_ecken  = spiel["hz1_ecken"]
    grenze     = hz1_ecken * 2 + 2  # z.B. 4 Ecken HZ1 -> Unter 9 Ecken
    home       = spiel["home"]
    away       = spiel["away"]

    try:
        stats       = get_statistiken(match_id)
        total_ecken = int(stats.get("corners", {}).get("home", 0)) + int(stats.get("corners", {}).get("away", 0))
        gewonnen    = total_ecken < grenze
        emoji       = "✅ GEWONNEN" if gewonnen else "❌ VERLOREN"

        return (
            f"📊 <b>AUSWERTUNG – Ecken-Tipp</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📌 {home} vs {away}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📐 Ecken HZ1: <b>{hz1_ecken}</b>\n"
            f"🎯 Tipp: Unter <b>{grenze}</b> Ecken gesamt\n"
            f"📈 Tatsächlich: <b>{total_ecken}</b> Ecken\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"{emoji}\n"
            f"🕐 {jetzt()} Uhr"
        )
    except Exception as e:
        return None

def auswertung_torwart(spiel):
    """Torwart-Tipp: Mindestens 1 Tor fällt noch"""
    match_id = spiel["match_id"]
    home     = spiel["home"]
    away     = spiel["away"]

    try:
        # Endstand holen
        params = {**AUTH_FB, "id": match_id}
        resp   = requests.get(f"{BASE_URL_FB}/matches/single.json", params=params, timeout=10)
        resp.raise_for_status()
        match    = resp.json().get("data", {}).get("match", {})
        score    = match.get("scores", {}).get("score", "0 - 0")
        
        # Tore zählen
        parts = score.replace(" ", "").split("-")
        tore_gesamt = int(parts[0]) + int(parts[1]) if len(parts) == 2 else 0
        gewonnen    = tore_gesamt >= 1
        emoji       = "✅ GEWONNEN" if gewonnen else "❌ VERLOREN"

        return (
            f"📊 <b>AUSWERTUNG – Torwart-Tipp</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📌 {home} vs {away}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🎯 Tipp: Mindestens 1 Tor fällt noch\n"
            f"📈 Endstand: <b>{score}</b> ({tore_gesamt} Tore gesamt)\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"{emoji}\n"
            f"🕐 {jetzt()} Uhr"
        )
    except Exception as e:
        return None

def auswertung_karten(spiel):
    """Karten-Tipp: Über 5 Karten im gesamten Spiel"""
    match_id      = spiel["match_id"]
    home          = spiel["home"]
    away          = spiel["away"]
    karten_hz1    = spiel["karten_anzahl"]
    KARTEN_GRENZE = 5

    try:
        events        = get_events(match_id)
        karten_gesamt = [e for e in events if e.get("event") in KARTEN_TYPEN]
        anzahl        = len(karten_gesamt)
        gewonnen      = anzahl > KARTEN_GRENZE
        emoji         = "✅ GEWONNEN" if gewonnen else "❌ VERLOREN"

        return (
            f"📊 <b>AUSWERTUNG – Karten-Tipp</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📌 {home} vs {away}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🃏 Karten nach 30 Min.: <b>{karten_hz1}</b>\n"
            f"🎯 Tipp: Über <b>{KARTEN_GRENZE}</b> Karten gesamt\n"
            f"📈 Tatsächlich: <b>{anzahl}</b> Karten\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"{emoji}\n"
            f"🕐 {jetzt()} Uhr"
        )
    except Exception as e:
        return None

# ============================================================
#  AUSWERTUNGS-THREAD – prüft ob beobachtete Spiele fertig sind
# ============================================================

def bot_auswertung():
    print("[Auswertung-Bot] Gestartet | prüft Spielende alle 5 Min.")
    while True:
        try:
            if beobachtete_spiele:
                # Alle laufenden Spiele holen um Status zu prüfen
                alle = get_live_matches()
                live_ids = {m.get("id") for m in alle}

                for match_id, spiel in list(beobachtete_spiele.items()):
                    if match_id in auswertung_done:
                        continue

                    # Spiel nicht mehr live = beendet
                    if match_id not in live_ids:
                        time.sleep(30)  # kurz warten damit finale Stats da sind
                        typ = spiel["typ"]
                        msg = None

                        if typ == "ecken":
                            msg = auswertung_ecken(spiel)
                        elif typ == "torwart":
                            msg = auswertung_torwart(spiel)
                        elif typ == "karten":
                            msg = auswertung_karten(spiel)

                        if msg:
                            send_telegram(msg)
                            auswertung_done.add(match_id)
                            print(f"  [Auswertung] Gesendet: {spiel['home']} vs {spiel['away']} ({typ})")

        except Exception as e:
            print(f"  [Auswertung-Bot] Fehler: {e}")

        time.sleep(5 * 60)

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
                grenze  = corners * 2 + 2

                if corners <= MAX_CORNERS:
                    msg = (f"⚽ <b>HALBZEIT-TIPP!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                           f"🏆 {comp} ({country})\n📌 {home} vs {away}\n"
                           f"📊 Stand: <b>{score}</b>\n"
                           f"📐 Ecken HZ1: <b>{corners}</b>\n"
                           f"🎯 Tipp: Unter <b>{grenze}</b> Ecken gesamt\n"
                           f"━━━━━━━━━━━━━━━━━━━━\n🕐 {jetzt()} Uhr")
                    send_telegram(msg)
                    notified_ecken.add(match_id)
                    # Für Auswertung merken
                    beobachtete_spiele[match_id] = {
                        "typ": "ecken", "match_id": match_id,
                        "home": home, "away": away, "hz1_ecken": corners
                    }
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
                           f"🎯 Tipp: Über <b>5</b> Karten gesamt\n"
                           f"━━━━━━━━━━━━━━━━━━━━\n🕐 {jetzt()} Uhr")
                    send_telegram(msg)
                    notified_karten.add(match_id)
                    beobachtete_spiele[match_id] = {
                        "typ": "karten", "match_id": match_id,
                        "home": home, "away": away, "karten_anzahl": len(karten)
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
                           f"🎯 Tipp: Mindestens <b>1 Tor</b> fällt noch\n"
                           f"━━━━━━━━━━━━━━━━━━━━\n🕐 {jetzt()} Uhr")
                    send_telegram(msg)
                    notified_torwart.add(match_id)
                    beobachtete_spiele[match_id] = {
                        "typ": "torwart", "match_id": match_id,
                        "home": home, "away": away
                    }
                    print(f"  [Torwart-Bot] OK: {home} vs {away} | {shots_ges} Schüsse")
                time.sleep(0.5)
        except Exception as e:
            print(f"  [Torwart-Bot] Fehler: {e}")
        time.sleep(FUSSBALL_INTERVAL * 60)

if __name__ == "__main__":
    print("=" * 50)
    print("  ⚽ FUSSBALL BOTS GESTARTET")
    print("  Ecken + Karten + Torwart + Auswertung")
    print("=" * 50 + "\n")

    threads = [
        threading.Thread(target=bot_ecken,      daemon=True, name="Ecken-Bot"),
        threading.Thread(target=bot_karten,      daemon=True, name="Karten-Bot"),
        threading.Thread(target=bot_torwart,     daemon=True, name="Torwart-Bot"),
        threading.Thread(target=bot_auswertung,  daemon=True, name="Auswertung-Bot"),
    ]

    for t in threads:
        t.start()
        time.sleep(2)

    print("Alle Bots laufen!\n")
    while True:
        time.sleep(60)
