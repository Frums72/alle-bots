# v5 - Quoten + Trefferquote + Tagesbericht + Wochenbericht
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

EINSATZ            = 10.0  # Simulierter Einsatz pro Tipp in Euro

MAX_CORNERS         = 5
MIN_KARTEN          = 2
KARTEN_BIS_MINUTE   = 30
MIN_SHOTS_ON_TARGET = 5
FUSSBALL_INTERVAL   = 2

TAGESBERICHT_UHRZEIT = 22  # Uhr (deutsche Zeit)
# ============================================================

BASE_URL_FB  = "https://livescore-api.com/api-client"
AUTH_FB      = {"key": API_KEY, "secret": API_SECRET}
KARTEN_TYPEN = {"YELLOW_CARD", "RED_CARD", "YELLOW_RED_CARD"}

notified_ecken   = set()
notified_karten  = set()
notified_torwart = set()
beobachtete_spiele = {}
auswertung_done    = set()

# Statistik-Tracking
statistik = {
    "ecken":   {"gewonnen": 0, "verloren": 0, "gewinn": 0.0},
    "karten":  {"gewonnen": 0, "verloren": 0, "gewinn": 0.0},
    "torwart": {"gewonnen": 0, "verloren": 0, "gewinn": 0.0},
}
wochen_statistik = {
    "ecken":   {"gewonnen": 0, "verloren": 0, "gewinn": 0.0},
    "karten":  {"gewonnen": 0, "verloren": 0, "gewinn": 0.0},
    "torwart": {"gewonnen": 0, "verloren": 0, "gewinn": 0.0},
}
tagesbericht_gesendet = None

# ============================================================
#  HILFSFUNKTIONEN
# ============================================================

def jetzt():
    de_time = datetime.now(timezone.utc) + timedelta(hours=2)
    return de_time.strftime("%H:%M")

def heute():
    de_time = datetime.now(timezone.utc) + timedelta(hours=2)
    return de_time.strftime("%d.%m.%Y")

def de_now():
    return datetime.now(timezone.utc) + timedelta(hours=2)

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

# ============================================================
#  QUOTEN ABRUFEN (The Odds API - kostenlos)
# ============================================================

ODDS_API_KEY = "866948de5d6c34ca51faf6bd77e0bb2a"  # Optional: kostenloser Key von the-odds-api.com

def get_quote(home, away, typ):
    """Versucht eine passende Quote zu finden."""
    if not ODDS_API_KEY:
        return None
    try:
        url    = "https://api.the-odds-api.com/v4/sports/soccer/odds/"
        params = {
            "apiKey":   ODDS_API_KEY,
            "regions":  "eu",
            "markets":  "totals",
            "oddsFormat": "decimal"
        }
        resp = requests.get(url, params=params, timeout=8)
        if resp.status_code != 200:
            return None
        games = resp.json()
        for game in games:
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
#  STATISTIK UPDATEN & BERICHTE
# ============================================================

def update_statistik(typ, gewonnen, quote):
    global statistik, wochen_statistik
    if gewonnen:
        gewinn = round((quote - 1) * EINSATZ, 2) if quote else EINSATZ * 0.7
        statistik[typ]["gewonnen"]    += 1
        statistik[typ]["gewinn"]      += gewinn
        wochen_statistik[typ]["gewonnen"] += 1
        wochen_statistik[typ]["gewinn"]   += gewinn
    else:
        statistik[typ]["verloren"]    += 1
        statistik[typ]["gewinn"]      -= EINSATZ
        wochen_statistik[typ]["verloren"] += 1
        wochen_statistik[typ]["gewinn"]   -= EINSATZ

def statistik_zeile(name, stat):
    gesamt   = stat["gewonnen"] + stat["verloren"]
    if gesamt == 0:
        return f"{name}: Noch keine Tipps"
    quote    = round((stat["gewonnen"] / gesamt) * 100)
    gewinn   = round(stat["gewinn"], 2)
    emoji    = "📈" if gewinn >= 0 else "📉"
    return (f"{name}: {stat['gewonnen']}/{gesamt} ({quote}%) "
            f"{emoji} {'+' if gewinn >= 0 else ''}{gewinn}€")

def send_tagesbericht():
    gesamt_gewonnen = sum(statistik[t]["gewonnen"] for t in statistik)
    gesamt_verloren = sum(statistik[t]["verloren"] for t in statistik)
    gesamt_tipps    = gesamt_gewonnen + gesamt_verloren
    gesamt_gewinn   = round(sum(statistik[t]["gewinn"] for t in statistik), 2)
    quote_pct       = round((gesamt_gewonnen / gesamt_tipps * 100)) if gesamt_tipps else 0
    gewinn_emoji    = "📈" if gesamt_gewinn >= 0 else "📉"

    msg = (
        f"📋 <b>TAGESBERICHT – {heute()}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"⏰ Stand: {jetzt()} Uhr\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>Gesamt heute:</b>\n"
        f"✅ Gewonnen: <b>{gesamt_gewonnen}</b>\n"
        f"❌ Verloren: <b>{gesamt_verloren}</b>\n"
        f"🎯 Trefferquote: <b>{quote_pct}%</b>\n"
        f"{gewinn_emoji} Simulation ({EINSATZ}€/Tipp): "
        f"<b>{'+' if gesamt_gewinn >= 0 else ''}{gesamt_gewinn}€</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>Nach Bot:</b>\n"
        f"⚽ {statistik_zeile('Ecken', statistik['ecken'])}\n"
        f"🃏 {statistik_zeile('Karten', statistik['karten'])}\n"
        f"🧤 {statistik_zeile('Torwart', statistik['torwart'])}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🕐 {jetzt()} Uhr"
    )
    send_telegram(msg)
    # Tagesstatistik zurücksetzen
    for t in statistik:
        statistik[t] = {"gewonnen": 0, "verloren": 0, "gewinn": 0.0}
    print(f"  [Bericht] Tagesbericht gesendet")

def send_wochenbericht():
    gesamt_gewonnen = sum(wochen_statistik[t]["gewonnen"] for t in wochen_statistik)
    gesamt_verloren = sum(wochen_statistik[t]["verloren"] for t in wochen_statistik)
    gesamt_tipps    = gesamt_gewonnen + gesamt_verloren
    gesamt_gewinn   = round(sum(wochen_statistik[t]["gewinn"] for t in wochen_statistik), 2)
    quote_pct       = round((gesamt_gewonnen / gesamt_tipps * 100)) if gesamt_tipps else 0
    gewinn_emoji    = "📈" if gesamt_gewinn >= 0 else "📉"

    msg = (
        f"📅 <b>WOCHENBERICHT</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>Gesamt diese Woche:</b>\n"
        f"✅ Gewonnen: <b>{gesamt_gewonnen}</b>\n"
        f"❌ Verloren: <b>{gesamt_verloren}</b>\n"
        f"🎯 Trefferquote: <b>{quote_pct}%</b>\n"
        f"{gewinn_emoji} Simulation ({EINSATZ}€/Tipp): "
        f"<b>{'+' if gesamt_gewinn >= 0 else ''}{gesamt_gewinn}€</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>Nach Bot:</b>\n"
        f"⚽ {statistik_zeile('Ecken', wochen_statistik['ecken'])}\n"
        f"🃏 {statistik_zeile('Karten', wochen_statistik['karten'])}\n"
        f"🧤 {statistik_zeile('Torwart', wochen_statistik['torwart'])}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🕐 {jetzt()} Uhr"
    )
    send_telegram(msg)
    # Wochenstatistik zurücksetzen
    for t in wochen_statistik:
        wochen_statistik[t] = {"gewonnen": 0, "verloren": 0, "gewinn": 0.0}
    print(f"  [Bericht] Wochenbericht gesendet")

# ============================================================
#  AUSWERTUNG nach Spielende
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
        total_ecken = int(stats.get("corners", {}).get("home", 0)) + int(stats.get("corners", {}).get("away", 0))
        gewonnen    = total_ecken < grenze
        emoji       = "✅ GEWONNEN" if gewonnen else "❌ VERLOREN"
        update_statistik("ecken", gewonnen, quote)

        quote_zeile = f"💶 Quote: <b>{quote}</b> → Gewinn: <b>+{round((quote-1)*EINSATZ, 2)}€</b>\n" if quote and gewonnen else ""

        return (
            f"📊 <b>AUSWERTUNG – Ecken-Tipp</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📌 {home} vs {away}\n"
            f"📐 Ecken HZ1: <b>{hz1_ecken}</b>\n"
            f"🎯 Tipp: Unter <b>{grenze}</b> Ecken gesamt\n"
            f"📈 Tatsächlich: <b>{total_ecken}</b> Ecken\n"
            f"{quote_zeile}"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"{emoji}\n🕐 {jetzt()} Uhr"
        )
    except Exception as e:
        print(f"  [Auswertung] Ecken Fehler: {e}")
        return None

def auswertung_torwart(spiel):
    match_id = spiel["match_id"]
    home     = spiel["home"]
    away     = spiel["away"]
    quote    = spiel.get("quote")

    try:
        params = {**AUTH_FB, "id": match_id}
        resp   = requests.get(f"{BASE_URL_FB}/matches/single.json", params=params, timeout=10)
        resp.raise_for_status()
        match       = resp.json().get("data", {}).get("match", {})
        score       = match.get("scores", {}).get("score", "0 - 0")
        parts       = score.replace(" ", "").split("-")
        tore_gesamt = int(parts[0]) + int(parts[1]) if len(parts) == 2 else 0
        gewonnen    = tore_gesamt >= 1
        emoji       = "✅ GEWONNEN" if gewonnen else "❌ VERLOREN"
        update_statistik("torwart", gewonnen, quote)

        quote_zeile = f"💶 Quote: <b>{quote}</b> → Gewinn: <b>+{round((quote-1)*EINSATZ, 2)}€</b>\n" if quote and gewonnen else ""

        return (
            f"📊 <b>AUSWERTUNG – Torwart-Tipp</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📌 {home} vs {away}\n"
            f"🎯 Tipp: Mindestens 1 Tor fällt noch\n"
            f"📈 Endstand: <b>{score}</b> ({tore_gesamt} Tore)\n"
            f"{quote_zeile}"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"{emoji}\n🕐 {jetzt()} Uhr"
        )
    except Exception as e:
        print(f"  [Auswertung] Torwart Fehler: {e}")
        return None

def auswertung_karten(spiel):
    match_id   = spiel["match_id"]
    home       = spiel["home"]
    away       = spiel["away"]
    karten_hz1 = spiel["karten_anzahl"]
    quote      = spiel.get("quote")
    GRENZE     = 5

    try:
        events        = get_events(match_id)
        karten_gesamt = [e for e in events if e.get("event") in KARTEN_TYPEN]
        anzahl        = len(karten_gesamt)
        gewonnen      = anzahl > GRENZE
        emoji         = "✅ GEWONNEN" if gewonnen else "❌ VERLOREN"
        update_statistik("karten", gewonnen, quote)

        quote_zeile = f"💶 Quote: <b>{quote}</b> → Gewinn: <b>+{round((quote-1)*EINSATZ, 2)}€</b>\n" if quote and gewonnen else ""

        return (
            f"📊 <b>AUSWERTUNG – Karten-Tipp</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📌 {home} vs {away}\n"
            f"🃏 Karten nach 30 Min.: <b>{karten_hz1}</b>\n"
            f"🎯 Tipp: Über <b>{GRENZE}</b> Karten gesamt\n"
            f"📈 Tatsächlich: <b>{anzahl}</b> Karten\n"
            f"{quote_zeile}"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"{emoji}\n🕐 {jetzt()} Uhr"
        )
    except Exception as e:
        print(f"  [Auswertung] Karten Fehler: {e}")
        return None

# ============================================================
#  AUSWERTUNGS & BERICHT THREAD
# ============================================================

def bot_auswertung_und_berichte():
    global tagesbericht_gesendet
    print("[Auswertung-Bot] Gestartet")
    letzter_wochenbericht = de_now().isocalendar()[1]

    while True:
        try:
            now = de_now()

            # Tagesbericht um eingestellter Uhrzeit
            if now.hour == TAGESBERICHT_UHRZEIT and tagesbericht_gesendet != now.date():
                send_tagesbericht()
                tagesbericht_gesendet = now.date()

            # Wochenbericht jeden Montag
            aktuelle_woche = now.isocalendar()[1]
            if now.weekday() == 0 and aktuelle_woche != letzter_wochenbericht:
                send_wochenbericht()
                letzter_wochenbericht = aktuelle_woche

            # Spielauswertungen
            if beobachtete_spiele:
                alle     = get_live_matches()
                live_ids = {m.get("id") for m in alle}

                for match_id, spiel in list(beobachtete_spiele.items()):
                    if match_id in auswertung_done:
                        continue
                    if match_id not in live_ids:
                        time.sleep(30)
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
                            print(f"  [Auswertung] {spiel['home']} vs {spiel['away']} ({typ})")

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
                    quote      = get_quote(home, away, "ecken")
                    quote_text = f"\n💶 Quote: <b>{quote}</b>" if quote else ""
                    msg = (f"⚽ <b>Halbzeit-Tipp!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                           f"🏆 {comp} ({country})\n📌 {home} vs {away}\n"
                           f"📊 Stand: <b>{score}</b>\n"
                           f"📐 Ecken HZ1: <b>{corners}</b>\n"
                           f"🎯 Tipp: Unter <b>{grenze}</b> Ecken gesamt"
                           f"{quote_text}\n"
                           f"━━━━━━━━━━━━━━━━━━━━\n🕐 {jetzt()} Uhr")
                    send_telegram(msg)
                    notified_ecken.add(match_id)
                    beobachtete_spiele[match_id] = {
                        "typ": "ecken", "match_id": match_id,
                        "home": home, "away": away,
                        "hz1_ecken": corners, "quote": quote
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
                    quote      = get_quote(home, away, "karten")
                    quote_text = f"\n💶 Quote: <b>{quote}</b>" if quote else ""
                    zeilen = ""
                    for k in karten:
                        spieler = (k.get("player") or {}).get("name", "?")
                        team    = home if k.get("is_home") else away
                        zeilen += f"  {karten_emoji(k.get('event'))} {k.get('time')}' {spieler} ({team})\n"
                    msg = (f"🃏 <b>Karten-Alarm!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                           f"🏆 {comp} ({country})\n📌 {home} vs {away}\n"
                           f"📊 Stand: <b>{score}</b> | Minute: <b>{minute}'</b>\n"
                           f"━━━━━━━━━━━━━━━━━━━━\n<b>{len(karten)} Karten bis Min. {KARTEN_BIS_MINUTE}:</b>\n{zeilen}"
                           f"🎯 Tipp: Über <b>5</b> Karten gesamt"
                           f"{quote_text}\n"
                           f"━━━━━━━━━━━━━━━━━━━━\n🕐 {jetzt()} Uhr")
                    send_telegram(msg)
                    notified_karten.add(match_id)
                    beobachtete_spiele[match_id] = {
                        "typ": "karten", "match_id": match_id,
                        "home": home, "away": away,
                        "karten_anzahl": len(karten), "quote": quote
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
                    quote      = get_quote(home, away, "torwart")
                    quote_text = f"\n💶 Quote: <b>{quote}</b>" if quote else ""
                    msg = (f"🧤 <b>Torwart-Alarm!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                           f"🏆 {comp} ({country})\n📌 {home} vs {away}\n"
                           f"📊 Stand: <b>0:0</b> | {min_text}\n━━━━━━━━━━━━━━━━━━━━\n"
                           f"🎯 Schüsse: <b>{shots_ges}</b> ({shots_home}|{shots_away})\n"
                           f"🧤 Paraden: <b>{saves_home+saves_away}</b> ({saves_home}|{saves_away})\n"
                           f"⚽ Ballbesitz: {poss_home}%|{poss_away}%\n"
                           f"🎯 Tipp: Mindestens <b>1 Tor</b> fällt noch"
                           f"{quote_text}\n"
                           f"━━━━━━━━━━━━━━━━━━━━\n🕐 {jetzt()} Uhr")
                    send_telegram(msg)
                    notified_torwart.add(match_id)
                    beobachtete_spiele[match_id] = {
                        "typ": "torwart", "match_id": match_id,
                        "home": home, "away": away, "quote": quote
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
    print("  ⚽ FUSSBALL BOTS v5")
    print("  Ecken + Karten + Torwart")
    print("  + Auswertung + Tagesbericht + Wochenbericht")
    print("=" * 50 + "\n")

    threads = [
        threading.Thread(target=bot_ecken,                   daemon=True, name="Ecken-Bot"),
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
