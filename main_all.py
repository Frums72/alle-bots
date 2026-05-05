# v7 - Discord Embeds + Ecken fix
import requests
import re
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

DISCORD_WEBHOOK_ECKEN   = "https://discord.com/api/webhooks/1501122762096377957/OqjCXNqBBnMvaQlSz5npaYYnjbWpdh3DENhPE7aJr1ZA_WgGo0PkRRG6ZFZURi9X1CK4"
DISCORD_WEBHOOK_KARTEN  = "https://discord.com/api/webhooks/1501123056544907378/X5xjFTx81adqbY6vkigbJHqwKOSO68BXjSqTeY_WOaywGn8A4-Q9c98tkRE-d2K_8p0p"
DISCORD_WEBHOOK_TORWART = "https://discord.com/api/webhooks/1501122812700786870/3667BQTjRqVHhy_c6KJ6XmurwyOeKClHLVLhoK8-idRcAZYIVXPL9PBa-ZyXLH5j4pz5"

ODDS_API_KEY       = "866948de5d6c34ca51faf6bd77e0bb2a"  # Optional: the-odds-api.com
EINSATZ            = 10.0

MAX_CORNERS         = 5
MIN_KARTEN          = 2
KARTEN_BIS_MINUTE   = 30
MIN_SHOTS_ON_TARGET = 5
FUSSBALL_INTERVAL   = 2
TAGESBERICHT_UHRZEIT = 22
# ============================================================

BASE_URL_FB  = "https://livescore-api.com/api-client"
AUTH_FB      = {"key": API_KEY, "secret": API_SECRET}
KARTEN_TYPEN = {"YELLOW_CARD", "RED_CARD", "YELLOW_RED_CARD"}

notified_ecken   = set()
notified_karten  = set()
notified_torwart = set()
beobachtete_spiele = {}
auswertung_done    = set()

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
    return (datetime.now(timezone.utc) + timedelta(hours=2)).strftime("%H:%M")

def heute():
    return (datetime.now(timezone.utc) + timedelta(hours=2)).strftime("%d.%m.%Y")

def de_now():
    return datetime.now(timezone.utc) + timedelta(hours=2)

def html_zu_discord(text):
    """Konvertiert Telegram HTML zu Discord Markdown."""
    text = text.replace("<b>", "**").replace("</b>", "**")
    text = re.sub(r"<[^>]+>", "", text)
    return text

def send_telegram(message: str):
    url     = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    resp    = requests.post(url, json=payload, timeout=10)
    if resp.status_code != 200:
        print(f"  [Telegram Fehler] {resp.text}")

FARBE_ECKEN   = 0xF4A300  # Orange
FARBE_KARTEN  = 0xE74C3C  # Rot
FARBE_TORWART = 0x1ABC9C  # Türkis
FARBE_AUSWERTUNG_GEWONNEN = 0x2ECC71  # Grün
FARBE_AUSWERTUNG_VERLOREN = 0xE74C3C  # Rot
FARBE_BERICHT = 0x3498DB  # Blau

def send_discord_embed(webhook_url: str, embed: dict):
    """Sendet ein Discord Embed."""
    if not webhook_url or webhook_url.startswith("DISCORD"):
        return
    payload = {"embeds": [embed]}
    resp = requests.post(webhook_url, json=payload, timeout=10)
    if resp.status_code not in (200, 204):
        print(f"  [Discord Fehler] {resp.status_code}: {resp.text}")

def discord_ecken_tipp(home, away, comp, country, score, corners_home, corners_away, corners, grenze, quote):
    """Erstellt Discord Embed für Ecken-Tipp."""
    quote_text = f"\n💶 **Quote:** {quote}" if quote else ""
    return {
        "title": "📐 Ecken Tipp",
        "color": FARBE_ECKEN,
        "fields": [
            {"name": "🏆 Liga", "value": f"{comp} ({country})", "inline": True},
            {"name": "⚽ Spiel", "value": f"{home} vs {away}", "inline": True},
            {"name": "📊 Halbzeitstand", "value": f"**{score}**", "inline": True},
            {"name": "📐 Ecken zur Halbzeit",
             "value": f"🔵 {home}: **{corners_home}**\n🔴 {away}: **{corners_away}**\n📊 Gesamt: **{corners}**", "inline": False},
            {"name": "🎯 Empfehlung",
             "value": f"Unter **{grenze} Ecken** (Gesamtspiel){quote_text}", "inline": False},
        ],
        "footer": {"text": f"Fixture • {heute()} {jetzt()}"},
        "thumbnail": {"url": "https://i.imgur.com/4M34hi2.png"}
    }

def discord_karten_tipp(home, away, comp, country, score, minute, karten_liste, quote):
    """Erstellt Discord Embed für Karten-Tipp."""
    karten_text = "\n".join(karten_liste) if karten_liste else "–"
    quote_text  = f"\n💶 **Quote:** {quote}" if quote else ""
    return {
        "title": "🃏 Karten Signal",
        "color": FARBE_KARTEN,
        "fields": [
            {"name": "🏆 Liga", "value": f"{comp} ({country})", "inline": True},
            {"name": "⚽ Spiel", "value": f"{home} vs {away}", "inline": True},
            {"name": "📊 Stand", "value": f"**{score}** | Min. **{minute}'**", "inline": True},
            {"name": "🃏 Karten bis Minute 30", "value": karten_text, "inline": False},
            {"name": "🎯 Empfehlung",
             "value": f"Über **5 Karten** (Gesamtspiel){quote_text}", "inline": False},
        ],
        "footer": {"text": f"Fixture • {heute()} {jetzt()}"},
    }

def discord_torwart_tipp(home, away, comp, country, shots_home, shots_away,
                          saves_h, saves_a, poss_h, poss_a, min_text, quote):
    """Erstellt Discord Embed für Torwart-Tipp."""
    quote_text = f"\n💶 **Quote:** {quote}" if quote else ""
    return {
        "title": "🧤 Torwart Alarm",
        "color": FARBE_TORWART,
        "fields": [
            {"name": "🏆 Liga", "value": f"{comp} ({country})", "inline": True},
            {"name": "⚽ Spiel", "value": f"{home} vs {away}", "inline": True},
            {"name": "📊 Stand", "value": f"**0:0** | {min_text}", "inline": True},
            {"name": "🎯 Schüsse aufs Tor",
             "value": f"Gesamt: **{shots_home+shots_away}** ({shots_home} | {shots_away})", "inline": True},
            {"name": "🧤 Paraden",
             "value": f"Gesamt: **{saves_h+saves_a}** ({saves_h} | {saves_a})", "inline": True},
            {"name": "⚽ Ballbesitz",
             "value": f"{poss_h}% | {poss_a}%", "inline": True},
            {"name": "🎯 Empfehlung",
             "value": f"Mindestens **1 Tor** fällt noch{quote_text}", "inline": False},
        ],
        "footer": {"text": f"Fixture • {heute()} {jetzt()}"},
    }

def discord_auswertung(typ, home, away, gewonnen, details: dict):
    """Erstellt Discord Embed für Auswertung."""
    farbe  = FARBE_AUSWERTUNG_GEWONNEN if gewonnen else FARBE_AUSWERTUNG_VERLOREN
    emoji  = "✅ GEWONNEN" if gewonnen else "❌ VERLOREN"
    titel  = {"ecken": "📐 Auswertung – Eckwetten",
              "karten": "🃏 Auswertung – Karten",
              "torwart": "🧤 Auswertung – Torwart"}[typ]
    felder = [{"name": "⚽ Spiel", "value": f"{home} vs {away}", "inline": False}]
    for k, v in details.items():
        felder.append({"name": k, "value": v, "inline": True})
    felder.append({"name": "Ergebnis", "value": f"**{emoji}**", "inline": False})
    return {
        "title": titel,
        "color": farbe,
        "fields": felder,
        "footer": {"text": f"Auswertung • {heute()} {jetzt()}"},
    }

def discord_bericht(titel, felder):
    """Erstellt Discord Embed für Tages/Wochenbericht."""
    return {
        "title": titel,
        "color": FARBE_BERICHT,
        "fields": felder,
        "footer": {"text": f"Bericht • {heute()} {jetzt()}"},
    }

def send_discord(webhook_url: str, message: str):
    """Fallback: Sendet plain text (für Berichte ohne eigenes Embed)."""
    if not webhook_url or webhook_url.startswith("DISCORD"):
        return
    discord_msg = html_zu_discord(message)
    resp = requests.post(webhook_url, json={"content": discord_msg}, timeout=10)
    if resp.status_code not in (200, 204):
        print(f"  [Discord Fehler] {resp.status_code}: {resp.text}")

def send_alle(message: str, webhook: str):
    """Schickt Nachricht an Telegram UND Discord (plain text fallback)."""
    send_telegram(message)
    send_discord(webhook, message)

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

def get_quote(home, away, typ):
    if not ODDS_API_KEY:
        return None
    try:
        url    = "https://api.the-odds-api.com/v4/sports/soccer/odds/"
        params = {"apiKey": ODDS_API_KEY, "regions": "eu", "markets": "totals", "oddsFormat": "decimal"}
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
           f"⚽ {statistik_zeile('Ecken', statistik['ecken'])}\n"
           f"🃏 {statistik_zeile('Karten', statistik['karten'])}\n"
           f"🧤 {statistik_zeile('Torwart', statistik['torwart'])}\n"
           f"━━━━━━━━━━━━━━━━━━━━\n"
           f"🕐 {jetzt()} Uhr")
    send_telegram(msg)
    send_discord(DISCORD_WEBHOOK_ECKEN, msg)  # Tagesbericht in Ecken-Channel
    for t in statistik:
        statistik[t] = {"gewonnen": 0, "verloren": 0, "gewinn": 0.0}
    print(f"  [Bericht] Tagesbericht gesendet")

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
           f"⚽ {statistik_zeile('Ecken', wochen_statistik['ecken'])}\n"
           f"🃏 {statistik_zeile('Karten', wochen_statistik['karten'])}\n"
           f"🧤 {statistik_zeile('Torwart', wochen_statistik['torwart'])}\n"
           f"━━━━━━━━━━━━━━━━━━━━\n"
           f"🕐 {jetzt()} Uhr")
    send_telegram(msg)
    send_discord(DISCORD_WEBHOOK_ECKEN, msg)
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
        total_ecken = int(stats.get("corners", {}).get("home", 0)) + int(stats.get("corners", {}).get("away", 0))
        gewonnen    = total_ecken < grenze
        emoji       = "✅ GEWONNEN" if gewonnen else "❌ VERLOREN"
        update_statistik("ecken", gewonnen, quote)
        ql = f"💶 Quote: <b>{quote}</b> → Gewinn: <b>+{round((quote-1)*EINSATZ,2)}€</b>\n" if quote and gewonnen else ""
        return (f"📊 <b>Auswertung – Ecken-Tipp</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                f"📌 {home} vs {away}\n"
                f"📐 Ecken HZ1: <b>{hz1_ecken}</b>\n"
                f"🎯 Tipp: Unter <b>{grenze}</b> Ecken gesamt\n"
                f"📈 Tatsächlich: <b>{total_ecken}</b> Ecken\n{ql}"
                f"━━━━━━━━━━━━━━━━━━━━\n{emoji}\n🕐 {jetzt()} Uhr")
    except Exception as e:
        print(f"  [Auswertung] Ecken Fehler: {e}")
        return None

def auswertung_karten(spiel):
    match_id   = spiel["match_id"]
    home       = spiel["home"]
    away       = spiel["away"]
    karten_hz1 = spiel["karten_anzahl"]
    quote      = spiel.get("quote")
    GRENZE     = 5
    try:
        events   = get_events(match_id)
        anzahl   = len([e for e in events if e.get("event") in KARTEN_TYPEN])
        gewonnen = anzahl > GRENZE
        emoji    = "✅ GEWONNEN" if gewonnen else "❌ VERLOREN"
        update_statistik("karten", gewonnen, quote)
        ql = f"💶 Quote: <b>{quote}</b> → Gewinn: <b>+{round((quote-1)*EINSATZ,2)}€</b>\n" if quote and gewonnen else ""
        return (f"📊 <b>Auswertung – Karten-Tipp</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                f"📌 {home} vs {away}\n"
                f"🃏 Karten nach 30 Min.: <b>{karten_hz1}</b>\n"
                f"🎯 Tipp: Über <b>{GRENZE}</b> Karten gesamt\n"
                f"📈 Tatsächlich: <b>{anzahl}</b> Karten\n{ql}"
                f"━━━━━━━━━━━━━━━━━━━━\n{emoji}\n🕐 {jetzt()} Uhr")
    except Exception as e:
        print(f"  [Auswertung] Karten Fehler: {e}")
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
        tore        = int(parts[0]) + int(parts[1]) if len(parts) == 2 else 0
        gewonnen    = tore >= 1
        emoji       = "✅ GEWONNEN" if gewonnen else "❌ VERLOREN"
        update_statistik("torwart", gewonnen, quote)
        ql = f"💶 Quote: <b>{quote}</b> → Gewinn: <b>+{round((quote-1)*EINSATZ,2)}€</b>\n" if quote and gewonnen else ""
        return (f"📊 <b>Auswertung – Torwart-Tipp</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                f"📌 {home} vs {away}\n"
                f"🎯 Tipp: Mindestens 1 Tor fällt noch\n"
                f"📈 Endstand: <b>{score}</b> ({tore} Tore)\n{ql}"
                f"━━━━━━━━━━━━━━━━━━━━\n{emoji}\n🕐 {jetzt()} Uhr")
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
    spiel_zuletzt_live    = {}  # match_id -> timestamp wann zuletzt in Live-Liste gesehen

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

                    if match_id in live_ids:
                        # Spiel noch live – Zeitstempel aktualisieren
                        spiel_zuletzt_live[match_id] = aktuell
                        continue

                    # Spiel nicht mehr in Live-Liste
                    zuletzt      = spiel_zuletzt_live.get(match_id, 0)
                    minuten_weg  = (aktuell - zuletzt) / 60 if zuletzt > 0 else 999
                    status       = live_status.get(match_id, "")
                    beendet_status = status in ("FT", "FINISHED", "AET", "FULL TIME", "AFTER EXTRA TIME")

                    # Auswerten wenn Status FT ist ODER Spiel mind. 3 Min. weg
                    if beendet_status or minuten_weg >= 3:
                        print(f"  [Auswertung] Spiel beendet: {spiel['home']} vs {spiel['away']} (weg seit {round(minuten_weg)} Min.)")
                        time.sleep(20)
                        typ     = spiel["typ"]
                        webhook = spiel["webhook"]
                        msg     = None
                        if typ == "ecken":
                            msg = auswertung_ecken(spiel)
                        elif typ == "karten":
                            msg = auswertung_karten(spiel)
                        elif typ == "torwart":
                            msg = auswertung_torwart(spiel)
                        if msg:
                            send_telegram(msg)
                            # Discord Embed für Auswertung
                            gewonnen = "GEWONNEN" in msg
                            if typ == "ecken":
                                hz1 = spiel["hz1_ecken"]
                                grenze = hz1 * 2 + 2
                                total = re.search(r"Tatsächlich.*?(\d+)", msg)
                                total_val = total.group(1) if total else "?"
                                details = {
                                    "📐 Ecken HZ1": f"**{hz1}**",
                                    "🎯 Tipp": f"Unter **{grenze}** Ecken gesamt",
                                    "📈 Tatsächlich": f"**{total_val}** Ecken"
                                }
                            elif typ == "karten":
                                details = {
                                    "🃏 Karten bis 30'": f"**{spiel['karten_anzahl']}**",
                                    "🎯 Tipp": "Über **5** Karten gesamt",
                                    "📈 Endstand": "siehe Nachricht"
                                }
                            else:
                                score_match = re.search(r"Endstand.*?([\d]+ - [\d]+)", msg)
                                endstand = score_match.group(1) if score_match else "?"
                                details = {
                                    "🎯 Tipp": "Mindestens **1 Tor** fällt noch",
                                    "📈 Endstand": f"**{endstand}**"
                                }
                            embed = discord_auswertung(typ, spiel["home"], spiel["away"], gewonnen, details)
                            send_discord_embed(webhook, embed)
                            auswertung_done.add(match_id)
                            print(f"  [Auswertung] Gesendet: {spiel['home']} vs {spiel['away']} ({typ})")
                        else:
                            auswertung_done.add(match_id)
                            print(f"  [Auswertung] Keine Daten: {spiel['home']} vs {spiel['away']}")

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
                    quote = get_quote(home, away, "ecken")
                    ql    = f"\n💶 Quote: <b>{quote}</b>" if quote else ""
                    corners_home = int(stats.get("corners", {}).get("home", 0))
                    corners_away = int(stats.get("corners", {}).get("away", 0))
                    msg   = (f"📐 <b>Ecken Tipp!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                             f"🏆 {comp} ({country})\n📌 {home} vs {away}\n"
                             f"📊 Stand: <b>{score}</b>\n"
                             f"🔵 {home}: <b>{corners_home}</b>\n"
                             f"🔴 {away}: <b>{corners_away}</b>\n"
                             f"📊 Gesamt: <b>{corners}</b>\n"
                             f"🎯 Tipp: Unter <b>{grenze}</b> Ecken gesamt{ql}\n"
                             f"━━━━━━━━━━━━━━━━━━━━\n🕐 {jetzt()} Uhr")
                    send_telegram(msg)
                    send_discord_embed(DISCORD_WEBHOOK_ECKEN, discord_ecken_tipp(home, away, comp, country, score, corners_home, corners_away, corners, grenze, quote))
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
                    quote  = get_quote(home, away, "karten")
                    ql     = f"\n💶 Quote: <b>{quote}</b>" if quote else ""
                    zeilen = ""
                    for k in karten:
                        spieler = (k.get("player") or {}).get("name", "?")
                        team    = home if k.get("is_home") else away
                        zeilen += f"  {karten_emoji(k.get('event'))} {k.get('time')}' {spieler} ({team})\n"
                    msg = (f"🃏 <b>Karten-Alarm!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                           f"🏆 {comp} ({country})\n📌 {home} vs {away}\n"
                           f"📊 Stand: <b>{score}</b> | Minute: <b>{minute}'</b>\n"
                           f"━━━━━━━━━━━━━━━━━━━━\n<b>{len(karten)} Karten bis Min. {KARTEN_BIS_MINUTE}:</b>\n{zeilen}"
                           f"🎯 Tipp: Über <b>5</b> Karten gesamt{ql}\n"
                           f"━━━━━━━━━━━━━━━━━━━━\n🕐 {jetzt()} Uhr")
                    send_telegram(msg)
                    karten_discord = [f"{karten_emoji(k.get('event'))} {k.get('time')}' {(k.get('player') or {}).get('name', '?')} ({home if k.get('is_home') else away})" for k in karten]
                    send_discord_embed(DISCORD_WEBHOOK_KARTEN, discord_karten_tipp(home, away, comp, country, score, minute, karten_discord, quote))
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
                    send_discord_embed(DISCORD_WEBHOOK_TORWART, discord_torwart_tipp(home, away, comp, country, shots_home, shots_away, saves_home, saves_away, poss_home, poss_away, min_text, quote))
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
    print("  ⚽ FUSSBALL BOTS v6")
    print("  Telegram + Discord (3 Webhooks)")
    print("  Ecken + Karten + Torwart + Auswertung")
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
