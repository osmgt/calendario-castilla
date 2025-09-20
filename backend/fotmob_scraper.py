import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pytz
import re

# Página "Fixtures by date", más estable que /spielplan/
URL = "https://www.transfermarkt.us/real-madrid-b-castilla-/spielplandatum/verein/6767"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Referer": "https://www.transfermarkt.us/",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

TZ_MADRID = pytz.timezone("Europe/Madrid")

def _parse_score(txt: str):
    """
    Convierte '2:1' en (2,1). Si no hay marcador, devuelve (None, None).
    """
    if not txt:
        return None, None
    m = re.search(r"(\d+)\s*:\s*(\d+)", txt)
    if not m:
        return None, None
    return int(m.group(1)), int(m.group(2))

def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def scrape_matches():
    """
    Extrae partidos de la tabla 'fixtures by date'.
    Devuelve una lista de diccionarios con los campos normalizados.
    """
    resp = requests.get(URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    table = soup.select_one("table.items")
    if not table:
        # Si Transfermarkt cambia la estructura o nos bloquea, devolver vacío (no reventar backend)
        return []

    matches = []
    # La tabla tiene thead con cabeceras (Matchday, Date, Time, Venue, Opponent, Attendance, Result, ...)
    # y tbody con filas de partidos. Ignoramos filas separadoras.
    for row in table.select("tbody tr"):
        tds = row.find_all("td")
        if not tds or len(tds) < 6:
            continue

        # Campos básicos con defensiva por índices
        # Por experiencia en "spielplandatum":
        # 0=Matchday, 1=Date, 2=Time, 3=Venue(H/A y ranking), 4=Opponent, (5=System/Attendance), 6=Result (depende)
        date_txt = _clean(tds[1].get_text()) if len(tds) > 1 else ""
        time_txt = _clean(tds[2].get_text()) if len(tds) > 2 else ""
        opponent_txt = _clean(tds[4].get_text()) if len(tds) > 4 else ""
        result_txt = _clean(tds[-1].get_text())  # la última suele ser el resultado

        # Omitir filas sin fecha u oponente
        if not date_txt or not opponent_txt:
            continue

        # Determinar casa/fuera a partir de la columna de "Venue (H/A ...)"
        venue_col = _clean(tds[3].get_text()) if len(tds) > 3 else ""
        is_home = "H" in venue_col  # H (home) / A (away)

        # Parse fecha/hora en Madrid
        dt_local = None
        try:
            if time_txt and re.search(r"\d{1,2}:\d{2}", time_txt):
                # Transfermarkt en .us suele mostrar formato tipo 'Fri 8/29/25'
                # Intentamos dos patrones: 'Fri 8/29/25' o '29/08/2025'
                try:
                    dt = datetime.strptime(f"{date_txt} {time_txt}", "%a %m/%d/%y %I:%M %p")
                except Exception:
                    # fallback 24h
                    try:
                        dt = datetime.strptime(f"{date_txt} {time_txt}", "%a %m/%d/%y %H:%M")
                    except Exception:
                        # formato europeo posible
                        dt = datetime.strptime(f"{date_txt} {time_txt}", "%d/%m/%Y %H:%M")
                dt_local = TZ_MADRID.localize(dt)
            else:
                # Solo fecha
                try:
                    dt = datetime.strptime(date_txt, "%a %m/%d/%y")
                except Exception:
                    try:
                        dt = datetime.strptime(date_txt, "%d/%m/%Y")
                    except Exception:
                        # Si no parsea, salta fila
                        continue
                # asignamos 12:00 local por defecto (no inventamos marcador; solo hora neutra)
                dt = dt.replace(hour=12, minute=0)
                dt_local = TZ_MADRID.localize(dt)
        except Exception:
            continue

        home_goals, away_goals = _parse_score(result_txt)
        status = "FINISHED" if home_goals is not None and away_goals is not None else "SCHEDULED"

        # Determinar equipos: Transfermarkt muestra al rival; el club es Castilla
        castilla = "Real Madrid Castilla"
        if is_home:
            home_team = castilla
            away_team = opponent_txt
        else:
            home_team = opponent_txt
            away_team = castilla

        match = {
            "utcDate": dt_local.astimezone(pytz.UTC).isoformat(),
            "homeTeam": home_team,
            "awayTeam": away_team,
            "competition": "Primera Federación",
            "venue": "Alfredo Di Stéfano" if is_home else "Por confirmar",
            "status": status,
            "score": {
                "fullTime": {
                    "home": home_goals,
                    "away": away_goals
                }
            },
            "source": "transfermarkt-scraped"
        }

        # Clave anti-duplicados por fecha (día), equipos y competición
        key = f"{match['utcDate'][:10]}|{home_team}|{away_team}|{match['competition']}"
        if not any(
            f"{m['utcDate'][:10]}|{m['homeTeam']}|{m['awayTeam']}|{m['competition']}" == key
            for m in matches
        ):
            matches.append(match)

    return matches


# --- Compatibilidad con app.py mientras refactorizamos ---
class FotMobScraper:
    def get_matches(self):
        return scrape_matches()

    def get_team_fixtures(self, team=None, season=None):
        return scrape_matches()


if __name__ == "__main__":
    print(scrape_matches())

