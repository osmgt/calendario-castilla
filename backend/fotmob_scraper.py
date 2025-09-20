import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pytz

URL = "https://www.transfermarkt.es/real-madrid-castilla/spielplan/verein/6767"

def scrape_matches():
    response = requests.get(URL, headers={"User-Agent": "Mozilla/5.0"})
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    matches = []

    for row in soup.select("div.responsive-table table tbody tr"):
        cols = row.find_all("td")
        if not cols or len(cols) < 5:
            continue

        try:
            # Fecha y hora
            date_text = cols[0].get_text(strip=True)
            time_text = cols[1].get_text(strip=True)

            # Equipos
            home_team = cols[2].get_text(strip=True) if len(cols) > 2 else None
            away_team = cols[4].get_text(strip=True) if len(cols) > 4 else None

            # Resultado (puede estar vacío)
            result = cols[5].get_text(strip=True) if len(cols) > 5 else ""

            if not home_team or not away_team:
                continue

            # Convertir fecha/hora a UTC
            try:
                if time_text and ":" in time_text:
                    dt = datetime.strptime(f"{date_text} {time_text}", "%d/%m/%Y %H:%M")
                else:
                    dt = datetime.strptime(date_text, "%d/%m/%Y")
                utcDate = pytz.timezone("Europe/Madrid").localize(dt).astimezone(pytz.UTC)
            except Exception:
                continue

            match = {
                "utcDate": utcDate.isoformat(),
                "homeTeam": home_team,
                "awayTeam": away_team,
                "competition": "Primera Federación",
                "venue": "Por confirmar",
                "status": "FINISHED" if result else "SCHEDULED",
                "score": {
                    "fullTime": {
                        "home": None if not result else int(result.split(":")[0]),
                        "away": None if not result else int(result.split(":")[1]),
                    }
                },
                "source": "transfermarkt-scraped"
            }

            # Evitar duplicados
            key = f"{match['utcDate']}-{home_team}-{away_team}"
            if not any(m for m in matches if f"{m['utcDate']}-{m['homeTeam']}-{m['awayTeam']}" == key):
                matches.append(match)

        except Exception:
            continue

    return matches


# --- 🔹 Parche de compatibilidad con app.py ---
class FotMobScraper:
    def get_matches(self):
        """Compatibilidad con código viejo"""
        return scrape_matches()

    def get_team_fixtures(self, team=None, season=None):
        """
        Compatibilidad con app.py:
        Ignora parámetros y devuelve scrape_matches().
        """
        return scrape_matches()


if __name__ == "__main__":
    data = scrape_matches()
    print(data)
