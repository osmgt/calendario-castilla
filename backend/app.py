# archivo: backend/app.py

from flask import Flask, Response, jsonify
from flask_cors import CORS
import requests
from datetime import datetime, timedelta
import pytz
import re
from bs4 import BeautifulSoup
import json
import os
from urllib.parse import urljoin

app = Flask(__name__)
CORS(app)

# Configuración
CACHE_DURATION = 3600  # 1 hora en segundos
cached_data = {}
last_update = None

class CastillaCalendar:
    def __init__(self):
        self.base_urls = [
            "https://www.realmadrid.com/futbol/castilla/partidos",
            "https://www.laliga.com/primera-rfef/clasificacion"
        ]
        self.matches_cache = []
        self.timezone = pytz.timezone('Europe/Madrid')
    
    def scrape_official_data(self):
        """Intenta obtener datos de fuentes oficiales"""
        matches = []
        
        try:
            # Datos de ejemplo - en producción aquí harías web scraping real
            sample_matches = [
                {
                    'id': 'castilla-2024-08-25-1',
                    'date': '2024-08-25',
                    'time': '18:00',
                    'home_team': 'Real Madrid Castilla',
                    'away_team': 'Atlético Baleares',
                    'competition': 'Primera RFEF - Grupo 1',
                    'venue': 'Ciudad Real Madrid',
                    'status': 'scheduled',
                    'result': None,
                    'matchday': 1
                },
                {
                    'id': 'castilla-2024-09-01-2',
                    'date': '2024-09-01',
                    'time': '16:00',
                    'home_team': 'Cultural Leonesa',
                    'away_team': 'Real Madrid Castilla',
                    'competition': 'Primera RFEF - Grupo 1',
                    'venue': 'Reino de León',
                    'status': 'scheduled',
                    'result': None,
                    'matchday': 2
                },
                {
                    'id': 'castilla-2024-09-08-3',
                    'date': '2024-09-08',
                    'time': '17:30',
                    'home_team': 'Real Madrid Castilla',
                    'away_team': 'CD Numancia',
                    'competition': 'Primera RFEF - Grupo 1',
                    'venue': 'Ciudad Real Madrid',
                    'status': 'scheduled',
                    'result': None,
                    'matchday': 3
                },
                {
                    'id': 'castilla-2024-08-18-0',
                    'date': '2024-08-18',
                    'time': '19:30',
                    'home_team': 'Real Madrid Castilla',
                    'away_team': 'Pontevedra CF',
                    'competition': 'Primera RFEF - Grupo 1',
                    'venue': 'Ciudad Real Madrid',
                    'status': 'finished',
                    'result': '2-1',
                    'matchday': 0
                }
            ]
            
            matches.extend(sample_matches)
            
        except Exception as e:
            print(f"Error scraping data: {e}")
            
        return matches
    
    def get_updated_matches(self):
        """Obtiene los partidos más actualizados"""
        global cached_data, last_update
        
        now = datetime.now()
        
        # Si no hay cache o está vencido, actualizar
        if not last_update or (now - last_update).seconds > CACHE_DURATION:
            print("Actualizando datos de partidos...")
            self.matches_cache = self.scrape_official_data()
            cached_data = self.matches_cache
            last_update = now
        
        return cached_data
    
    def generate_ics_content(self, matches):
        """Genera el contenido del archivo .ics"""
        
        ics_content = """BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//Real Madrid Castilla Calendar//ES
CALSCALE:GREGORIAN
METHOD:PUBLISH
X-WR-CALNAME:Real Madrid Castilla
X-WR-CALDESC:Calendario oficial partidos Real Madrid Castilla - Actualización automática
X-WR-TIMEZONE:Europe/Madrid
REFRESH-INTERVAL;VALUE=DURATION:PT1H
X-PUBLISHED-TTL:PT1H
COLOR:1E3A8A
"""
        
        for match in matches:
            try:
                # Crear fecha y hora del partido
                match_datetime = datetime.strptime(f"{match['date']} {match['time']}", "%Y-%m-%d %H:%M")
                match_datetime = self.timezone.localize(match_datetime)
                
                # Duración del partido (2 horas)
                end_datetime = match_datetime + timedelta(hours=2)
                
                # Formatear fechas para ICS
                start_dt = match_datetime.strftime("%Y%m%dT%H%M%S")
                end_dt = end_datetime.strftime("%Y%m%dT%H%M%S")
                
                # Crear descripción detallada
                description = f"🏆 {match['competition']}\\n"
                description += f"🏟️ {match['venue']}\\n"
                description += f"📅 Jornada {match.get('matchday', 'N/A')}\\n"
                
                if match['result']:
                    description += f"⚽ Resultado: {match['result']}\\n"
                
                if match['status'] == 'scheduled':
                    description += "🔄 Horario por confirmar\\n"
                elif match['status'] == 'live':
                    description += "🔴 ¡PARTIDO EN VIVO!\\n"
                
                description += "\\n👑 ¡Hala Madrid!"
                
                # Crear evento ICS
                ics_content += f"""
BEGIN:VEVENT
UID:{match['id']}@realmadrid-castilla.com
DTSTART;TZID=Europe/Madrid:{start_dt}
DTEND;TZID=Europe/Madrid:{end_dt}
DTSTAMP:{datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")}
SUMMARY:⚽ {match['home_team']} vs {match['away_team']}
DESCRIPTION:{description}
LOCATION:{match['venue']}
STATUS:{"CONFIRMED" if match['status'] == 'finished' else "TENTATIVE"}
CATEGORIES:FÚTBOL,REAL MADRID CASTILLA,{match['competition'].upper()}
PRIORITY:5
TRANSP:OPAQUE
END:VEVENT"""
                
            except Exception as e:
                print(f"Error procesando partido {match.get('id', 'unknown')}: {e}")
                continue
        
        ics_content += "\nEND:VCALENDAR"
        return ics_content

# Crear instancia del calendario
calendar_generator = CastillaCalendar()

@app.route('/')
def home():
    return jsonify({
        "message": "🏆 Real Madrid Castilla Calendar API",
        "endpoints": {
            "calendar": "/calendar.ics",
            "matches": "/api/matches",
            "status": "/api/status"
        },
        "last_update": last_update.isoformat() if last_update else None
    })

@app.route('/calendar.ics')
def generate_calendar():
    """Endpoint principal para el calendario ICS"""
    try:
        matches = calendar_generator.get_updated_matches()
        ics_content = calendar_generator.generate_ics_content(matches)
        
        response = Response(ics_content, mimetype='text/calendar')
        response.headers["Content-Disposition"] = "attachment; filename=real-madrid-castilla.ics"
        response.headers["Cache-Control"] = "public, max-age=3600"  # Cache 1 hora
        
        return response
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/matches')
def get_matches():
    """API para obtener partidos en formato JSON"""
    try:
        matches = calendar_generator.get_updated_matches()
        return jsonify({
            "matches": matches,
            "last_update": last_update.isoformat() if last_update else None,
            "total_matches": len(matches)
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/status')
def get_status():
    """Estado del sistema"""
    return jsonify({
        "status": "online",
        "last_update": last_update.isoformat() if last_update else None,
        "next_update": (last_update + timedelta(seconds=CACHE_DURATION)).isoformat() if last_update else None,
        "cache_duration_minutes": CACHE_DURATION // 60
    })

@app.route('/api/force-update')
def force_update():
    """Forzar actualización de datos"""
    global last_update
    last_update = None  # Resetear cache
    matches = calendar_generator.get_updated_matches()
    
    return jsonify({
        "message": "✅ Datos actualizados correctamente",
        "matches_count": len(matches),
        "update_time": last_update.isoformat()
    })

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)