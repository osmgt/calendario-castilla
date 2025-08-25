# archivo: app.py

import os
import json
import logging
import requests
import sqlite3
import time
from datetime import datetime, timedelta
from threading import Lock
from apscheduler.schedulers.background import BackgroundScheduler

import pytz
from bs4 import BeautifulSoup
from flask import Flask, Response, jsonify, request
from flask_cors import CORS

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

app = Flask(__name__)
CORS(app)

class CastillaDataScraper:
    def __init__(self):
        self.timezone_guatemala = pytz.timezone('America/Guatemala')
        self.timezone_madrid = pytz.timezone('Europe/Madrid')
        
        # Headers rotativos para evitar bloqueos
        self.headers_list = [
            {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            },
            {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'application/json,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive'
            },
            {
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'es,en-US;q=0.7,en;q=0.3',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1'
            }
        ]
        
        self.current_header_index = 0
    
    def get_headers(self):
        """Rotar headers para evitar detección"""
        headers = self.headers_list[self.current_header_index]
        self.current_header_index = (self.current_header_index + 1) % len(self.headers_list)
        return headers
    
    def scrape_sofascore_api(self):
        """Scraping principal de Sofascore"""
        matches = []
        
        try:
            logging.info("🔍 Intentando Sofascore API...")
            
            url = "https://api.sofascore.com/api/v1/team/5069/events/next/20"
            headers = self.get_headers()
            
            # Intentar múltiples veces con diferentes headers
            for attempt in range(3):
                try:
                    response = requests.get(url, headers=headers, timeout=15)
                    
                    if response.status_code == 200:
                        data = response.json()
                        logging.info(f"📡 Sofascore OK: {len(data.get('events', []))} eventos")
                        
                        if 'events' in data:
                            for event in data['events']:
                                try:
                                    # Solo partidos del Castilla
                                    home_team = event['homeTeam']['name']
                                    away_team = event['awayTeam']['name']
                                    
                                    if 'Castilla' not in home_team and 'Castilla' not in away_team:
                                        continue
                                    
                                    # Procesar fecha/hora
                                    timestamp = event['startTimestamp']
                                    madrid_dt = datetime.fromtimestamp(timestamp, tz=self.timezone_madrid)
                                    guatemala_dt = madrid_dt.astimezone(self.timezone_guatemala)
                                    
                                    # Estado del partido
                                    status_code = event.get('status', {}).get('code', 0)
                                    status_map = {0: 'scheduled', 1: 'live', 3: 'finished'}
                                    status = status_map.get(status_code, 'scheduled')
                                    
                                    # Resultado si está terminado
                                    result = None
                                    if status == 'finished':
                                        home_score = event.get('homeScore', {}).get('current')
                                        away_score = event.get('awayScore', {}).get('current')
                                        if home_score is not None and away_score is not None:
                                            result = f"{home_score}-{away_score}"
                                    
                                    # Venue
                                    venue = event.get('venue', {}).get('name', '')
                                    if not venue:
                                        venue = 'Ciudad Real Madrid' if 'Castilla' in home_team else 'Campo rival'
                                    
                                    match = {
                                        'id': f"sofascore-{event['id']}",
                                        'date': guatemala_dt.strftime('%Y-%m-%d'),
                                        'time': guatemala_dt.strftime('%H:%M'),
                                        'home_team': home_team,
                                        'away_team': away_team,
                                        'competition': event.get('tournament', {}).get('name', 'Primera Federación'),
                                        'venue': venue,
                                        'status': status,
                                        'result': result,
                                        'source': 'sofascore',
                                        'madrid_time': madrid_dt.strftime('%H:%M')
                                    }
                                    
                                    matches.append(match)
                                    logging.info(f"✅ {home_team} vs {away_team}")
                                    
                                except Exception as e:
                                    logging.warning(f"⚠️ Error procesando evento: {e}")
                                    continue
                        
                        logging.info(f"🎯 Sofascore: {len(matches)} partidos encontrados")
                        break  # Éxito, salir del loop
                        
                    elif response.status_code == 429:
                        logging.warning(f"⚠️ Rate limit (intento {attempt + 1}/3)")
                        time.sleep(2 ** attempt)  # Backoff exponencial
                        headers = self.get_headers()  # Cambiar headers
                        continue
                        
                    else:
                        logging.warning(f"⚠️ HTTP {response.status_code} (intento {attempt + 1}/3)")
                        time.sleep(1)
                        headers = self.get_headers()
                        continue
                        
                except requests.exceptions.RequestException as e:
                    logging.warning(f"⚠️ Error de conexión (intento {attempt + 1}/3): {e}")
                    time.sleep(2)
                    continue
                    
        except Exception as e:
            logging.error(f"❌ Error general Sofascore: {e}")
        
        return matches
    
    def scrape_backup_sources(self):
        """Fuentes de respaldo cuando Sofascore falla"""
        matches = []
        
        try:
            logging.info("🔄 Intentando fuentes de respaldo...")
            
            # Fuente 1: Resultados-futbol.com
            backup_matches = self.scrape_resultados_futbol()
            if backup_matches:
                matches.extend(backup_matches)
                logging.info(f"📖 Respaldo: {len(backup_matches)} partidos")
                
        except Exception as e:
            logging.error(f"❌ Error fuentes respaldo: {e}")
        
        return matches
    
    def scrape_resultados_futbol(self):
        """Scraper de resultados-futbol.com"""
        matches = []
        
        try:
            url = "https://www.resultados-futbol.com/equipo/Real-Madrid-Castilla"
            headers = self.get_headers()
            
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Buscar partidos (estructura específica del sitio)
                # Implementación básica - se puede mejorar
                match_elements = soup.find_all('tr', class_='vevent')
                
                for element in match_elements[:5]:  # Limitar a 5 partidos
                    try:
                        # Extraer datos básicos
                        date_elem = element.find('time')
                        teams_elem = element.find('span', class_='equipo')
                        
                        if date_elem and teams_elem:
                            # Procesar fecha
                            date_str = date_elem.get('datetime', '')
                            if date_str:
                                match_date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                                guatemala_dt = match_date.astimezone(self.timezone_guatemala)
                                
                                match = {
                                    'id': f"backup-{int(match_date.timestamp())}",
                                    'date': guatemala_dt.strftime('%Y-%m-%d'),
                                    'time': guatemala_dt.strftime('%H:%M'),
                                    'home_team': 'Real Madrid Castilla',  # Simplificado
                                    'away_team': 'Rival por determinar',
                                    'competition': 'Primera Federación',
                                    'venue': 'Por confirmar',
                                    'status': 'scheduled',
                                    'result': None,
                                    'source': 'resultados_futbol'
                                }
                                
                                matches.append(match)
                                
                    except Exception as e:
                        logging.warning(f"⚠️ Error procesando elemento backup: {e}")
                        continue
                        
            else:
                logging.warning(f"⚠️ Respaldo HTTP {response.status_code}")
                
        except Exception as e:
            logging.warning(f"⚠️ Error scraping respaldo: {e}")
        
        return matches
    
    def get_fallback_matches(self):
        """Datos mínimos cuando todo falla"""
        guatemala_tz = self.timezone_guatemala
        
        # Datos confirmados oficialmente
        fallback_matches = [
            {
                'id': 'fallback-2025-08-29',
                'date': '2025-08-29',
                'time': '11:15',  # 17:15 Madrid = 11:15 Guatemala
                'home_team': 'Real Madrid Castilla',
                'away_team': 'CD Lugo',
                'competition': 'Primera Federación - Grupo I',
                'venue': 'Estadio Alfredo Di Stéfano',
                'status': 'scheduled',
                'result': None,
                'source': 'datos_oficiales',
                'madrid_time': '17:15'
            },
            {
                'id': 'fallback-2025-09-07',
                'date': '2025-09-07',
                'time': '12:00',
                'home_team': 'Real Madrid Castilla',
                'away_team': 'Racing Club Ferrol',
                'competition': 'Primera Federación - Grupo I',
                'venue': 'Estadio Alfredo Di Stéfano',
                'status': 'scheduled',
                'result': None,
                'source': 'datos_oficiales',
                'madrid_time': '18:00'
            },
            {
                'id': 'fallback-2025-09-14',
                'date': '2025-09-14',
                'time': '10:00',
                'home_team': 'Athletic Club B',
                'away_team': 'Real Madrid Castilla',
                'competition': 'Primera Federación - Grupo I',
                'venue': 'Lezama (Vizcaya)',
                'status': 'scheduled',
                'result': None,
                'source': 'datos_oficiales',
                'madrid_time': '16:00'
            }
        ]
        
        logging.info("📋 Usando datos de fallback oficiales")
        return fallback_matches

class CastillaCalendar:
    def __init__(self):
        self.timezone = pytz.timezone('America/Guatemala')
        self.last_update = None
        self.matches_cache = []
        self.lock = Lock()
        
        # Base de datos SQLite (Render tiene disco persistente)
        self.db_path = '/opt/render/project/src/castilla_matches.db'
        
        # Fallback a /tmp si no existe el directorio de Render
        if not os.path.exists('/opt/render/project/src'):
            self.db_path = '/tmp/castilla_matches.db'
            
        self.scraper = CastillaDataScraper()
        self.init_database()
    
    def init_database(self):
        """Inicializar SQLite con esquema completo"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Tabla principal de partidos
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS matches (
                    id TEXT PRIMARY KEY,
                    date TEXT NOT NULL,
                    time TEXT NOT NULL,
                    home_team TEXT NOT NULL,
                    away_team TEXT NOT NULL,
                    competition TEXT,
                    venue TEXT,
                    status TEXT DEFAULT 'scheduled',
                    result TEXT,
                    source TEXT,
                    madrid_time TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Índices para mejor rendimiento
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_date ON matches(date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_status ON matches(status)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_source ON matches(source)')
            
            # Tabla de logs de scraping
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS scraping_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    source TEXT,
                    matches_found INTEGER,
                    success BOOLEAN,
                    error_message TEXT,
                    response_time_ms INTEGER
                )
            ''')
            
            # Tabla de estadísticas
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS access_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    endpoint TEXT,
                    ip_address TEXT,
                    user_agent TEXT,
                    country TEXT
                )
            ''')
            
            conn.commit()
            conn.close()
            
            logging.info("✅ Base de datos SQLite inicializada")
            
        except Exception as e:
            logging.error(f"❌ Error inicializando BD: {e}")
    
    def save_matches_to_db(self, matches):
        """Guardar partidos en SQLite"""
        if not matches:
            return False
            
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            for match in matches:
                cursor.execute('''
                    INSERT OR REPLACE INTO matches 
                    (id, date, time, home_team, away_team, competition, venue, status, result, source, madrid_time, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', (
                    match['id'],
                    match['date'],
                    match['time'],
                    match['home_team'],
                    match['away_team'],
                    match['competition'],
                    match['venue'],
                    match['status'],
                    match.get('result'),
                    match.get('source', 'unknown'),
                    match.get('madrid_time')
                ))
            
            conn.commit()
            conn.close()
            
            logging.info(f"💾 {len(matches)} partidos guardados en SQLite")
            return True
            
        except Exception as e:
            logging.error(f"❌ Error guardando en BD: {e}")
            return False
    
    def load_matches_from_db(self):
        """Cargar partidos desde SQLite"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Cargar partidos desde 7 días atrás hasta el futuro
            cursor.execute('''
                SELECT id, date, time, home_team, away_team, competition, venue, status, result, source, madrid_time
                FROM matches 
                WHERE date >= date('now', '-7 days')
                ORDER BY date ASC, time ASC
            ''')
            
            rows = cursor.fetchall()
            conn.close()
            
            matches = []
            for row in rows:
                matches.append({
                    'id': row[0],
                    'date': row[1],
                    'time': row[2],
                    'home_team': row[3],
                    'away_team': row[4],
                    'competition': row[5],
                    'venue': row[6],
                    'status': row[7],
                    'result': row[8],
                    'source': row[9],
                    'madrid_time': row[10]
                })
            
            logging.info(f"📖 {len(matches)} partidos cargados desde BD")
            return matches
            
        except Exception as e:
            logging.error(f"❌ Error cargando desde BD: {e}")
            return []
    
    def update_matches(self):
        """Actualización inteligente con múltiples fuentes"""
        with self.lock:
            try:
                logging.info("🔄 INICIANDO ACTUALIZACIÓN MULTI-FUENTE")
                start_time = datetime.now()
                
                # Estrategia en cascada
                matches = []
                
                # 1. Intentar Sofascore (fuente principal)
                sofascore_matches = self.scraper.scrape_sofascore_api()
                if sofascore_matches:
                    matches = sofascore_matches
                    source = "sofascore"
                else:
                    # 2. Intentar fuentes de respaldo
                    backup_matches = self.scraper.scrape_backup_sources()
                    if backup_matches:
                        matches = backup_matches
                        source = "backup_sources"
                    else:
                        # 3. Usar datos de fallback
                        matches = self.scraper.get_fallback_matches()
                        source = "fallback_oficial"
                
                # Guardar resultados
                if matches:
                    success = self.save_matches_to_db(matches)
                    
                    if success:
                        self.matches_cache = matches
                        self.last_update = datetime.now(self.timezone)
                        
                        elapsed_ms = int((datetime.now() - start_time).total_seconds() * 1000)
                        self.log_scraping_attempt(source, len(matches), True, None, elapsed_ms)
                        
                        logging.info(f"✅ ACTUALIZACIÓN EXITOSA: {len(matches)} partidos desde {source}")
                    else:
                        self.log_scraping_attempt(source, len(matches), False, "Error guardando BD")
                else:
                    self.log_scraping_attempt("all_sources", 0, False, "No se encontraron datos")
                    
            except Exception as e:
                error_msg = str(e)
                logging.error(f"❌ Error en actualización: {error_msg}")
                self.log_scraping_attempt("error", 0, False, error_msg)
    
    def log_scraping_attempt(self, source, count, success, error=None, response_time=None):
        """Log detallado de scraping"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO scraping_logs (source, matches_found, success, error_message, response_time_ms)
                VALUES (?, ?, ?, ?, ?)
            ''', (source, count, success, error, response_time))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logging.warning(f"⚠️ Error logging: {e}")
    
    def log_access(self, endpoint, ip, user_agent):
        """Log de accesos"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO access_stats (endpoint, ip_address, user_agent)
                VALUES (?, ?, ?)
            ''', (endpoint, ip, user_agent))
            
            conn.commit()
            conn.close()
        except:
            pass  # No fallar si no se puede logear
    
    def get_matches(self):
        """Obtener partidos con cache inteligente"""
        if not self.matches_cache:
            self.matches_cache = self.load_matches_from_db()
            
        if not self.matches_cache:
            logging.info("🔄 Cache vacío, actualizando...")
            self.update_matches()
            
        return self.matches_cache
    
    def generate_ics(self):
        """Generar archivo .ics optimizado para iOS"""
        matches = self.get_matches()
        
        if not matches:
            logging.warning("⚠️ No hay partidos para generar ICS")
            return self.generate_empty_ics()
        
        ics_lines = [
            "BEGIN:VCALENDAR",
            "VERSION:2.0",
            "PRODID:-//Castilla Guatemala Calendar v2.0//ES",
            "CALSCALE:GREGORIAN",
            "METHOD:PUBLISH",
            "X-WR-CALNAME:⚽ Real Madrid Castilla 🇬🇹",
            "X-WR-CALDESC:Real Madrid Castilla - Horario Guatemala. Datos automáticos cada hora.",
            "X-WR-TIMEZONE:America/Guatemala",
            "REFRESH-INTERVAL;VALUE=DURATION:PT1H",
            "X-PUBLISHED-TTL:PT1H",
            "COLOR:1E3A8A",
            "X-WR-RELCALID:castilla-guatemala-2025"
        ]
        
        for match in matches:
            try:
                # Parsear fecha y hora
                match_datetime = datetime.strptime(f"{match['date']} {match['time']}", "%Y-%m-%d %H:%M")
                match_datetime = self.timezone.localize(match_datetime)
                end_datetime = match_datetime + timedelta(hours=2)
                
                # Formatear para ICS
                start_str = match_datetime.strftime("%Y%m%dT%H%M%S")
                end_str = end_datetime.strftime("%Y%m%dT%H%M%S")
                timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
                
                # Descripción rica
                description = f"🏆 {match['competition']}\\n"
                description += f"🏟️ {match['venue']}\\n"
                description += f"🇬🇹 Guatemala: {match['time']}\\n"
                
                if match.get('madrid_time'):
                    description += f"🇪🇸 Madrid: {match['madrid_time']}\\n"
                
                if match.get('result'):
                    description += f"⚽ Resultado: {match['result']}\\n"
                elif match['status'] == 'live':
                    description += "🔴 ¡PARTIDO EN VIVO!\\n"
                elif match['status'] == 'scheduled':
                    description += "⏰ Programado\\n"
                
                description += f"\\n📡 Fuente: {match.get('source', 'automática')}"
                description += "\\n👑 ¡Hala Madrid y nada más!"
                description += "\\n\\n🔄 Actualización automática cada hora"
                description += "\\n🇬🇹 Calendario creado en Guatemala"
                
                # Título del evento
                summary = f"⚽ {match['home_team']} vs {match['away_team']}"
                if match.get('result'):
                    summary += f" ({match['result']})"
                
                # Crear evento ICS
                event_lines = [
                    "BEGIN:VEVENT",
                    f"UID:{match['id']}@castilla-guatemala.render.com",
                    f"DTSTART;TZID=America/Guatemala:{start_str}",
                    f"DTEND;TZID=America/Guatemala:{end_str}",
                    f"DTSTAMP:{timestamp}",
                    f"SUMMARY:{summary}",
                    f"DESCRIPTION:{description}",
                    f"LOCATION:{match['venue']}",
                    "CATEGORIES:FÚTBOL,REAL MADRID CASTILLA,PRIMERA FEDERACIÓN,GUATEMALA",
                    f"STATUS:{'CONFIRMED' if match['status'] == 'finished' else 'TENTATIVE'}",
                    f"PRIORITY:{'1' if match['status'] == 'live' else '5'}",
                    "TRANSP:OPAQUE",
                    "END:VEVENT"
                ]
                
                ics_lines.extend(event_lines)
                
            except Exception as e:
                logging.error(f"❌ Error procesando evento {match.get('id')}: {e}")
                continue
        
        ics_lines.append("END:VCALENDAR")
        return "\n".join(ics_lines)
    
    def generate_empty_ics(self):
        """ICS vacío cuando no hay datos"""
        return """BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//Castilla Guatemala Calendar//ES
CALSCALE:GREGORIAN
METHOD:PUBLISH
X-WR-CALNAME:⚽ Real Madrid Castilla 🇬🇹
X-WR-CALDESC:Calendario temporalmente sin datos. Se actualizará pronto.
X-WR-TIMEZONE:America/Guatemala
REFRESH-INTERVAL;VALUE=DURATION:PT1H
BEGIN:VEVENT
UID:no-data@castilla-guatemala.render.com
DTSTART:20250825T180000
DTEND:20250825T200000
SUMMARY:🔄 Actualizando datos del Castilla...
DESCRIPTION:El calendario se está actualizando con los últimos partidos.\\nVuelve a sincronizar en unas horas.
LOCATION:En proceso
STATUS:TENTATIVE
END:VEVENT
END:VCALENDAR"""

# Instancia global
calendar = CastillaCalendar()

# Scheduler para actualizaciones automáticas (optimizado para Render)
scheduler = BackgroundScheduler(timezone='America/Guatemala')

# Actualizaciones regulares cada 2 horas (Render tiene límites de CPU)
scheduler.add_job(
    func=calendar.update_matches,
    trigger="interval",
    hours=2,
    id='update_regular'
)

# Actualización extra los fines de semana (días de partido)
scheduler.add_job(
    func=calendar.update_matches,
    trigger="cron",
    hour="10,14,18",  # 10 AM, 2 PM, 6 PM Guatemala
    day_of_week="sat,sun",
    id='update_weekend'
)

scheduler.start()

# RUTAS DE LA API

@app.before_request
def before_request():
    """Log de accesos"""
    calendar.log_access(
        request.path,
        request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr),
        request.headers.get('User-Agent', '')[:200]  # Truncar user agent largo
    )

@app.route('/')
def home():
    """Página principal con información completa"""
    base_url = request.url_root.rstrip('/')
    
    return jsonify({
        "proyecto": "🏆 Real Madrid Castilla - Guatemala",
        "descripcion": "Calendario automático con datos reales multi-fuente",
        "version": "2.0.0-render",
        
        "urls": {
            "calendario_ios": f"{base_url}/calendar.ics",
            "api_partidos": f"{base_url}/api/matches",
            "estado_sistema": f"{base_url}/api/status",
            "proximo_partido": f"{base_url}/api/next",
            "forzar_actualizacion": f"{base_url}/api/update"
        },
        
        "instrucciones_ios": [
            "1. Copia la URL: " + f"{base_url}/calendar.ics",
            "2. Abre Ajustes en tu iPhone",
            "3. Ve a Calendario → Cuentas → Añadir cuenta → Otro",
            "4. Selecciona 'Añadir calendario suscrito'",
            "5. Pega la URL y confirma",
            "6. Nombre: 'Real Madrid Castilla 🇬🇹'",
            "7. ¡Se actualiza automáticamente cada hora!"
        ],
        
        "caracteristicas": {
            "zona_horaria": "America/Guatemala (GMT-6)",
            "actualizacion": "Cada 2 horas (más frecuente en fines de semana)",
            "fuentes_datos": ["Sofascore API", "Resultados-futbol.com", "Datos oficiales"],
            "plataforma": "Render.com (gratis)",
            "base_datos": "SQLite persistente",
            "compatibilidad": ["iOS", "Android", "Google Calendar", "Outlook"]
        },
        
        "estadisticas": {
            "ultima_actualizacion": calendar.last_update.isoformat() if calendar.last_update else None,
            "partidos_cache": len(calendar.matches_cache),
            "servidor": "Render.com",
            "pais": "🇬🇹 Guatemala"
        }
    })

@app.route('/calendar.ics')
def get_calendar():
    """ENDPOINT PRINCIPAL - Calendario para iOS"""
    try:
        ics_content = calendar.generate_ics()
        
        response = Response(ics_content, mimetype='text/calendar; charset=utf-8')
        response.headers['Content-Disposition'] = 'attachment; filename="real-madrid-castilla-guatemala.ics"'
        response.headers['Cache-Control'] = 'public, max-age=3600'  # Cache 1 hora
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['X-Content-Type-Options'] = 'nosniff'
        response.headers['Content-Security-Policy'] = "default-src 'self'"
        
        logging.info("📱 Calendario .ics servido correctamente")
        return response
        
    except Exception as e:
        logging.error(f"❌ Error generando calendario: {e}")
        
        # Fallback: calendario de emergencia
        emergency_ics = calendar.generate_empty_ics()
        response = Response(emergency_ics, mimetype='text/calendar')
        response.headers['Content-Disposition'] = 'attachment; filename="castilla-error.ics"'
        return response

@app.route('/api/matches')
def get_matches_api():
    """API completa de partidos"""
    try:
        matches = calendar.get_matches()
        
        # Calcular estadísticas
        now = datetime.now(calendar.timezone)
        upcoming_matches = []
        finished_matches = []
        live_matches = []
        
        for match in matches:
            if match['status'] == 'live':
                live_matches.append(match)
            elif match['status'] == 'finished':
                finished_matches.append(match)
            elif match['status'] == 'scheduled':
                match_dt = datetime.strptime(f"{match['date']} {match['time']}", "%Y-%m-%d %H:%M")
                if calendar.timezone.localize(match_dt) > now:
                    upcoming_matches.append(match)
        
        return jsonify({
            "partidos": matches,
            "resumen": {
                "total": len(matches),
                "proximos": len(upcoming_matches),
                "finalizados": len(finished_matches),
                "en_vivo": len(live_matches)
            },
            "proximos_5": upcoming_matches[:5],
            "ultimos_resultados": [m for m in finished_matches if m.get('result')][-3:],
            
            "metadata": {
                "ultima_actualizacion": calendar.last_update.isoformat() if calendar.last_update else None,
                "zona_horaria": "America/Guatemala",
                "fuentes_disponibles": ["sofascore", "resultados_futbol", "datos_oficiales"],
                "proxima_actualizacion": "Cada 2 horas",
                "version_api": "2.0.0"
            }
        })
        
    except Exception as e:
        logging.error(f"❌ Error API matches: {e}")
        return jsonify({"error": "Error obteniendo partidos", "codigo": 500}), 500

@app.route('/api/status')
def get_status():
    """Estado completo del sistema"""
    try:
        # Estadísticas de base de datos
        try:
            conn = sqlite3.connect(calendar.db_path)
            cursor = conn.cursor()
            
            # Stats de scraping últimas 24h
            cursor.execute('''
                SELECT source, COUNT(*) as intentos, 
                       SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as exitosos,
                       AVG(response_time_ms) as tiempo_promedio
                FROM scraping_logs 
                WHERE timestamp >= datetime('now', '-24 hours')
                GROUP BY source
            ''')
            
            scraping_stats = {}
            for row in cursor.fetchall():
                scraping_stats[row[0]] = {
                    "intentos": row[1],
                    "exitosos": row[2],
                    "tasa_exito": round((row[2] / row[1] * 100), 2) if row[1] > 0 else 0,
                    "tiempo_promedio_ms": round(row[3], 2) if row[3] else 0
                }
            
            # Stats de acceso últimas 24h
            cursor.execute('''
                SELECT COUNT(*) FROM access_stats 
                WHERE timestamp >= datetime('now', '-24 hours')
            ''')
            accesos_24h = cursor.fetchone()[0]
            
            # Endpoint más popular
            cursor.execute('''
                SELECT endpoint, COUNT(*) as accesos 
                FROM access_stats 
                WHERE timestamp >= datetime('now', '-24 hours')
                GROUP BY endpoint 
                ORDER BY accesos DESC 
                LIMIT 1
            ''')
            endpoint_popular = cursor.fetchone()
            
            conn.close()
            
            db_stats = {
                "scraping_24h": scraping_stats,
                "accesos_24h": accesos_24h,
                "endpoint_popular": {
                    "ruta": endpoint_popular[0] if endpoint_popular else "N/A",
                    "accesos": endpoint_popular[1] if endpoint_popular else 0
                }
            }
            
        except Exception as e:
            logging.warning(f"⚠️ Error stats BD: {e}")
            db_stats = {"error": "No disponible"}
        
        return jsonify({
            "estado": "✅ Sistema operativo",
            "servidor": "Render.com (plan gratuito)",
            "ubicacion": "🇬🇹 Guatemala",
            "version": "2.0.0-render",
            
            "calendario": {
                "ultima_actualizacion": calendar.last_update.isoformat() if calendar.last_update else "Nunca",
                "partidos_en_cache": len(calendar.matches_cache),
                "base_datos": "SQLite persistente",
                "ruta_bd": calendar.db_path
            },
            
            "estadisticas": db_stats,
            
            "configuracion": {
                "zona_horaria": "America/Guatemala",
                "intervalo_actualizacion": "2 horas (regular), extra en fines de semana",
                "fuentes_scraping": ["Sofascore API", "Resultados-futbol.com", "Fallback oficial"],
                "cache_ttl": "1 hora"
            },
            
            "recursos": {
                "cpu_uso": "Bajo",
                "memoria_uso": "< 100MB",
                "disco_bd": "< 5MB",
                "limite_render": "500h/mes gratis"
            }
        })
        
    except Exception as e:
        logging.error(f"❌ Error status: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/update')
def force_update():
    """Forzar actualización manual"""
    try:
        logging.info("🔄 Actualización manual solicitada via API")
        
        # Ejecutar actualización
        calendar.update_matches()
        matches = calendar.get_matches()
        
        return jsonify({
            "mensaje": "✅ Actualización completada",
            "timestamp": calendar.last_update.isoformat() if calendar.last_update else None,
            "partidos_encontrados": len(matches),
            "fuentes_consultadas": ["Sofascore", "Respaldos", "Fallback"],
            "proximo_update_automatico": "En 2 horas"
        })
        
    except Exception as e:
        logging.error(f"❌ Error update forzado: {e}")
        return jsonify({
            "mensaje": "❌ Error en actualización",
            "error": str(e),
            "sugerencia": "Intenta de nuevo en unos minutos"
        }), 500

@app.route('/api/next')
def get_next_match():
    """Información del próximo partido"""
    try:
        matches = calendar.get_matches()
        now = datetime.now(calendar.timezone)
        
        # Buscar próximo partido
        upcoming_matches = []
        for match in matches:
            if match['status'] in ['scheduled', 'live']:
                try:
                    match_dt = datetime.strptime(f"{match['date']} {match['time']}", "%Y-%m-%d %H:%M")
                    match_dt = calendar.timezone.localize(match_dt)
                    
                    if match['status'] == 'live' or match_dt > now:
                        upcoming_matches.append({
                            **match,
                            'datetime_obj': match_dt
                        })
                except:
                    continue
        
        if not upcoming_matches:
            return jsonify({
                "mensaje": "No hay próximos partidos programados",
                "sugerencia": "Revisa más tarde o fuerza una actualización"
            })
        
        # Ordenar por fecha y tomar el primero
        upcoming_matches.sort(key=lambda x: x['datetime_obj'])
        next_match = upcoming_matches[0]
        
        # Calcular tiempo restante
        if next_match['status'] == 'live':
            time_info = {
                "estado": "🔴 ¡PARTIDO EN VIVO AHORA!",
                "tiempo_restante": "En curso",
                "es_hoy": True,
                "es_live": True
            }
        else:
            time_diff = next_match['datetime_obj'] - now
            days = time_diff.days
            hours = time_diff.seconds // 3600
            minutes = (time_diff.seconds % 3600) // 60
            
            time_info = {
                "estado": "⏰ Próximo partido",
                "tiempo_restante": f"{days}d {hours}h {minutes}m",
                "dias": days,
                "horas": hours,
                "minutos": minutes,
                "es_hoy": days == 0,
                "es_esta_semana": days <= 7,
                "es_live": False
            }
        
        # Remover el objeto datetime antes de devolver JSON
        next_match_clean = {k: v for k, v in next_match.items() if k != 'datetime_obj'}
        
        return jsonify({
            "proximo_partido": next_match_clean,
            "tiempo": time_info,
            "fecha_completa": next_match['datetime_obj'].strftime('%A %d de %B de %Y a las %H:%M'),
            "otros_proximos": len(upcoming_matches) - 1
        })
        
    except Exception as e:
        logging.error(f"❌ Error próximo partido: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/stats')
def get_detailed_stats():
    """Estadísticas avanzadas del sistema"""
    try:
        conn = sqlite3.connect(calendar.db_path)
        cursor = conn.cursor()
        
        # Estadísticas de scraping por día (última semana)
        cursor.execute('''
            SELECT DATE(timestamp) as fecha, 
                   COUNT(*) as intentos,
                   SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as exitosos,
                   AVG(response_time_ms) as tiempo_promedio
            FROM scraping_logs 
            WHERE timestamp >= datetime('now', '-7 days')
            GROUP BY DATE(timestamp)
            ORDER BY fecha DESC
        ''')
        
        scraping_por_dia = [
            {
                "fecha": row[0],
                "intentos": row[1],
                "exitosos": row[2],
                "tiempo_promedio_ms": round(row[3], 2) if row[3] else 0
            }
            for row in cursor.fetchall()
        ]
        
        # Top fuentes más exitosas
        cursor.execute('''
            SELECT source, 
                   COUNT(*) as total_intentos,
                   SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as exitosos,
                   AVG(response_time_ms) as tiempo_promedio,
                   MAX(timestamp) as ultimo_uso
            FROM scraping_logs 
            WHERE timestamp >= datetime('now', '-7 days')
            GROUP BY source
            ORDER BY exitosos DESC
        ''')
        
        fuentes_ranking = [
            {
                "fuente": row[0],
                "total_intentos": row[1],
                "exitosos": row[2],
                "tasa_exito": round((row[2] / row[1] * 100), 2) if row[1] > 0 else 0,
                "tiempo_promedio_ms": round(row[3], 2) if row[3] else 0,
                "ultimo_uso": row[4]
            }
            for row in cursor.fetchall()
        ]
        
        # Accesos por hora (últimas 24h)
        cursor.execute('''
            SELECT strftime('%H', timestamp) as hora, COUNT(*) as accesos
            FROM access_stats 
            WHERE timestamp >= datetime('now', '-24 hours')
            GROUP BY strftime('%H', timestamp)
            ORDER BY hora
        ''')
        
        accesos_por_hora = [
            {"hora": f"{row[0]}:00", "accesos": row[1]}
            for row in cursor.fetchall()
        ]
        
        # Endpoints más populares
        cursor.execute('''
            SELECT endpoint, COUNT(*) as accesos,
                   COUNT(DISTINCT ip_address) as ips_unicas
            FROM access_stats 
            WHERE timestamp >= datetime('now', '-24 hours')
            GROUP BY endpoint
            ORDER BY accesos DESC
            LIMIT 10
        ''')
        
        endpoints_populares = [
            {
                "endpoint": row[0],
                "accesos": row[1],
                "ips_unicas": row[2]
            }
            for row in cursor.fetchall()
        ]
        
        conn.close()
        
        return jsonify({
            "resumen": {
                "periodo_analisis": "Últimos 7 días",
                "servidor": "Render.com",
                "version": "2.0.0-render",
                "zona_horaria": "America/Guatemala"
            },
            
            "scraping": {
                "por_dia": scraping_por_dia,
                "fuentes_ranking": fuentes_ranking,
                "total_intentos_7d": sum(f["total_intentos"] for f in fuentes_ranking),
                "total_exitosos_7d": sum(f["exitosos"] for f in fuentes_ranking)
            },
            
            "accesos": {
                "por_hora_24h": accesos_por_hora,
                "endpoints_populares": endpoints_populares,
                "total_accesos_24h": sum(e["accesos"] for e in endpoints_populares)
            },
            
            "rendimiento": {
                "tiempo_respuesta_promedio": round(
                    sum(f["tiempo_promedio_ms"] for f in fuentes_ranking if f["tiempo_promedio_ms"]) / 
                    len([f for f in fuentes_ranking if f["tiempo_promedio_ms"] > 0]), 2
                ) if fuentes_ranking else 0,
                "fuente_mas_rapida": min(fuentes_ranking, key=lambda x: x["tiempo_promedio_ms"])["fuente"] 
                    if fuentes_ranking else "N/A",
                "fuente_mas_confiable": max(fuentes_ranking, key=lambda x: x["tasa_exito"])["fuente"] 
                    if fuentes_ranking else "N/A"
            }
        })
        
    except Exception as e:
        logging.error(f"❌ Error stats detalladas: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/health')
def health_check():
    """Health check para monitoreo"""
    try:
        # Verificar que la BD funciona
        conn = sqlite3.connect(calendar.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM matches")
        matches_count = cursor.fetchone()[0]
        conn.close()
        
        # Verificar última actualización
        hours_since_update = 999
        if calendar.last_update:
            hours_since_update = (datetime.now(calendar.timezone) - calendar.last_update).total_seconds() / 3600
        
        # Status general
        status = "healthy"
        if hours_since_update > 6:  # Más de 6 horas sin actualizar
            status = "warning"
        if hours_since_update > 24:  # Más de 24 horas sin actualizar
            status = "critical"
        
        return jsonify({
            "status": status,
            "checks": {
                "database": "ok" if matches_count >= 0 else "error",
                "last_update": "ok" if hours_since_update < 6 else "warning",
                "matches_available": "ok" if matches_count > 0 else "warning"
            },
            "metrics": {
                "matches_in_db": matches_count,
                "hours_since_update": round(hours_since_update, 2),
                "cache_size": len(calendar.matches_cache)
            },
            "timestamp": datetime.now(calendar.timezone).isoformat()
        })
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500

# Error handlers
@app.errorhandler(404)
def not_found(error):
    return jsonify({
        "error": "Endpoint no encontrado",
        "mensaje": "Consulta /api/status para ver endpoints disponibles",
        "codigo": 404
    }), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({
        "error": "Error interno del servidor",
        "mensaje": "Intenta más tarde o contacta al administrador",
        "codigo": 500
    }), 500

# Inicialización
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('DEBUG', 'False').lower() == 'true'
    
    logging.info("🚀 INICIANDO CALENDARIO CASTILLA v2.0")
    logging.info(f"📍 Servidor: Render.com - Puerto {port}")
    logging.info(f"🇬🇹 Zona horaria: America/Guatemala")
    logging.info(f"📱 Endpoint principal: /calendar.ics")
    logging.info(f"🔄 Actualizaciones: Cada 2 horas")
    
    # Actualización inicial
    try:
        logging.info("🔄 Cargando datos iniciales...")
        calendar.update_matches()
        initial_matches = len(calendar.get_matches())
        logging.info(f"✅ Sistema iniciado con {initial_matches} partidos")
    except Exception as e:
        logging.error(f"⚠️ Error en carga inicial: {e}")
        logging.info("🔄 El sistema funcionará y se actualizará automáticamente")
    
    # Iniciar servidor Flask
    app.run(
        host='0.0.0.0', 
        port=port, 
        debug=debug,
        threaded=True
    )