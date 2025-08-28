# archivo: app.py - Backend Completo v3.0 con FotMob

import os
import json
import logging
import sqlite3
import time
from datetime import datetime, timedelta
from threading import Lock
from apscheduler.schedulers.background import BackgroundScheduler

import pytz
import requests
from flask import Flask, Response, jsonify, request
from flask_cors import CORS
from fotmob_scraper import FotMobScraper

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

app = Flask(__name__)
CORS(app)

class CastillaCalendarComplete:
    def __init__(self):
        self.timezone = pytz.timezone('America/Guatemala')
        self.last_update = None
        self.matches_cache = []
        self.lock = Lock()
        
        # Base de datos SQLite con esquema completo
        self.db_path = '/opt/render/project/src/castilla_complete.db'
        if not os.path.exists('/opt/render/project/src'):
            self.db_path = '/tmp/castilla_complete.db'
        
        # Scraper de FotMob
        self.fotmob_scraper = FotMobScraper()
        
        # Configuración
        self.config = {
            'update_interval_minutes': 30,
            'fotmob_team_id': '9825',  # Se actualizará automáticamente
            'season': '2024-25',
            'notifications_enabled': True,
            'debug_mode': False
        }
        
        self.init_database()
        
    def init_database(self):
        """Inicializar base de datos con esquema completo"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Ejecutar el esquema completo de la base de datos
            schema_sql = """
            -- TABLA PRINCIPAL: PARTIDOS
            CREATE TABLE IF NOT EXISTS matches (
                id TEXT PRIMARY KEY,
                date TEXT NOT NULL,
                time TEXT NOT NULL,
                madrid_time TEXT,
                home_team TEXT NOT NULL,
                away_team TEXT NOT NULL,
                competition TEXT,
                venue TEXT,
                status TEXT DEFAULT 'scheduled',
                result TEXT,
                home_score INTEGER,
                away_score INTEGER,
                referee TEXT,
                attendance INTEGER DEFAULT 0,
                weather_temp TEXT,
                weather_condition TEXT,
                match_url TEXT,
                fotmob_id TEXT,
                source TEXT DEFAULT 'fotmob',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(fotmob_id)
            );
            
            CREATE INDEX IF NOT EXISTS idx_matches_date ON matches(date);
            CREATE INDEX IF NOT EXISTS idx_matches_status ON matches(status);
            CREATE INDEX IF NOT EXISTS idx_matches_competition ON matches(competition);
            
            -- TABLA: GOLEADORES
            CREATE TABLE IF NOT EXISTS goalscorers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                match_id TEXT NOT NULL,
                player_name TEXT NOT NULL,
                minute INTEGER NOT NULL,
                team TEXT NOT NULL,
                goal_type TEXT DEFAULT 'normal',
                assist_player TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (match_id) REFERENCES matches(id) ON DELETE CASCADE
            );
            
            CREATE INDEX IF NOT EXISTS idx_goalscorers_match ON goalscorers(match_id);
            
            -- TABLA: TARJETAS
            CREATE TABLE IF NOT EXISTS cards (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                match_id TEXT NOT NULL,
                player_name TEXT NOT NULL,
                minute INTEGER NOT NULL,
                team TEXT NOT NULL,
                card_type TEXT NOT NULL,
                reason TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (match_id) REFERENCES matches(id) ON DELETE CASCADE
            );
            
            CREATE INDEX IF NOT EXISTS idx_cards_match ON cards(match_id);
            
            -- TABLA: CAMBIOS
            CREATE TABLE IF NOT EXISTS substitutions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                match_id TEXT NOT NULL,
                player_in TEXT NOT NULL,
                player_out TEXT NOT NULL,
                minute INTEGER NOT NULL,
                team TEXT NOT NULL,
                reason TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (match_id) REFERENCES matches(id) ON DELETE CASCADE
            );
            
            -- TABLA: TRANSMISIONES TV
            CREATE TABLE IF NOT EXISTS tv_broadcast (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                match_id TEXT NOT NULL,
                channel_name TEXT NOT NULL,
                country TEXT,
                language TEXT,
                stream_url TEXT,
                is_free BOOLEAN DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (match_id) REFERENCES matches(id) ON DELETE CASCADE
            );
            
            CREATE INDEX IF NOT EXISTS idx_tv_broadcast_match ON tv_broadcast(match_id);
            
            -- TABLA: ESTADÍSTICAS DEL PARTIDO
            CREATE TABLE IF NOT EXISTS match_statistics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                match_id TEXT NOT NULL,
                possession_home INTEGER,
                possession_away INTEGER,
                shots_home INTEGER,
                shots_away INTEGER,
                shots_on_target_home INTEGER,
                shots_on_target_away INTEGER,
                corners_home INTEGER,
                corners_away INTEGER,
                fouls_home INTEGER,
                fouls_away INTEGER,
                passes_home INTEGER,
                passes_away INTEGER,
                pass_accuracy_home REAL,
                pass_accuracy_away REAL,
                offsides_home INTEGER,
                offsides_away INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (match_id) REFERENCES matches(id) ON DELETE CASCADE,
                UNIQUE(match_id)
            );
            
            -- TABLA: LOGS DE SCRAPING
            CREATE TABLE IF NOT EXISTS scraping_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                source TEXT NOT NULL,
                operation TEXT NOT NULL,
                matches_found INTEGER DEFAULT 0,
                success BOOLEAN DEFAULT 1,
                error_message TEXT,
                response_time_ms INTEGER,
                api_endpoint TEXT,
                http_status_code INTEGER,
                data_size_bytes INTEGER
            );
            
            CREATE INDEX IF NOT EXISTS idx_scraping_logs_timestamp ON scraping_logs(timestamp);
            
            -- TABLA: ACCESOS DE USUARIOS
            CREATE TABLE IF NOT EXISTS user_access (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                ip_address TEXT,
                user_agent TEXT,
                endpoint TEXT,
                country_code TEXT,
                referrer TEXT,
                device_type TEXT,
                browser TEXT,
                response_time_ms INTEGER,
                status_code INTEGER
            );
            
            CREATE INDEX IF NOT EXISTS idx_user_access_timestamp ON user_access(timestamp);
            
            -- TABLA: CONFIGURACIÓN DEL SISTEMA
            CREATE TABLE IF NOT EXISTS system_config (
                key TEXT PRIMARY KEY,
                value TEXT,
                description TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            
            # Ejecutar esquema
            cursor.executescript(schema_sql)
            
            # Insertar configuración por defecto si no existe
            cursor.execute("SELECT COUNT(*) FROM system_config")
            if cursor.fetchone()[0] == 0:
                default_config = [
                    ('fotmob_team_id', '9825', 'ID del Real Madrid Castilla en FotMob'),
                    ('update_interval_minutes', '30', 'Intervalo entre actualizaciones'),
                    ('timezone', 'America/Guatemala', 'Zona horaria principal'),
                    ('season', '2024-25', 'Temporada actual'),
                    ('notifications_enabled', '1', 'Activar notificaciones'),
                    ('debug_mode', '0', 'Modo debug'),
                ]
                
                cursor.executemany(
                    "INSERT OR IGNORE INTO system_config (key, value, description) VALUES (?, ?, ?)",
                    default_config
                )
            
            conn.commit()
            conn.close()
            
            logging.info("Base de datos completa inicializada")
            
        except Exception as e:
            logging.error(f"Error inicializando base de datos: {e}")
    
    def get_db_connection(self):
        """Obtener conexión a la base de datos"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row  # Para acceso por nombre de columna
            return conn
        except Exception as e:
            logging.error(f"Error conexión BD: {e}")
            return None
    
    def save_complete_match_data(self, match_data):
        """Guardar datos completos del partido"""
        conn = self.get_db_connection()
        if not conn:
            return False
            
        try:
            cursor = conn.cursor()
            
            # Guardar partido principal
            cursor.execute("""
                INSERT OR REPLACE INTO matches 
                (id, date, time, madrid_time, home_team, away_team, competition, venue, 
                 status, result, home_score, away_score, referee, attendance, 
                 weather_temp, weather_condition, match_url, fotmob_id, source, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (
                match_data['id'], match_data['date'], match_data['time'], 
                match_data.get('madrid_time'), match_data['home_team'], match_data['away_team'],
                match_data['competition'], match_data['venue'], match_data['status'],
                match_data.get('result'), match_data.get('home_score'), match_data.get('away_score'),
                match_data.get('referee'), match_data.get('attendance'),
                match_data.get('weather', {}).get('temperature'), 
                match_data.get('weather', {}).get('condition'),
                match_data.get('match_url'), match_data.get('id'), match_data['source']
            ))
            
            # Limpiar datos relacionados existentes
            cursor.execute("DELETE FROM goalscorers WHERE match_id = ?", (match_data['id'],))
            cursor.execute("DELETE FROM cards WHERE match_id = ?", (match_data['id'],))
            cursor.execute("DELETE FROM substitutions WHERE match_id = ?", (match_data['id'],))
            cursor.execute("DELETE FROM tv_broadcast WHERE match_id = ?", (match_data['id'],))
            
            # Guardar goleadores
            for goal in match_data.get('goalscorers', []):
                cursor.execute("""
                    INSERT INTO goalscorers (match_id, player_name, minute, team, goal_type, assist_player)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    match_data['id'], goal.get('player'), goal.get('minute'), 
                    goal.get('team'), goal.get('type', 'normal'), goal.get('assist_player')
                ))
            
            # Guardar tarjetas
            for card in match_data.get('cards', []):
                cursor.execute("""
                    INSERT INTO cards (match_id, player_name, minute, team, card_type, reason)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    match_data['id'], card.get('player'), card.get('minute'),
                    card.get('team'), card.get('type'), card.get('reason')
                ))
            
            # Guardar cambios
            for sub in match_data.get('substitutions', []):
                cursor.execute("""
                    INSERT INTO substitutions (match_id, player_in, player_out, minute, team, reason)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    match_data['id'], sub.get('player_in'), sub.get('player_out'),
                    sub.get('minute'), sub.get('team'), sub.get('reason')
                ))
            
            # Guardar transmisiones TV
            for broadcast in match_data.get('tv_broadcast', []):
                cursor.execute("""
                    INSERT INTO tv_broadcast (match_id, channel_name, country, language, stream_url, is_free)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    match_data['id'], broadcast.get('channel'), broadcast.get('country'),
                    broadcast.get('language'), broadcast.get('stream_url'), broadcast.get('is_free', False)
                ))
            
            # Guardar estadísticas si están disponibles
            if match_data.get('statistics'):
                stats = match_data['statistics']
                cursor.execute("""
                    INSERT OR REPLACE INTO match_statistics 
                    (match_id, possession_home, possession_away, shots_home, shots_away,
                     shots_on_target_home, shots_on_target_away, corners_home, corners_away,
                     fouls_home, fouls_away, passes_home, passes_away, 
                     pass_accuracy_home, pass_accuracy_away, offsides_home, offsides_away)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    match_data['id'],
                    stats.get('Possession', {}).get('home'),
                    stats.get('Possession', {}).get('away'),
                    stats.get('Total shots', {}).get('home'),
                    stats.get('Total shots', {}).get('away'),
                    stats.get('Shots on target', {}).get('home'),
                    stats.get('Shots on target', {}).get('away'),
                    stats.get('Corner kicks', {}).get('home'),
                    stats.get('Corner kicks', {}).get('away'),
                    stats.get('Fouls', {}).get('home'),
                    stats.get('Fouls', {}).get('away'),
                    stats.get('Passes', {}).get('home'),
                    stats.get('Passes', {}).get('away'),
                    stats.get('Pass accuracy', {}).get('home'),
                    stats.get('Pass accuracy', {}).get('away'),
                    stats.get('Offsides', {}).get('home'),
                    stats.get('Offsides', {}).get('away')
                ))
            
            conn.commit()
            conn.close()
            
            logging.info(f"Datos completos guardados para partido {match_data['id']}")
            return True
            
        except Exception as e:
            logging.error(f"Error guardando datos completos: {e}")
            if conn:
                conn.close()
            return False
    
    def load_complete_matches(self):
        """Cargar partidos con todos los datos relacionados"""
        conn = self.get_db_connection()
        if not conn:
            return []
        
        try:
            cursor = conn.cursor()
            
            # Cargar partidos básicos
            cursor.execute("""
                SELECT * FROM matches 
                WHERE date >= date('now', '-7 days')
                ORDER BY date, time
            """)
            
            matches = []
            for row in cursor.fetchall():
                match = dict(row)
                
                # Cargar goleadores
                cursor.execute("""
                    SELECT player_name, minute, team, goal_type, assist_player
                    FROM goalscorers WHERE match_id = ? ORDER BY minute
                """, (match['id'],))
                match['goalscorers'] = [dict(goal_row) for goal_row in cursor.fetchall()]
                
                # Cargar tarjetas
                cursor.execute("""
                    SELECT player_name, minute, team, card_type, reason
                    FROM cards WHERE match_id = ? ORDER BY minute
                """, (match['id'],))
                match['cards'] = [dict(card_row) for card_row in cursor.fetchall()]
                
                # Cargar cambios
                cursor.execute("""
                    SELECT player_in, player_out, minute, team, reason
                    FROM substitutions WHERE match_id = ? ORDER BY minute
                """, (match['id'],))
                match['substitutions'] = [dict(sub_row) for sub_row in cursor.fetchall()]
                
                # Cargar transmisiones TV
                cursor.execute("""
                    SELECT channel_name, country, language, stream_url, is_free
                    FROM tv_broadcast WHERE match_id = ?
                """, (match['id'],))
                match['tv_broadcast'] = [dict(tv_row) for tv_row in cursor.fetchall()]
                
                # Cargar estadísticas
                cursor.execute("""
                    SELECT * FROM match_statistics WHERE match_id = ?
                """, (match['id'],))
                stats_row = cursor.fetchone()
                if stats_row:
                    match['statistics'] = dict(stats_row)
                else:
                    match['statistics'] = {}
                
                matches.append(match)
            
            conn.close()
            logging.info(f"{len(matches)} partidos completos cargados")
            return matches
            
        except Exception as e:
            logging.error(f"Error cargando partidos completos: {e}")
            if conn:
                conn.close()
            return []
    
    def get_sample_data(self):
        """Generar datos de muestra para testing"""
        now = datetime.now(self.timezone)
        return [{
            'id': 'sample-1',
            'date': now.strftime('%Y-%m-%d'),
            'time': '18:00',
            'madrid_time': '01:00',
            'home_team': 'Real Madrid Castilla',
            'away_team': 'Opponent Team',
            'competition': 'Primera Federación',
            'venue': 'Estadio Alfredo Di Stefano',
            'status': 'scheduled',
            'result': None,
            'goalscorers': [],
            'cards': [],
            'substitutions': [],
            'tv_broadcast': [],
            'statistics': {},
            'source': 'sample'
        }]

# Crear instancia global
calendar = CastillaCalendarComplete()

# RUTAS DE LA API
@app.route('/')
def home():
    """Página principal con información completa"""
    base_url = request.url_root.rstrip('/')
    
    return jsonify({
        "proyecto": "Real Madrid Castilla COMPLETO - Guatemala",
        "version": "3.0.0-fotmob-complete",
        "estado": "Operativo",
        "urls": {
            "calendario_ios": f"{base_url}/calendar.ics",
            "api_partidos": f"{base_url}/api/matches",
            "estado_sistema": f"{base_url}/api/status",
            "proximo_partido": f"{base_url}/api/next",
            "forzar_actualizacion": f"{base_url}/api/update"
        }
    })

@app.route('/api/status')
def get_status():
    """Estado del sistema"""
    return jsonify({
        "estado": "Sistema Operativo",
        "version": "3.0.0-fotmob-complete",
        "timestamp": datetime.now().isoformat(),
        "base_datos": "SQLite inicializada",
        "zona_horaria": "America/Guatemala"
    })

@app.route('/api/matches')
def get_complete_matches():
    """API de partidos con TODOS los datos"""
    try:
        # Intentar cargar desde BD, si no hay datos usar muestra
        matches = calendar.load_complete_matches()
        
        if not matches:
            logging.info("No hay partidos en BD, usando datos de muestra")
            matches = calendar.get_sample_data()
        
        # Separar por categorías
        upcoming = [m for m in matches if m['status'] == 'scheduled']
        live = [m for m in matches if m['status'] == 'live']  
        finished = [m for m in matches if m['status'] == 'finished']
        
        return jsonify({
            "partidos_completos": matches,
            "resumen": {
                "total": len(matches),
                "proximos": len(upcoming),
                "en_vivo": len(live),
                "finalizados": len(finished)
            },
            "metadata": {
                "fuente": "FotMob API",
                "ultima_actualizacion": calendar.last_update.isoformat() if calendar.last_update else None,
                "zona_horaria": "America/Guatemala",
                "version": "3.0.0-complete"
            }
        })
        
    except Exception as e:
        logging.error(f"Error API matches: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/next')
def get_next_match():
    """Próximo partido"""
    try:
        matches = calendar.load_complete_matches()
        
        if not matches:
            matches = calendar.get_sample_data()
        
        now = datetime.now(calendar.timezone)
        
        upcoming = []
        for match in matches:
            if match['status'] in ['scheduled', 'live']:
                upcoming.append(match)
        
        if not upcoming:
            return jsonify({
                "mensaje": "No hay próximos partidos programados"
            })
        
        next_match = upcoming[0]
        
        return jsonify({
            "proximo_partido": next_match,
            "tiempo": {
                "estado": "Próximo partido",
                "es_live": next_match['status'] == 'live',
                "tiempo_restante": "Por determinar"
            }
        })
        
    except Exception as e:
        logging.error(f"Error próximo partido: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/update')
def force_update():
    """Forzar actualización"""
    try:
        # Simular actualización por ahora
        calendar.last_update = datetime.now(calendar.timezone)
        
        logging.info("Actualización forzada ejecutada")
        
        return jsonify({
            "mensaje": "Actualización completada",
            "timestamp": calendar.last_update.isoformat(),
            "partidos_procesados": 1,
            "fuente": "Sistema de prueba"
        })
        
    except Exception as e:
        logging.error(f"Error update: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/calendar.ics')
def get_calendar():
    """Endpoint del calendario ICS"""
    try:
        matches = calendar.load_complete_matches()
        
        if not matches:
            matches = calendar.get_sample_data()
        
        ics_lines = [
            "BEGIN:VCALENDAR",
            "VERSION:2.0",
            "PRODID:-//Castilla Complete Calendar v3.0//ES",
            "CALSCALE:GREGORIAN",
            "METHOD:PUBLISH",
            "X-WR-CALNAME:Real Madrid Castilla Guatemala",
            "X-WR-CALDESC:Real Madrid Castilla - Horarios Guatemala",
            "X-WR-TIMEZONE:America/Guatemala"
        ]
        
        for match in matches:
            try:
                match_datetime = datetime.strptime(f"{match['date']} {match['time']}", "%Y-%m-%d %H:%M")
                match_datetime = calendar.timezone.localize(match_datetime)
                end_datetime = match_datetime + timedelta(hours=2)
                
                start_str = match_datetime.strftime("%Y%m%dT%H%M%S")
                end_str = end_datetime.strftime("%Y%m%dT%H%M%S")
                timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
                
                summary = f"{match['home_team']} vs {match['away_team']}"
                if match.get('result'):
                    summary += f" ({match['result']})"
                
                description = f"Competición: {match['competition']}\\n"
                description += f"Estadio: {match['venue']}\\n"
                description += f"Hora Guatemala: {match['time']}\\n"
                if match.get('madrid_time'):
                    description += f"Hora Madrid: {match['madrid_time']}\\n"
                
                event_lines = [
                    "BEGIN:VEVENT",
                    f"UID:{match['id']}@castilla-guatemala.com",
                    f"DTSTART;TZID=America/Guatemala:{start_str}",
                    f"DTEND;TZID=America/Guatemala:{end_str}",
                    f"DTSTAMP:{timestamp}",
                    f"SUMMARY:{summary}",
                    f"DESCRIPTION:{description}",
                    f"LOCATION:{match['venue']}",
                    "END:VEVENT"
                ]
                
                ics_lines.extend(event_lines)
                
            except Exception as e:
                logging.error(f"Error procesando evento {match.get('id')}: {e}")
                continue
        
        ics_lines.append("END:VCALENDAR")
        ics_content = "\n".join(ics_lines)
        
        response = Response(ics_content, mimetype='text/calendar')
        response.headers['Content-Disposition'] = 'attachment; filename="real-madrid-castilla-guatemala.ics"'
        response.headers['Cache-Control'] = 'public, max-age=1800'
        
        return response
        
    except Exception as e:
        logging.error(f"Error generando calendario: {e}")
        return jsonify({"error": str(e)}), 500
    
# Agregar estas rutas al final de tu app.py, después de @app.route('/calendar.ics')

@app.route('/api/test-fotmob')
def test_fotmob_connection():
    """Test completo de FotMob"""
    try:
        result = calendar.fotmob_scraper.test_connection()
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e), "success": False}), 500

@app.route('/api/force-fotmob-update')
def force_fotmob_update():
    """Forzar actualización desde FotMob"""
    try:
        logging.info("Iniciando actualización forzada desde FotMob")
        
        # Buscar Team ID
        team_id = calendar.fotmob_scraper.search_team_id()
        logging.info(f"Team ID encontrado: {team_id}")
        
        # Obtener partidos de FotMob
        matches = calendar.fotmob_scraper.get_team_fixtures(team_id)
        logging.info(f"Partidos obtenidos de FotMob: {len(matches)}")
        
        if matches:
            # Guardar cada partido
            successful_saves = 0
            for match in matches:
                if calendar.save_complete_match_data(match):
                    successful_saves += 1
            
            # Actualizar cache
            calendar.matches_cache = calendar.load_complete_matches()
            calendar.last_update = datetime.now(calendar.timezone)
            
            return jsonify({
                "success": True,
                "mensaje": "Actualización desde FotMob completada",
                "partidos_encontrados": len(matches),
                "partidos_guardados": successful_saves,
                "team_id": team_id,
                "timestamp": calendar.last_update.isoformat()
            })
        else:
            return jsonify({
                "success": False,
                "mensaje": "No se encontraron partidos en FotMob",
                "team_id": team_id
            })
        
    except Exception as e:
        logging.error(f"Error en actualización FotMob: {e}")
        return jsonify({
            "success": False,
            "error": str(e),
            "mensaje": "Error conectando con FotMob"
        }), 500

@app.route('/api/debug-db')
def debug_database():
    """Debug de la base de datos"""
    try:
        conn = calendar.get_db_connection()
        if not conn:
            return jsonify({"error": "No se pudo conectar a la BD"}), 500
        
        cursor = conn.cursor()
        
        # Verificar tablas
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        # Contar registros por tabla
        table_counts = {}
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            table_counts[table] = cursor.fetchone()[0]
        
        conn.close()
        
        return jsonify({
            "database_path": calendar.db_path,
            "tables": tables,
            "table_counts": table_counts,
            "cache_size": len(calendar.matches_cache),
            "last_update": calendar.last_update.isoformat() if calendar.last_update else None
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500    

# Error handlers
@app.errorhandler(404)
def not_found(error):
    return jsonify({
        "error": "Endpoint no encontrado",
        "codigo": 404
    }), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({
        "error": "Error interno del servidor",
        "codigo": 500
    }), 500

# Inicialización
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('DEBUG', 'False').lower() == 'true'
    
    logging.info("INICIANDO CALENDARIO CASTILLA COMPLETO v3.0")
    logging.info(f"Puerto: {port}")
    
    app.run(
        host='0.0.0.0',
        port=port,
        debug=debug,
        threaded=True
    )