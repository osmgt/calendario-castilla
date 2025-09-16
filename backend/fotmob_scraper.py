# archivo: fotmob_scraper.py - Versión mejorada contra error 401

from mobfot import MobFot
import logging
from datetime import datetime, timedelta
import pytz
from typing import List, Dict, Optional, Any
import time
import requests

class FotMobScraper:
    def __init__(self):
        """Inicializar scraper con mobfot mejorado"""
        
        # Configurar mobfot con headers mejorados
        self.client = MobFot()
        
        # Configurar session personalizada con headers más realistas
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Referer': 'https://www.fotmob.com/',
            'Origin': 'https://www.fotmob.com',
            'Connection': 'keep-alive',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        })
        
        self.timezone_gt = pytz.timezone('America/Guatemala')
        self.timezone_es = pytz.timezone('Europe/Madrid')
        
        # ID del Real Madrid Castilla confirmado
        self.castilla_team_id = "8367"
        
        # Mapeo de competiciones para Primera Federación
        self.competition_mapping = {
            'primera-federacion': 'Primera Federación',
            'primera federacion': 'Primera Federación', 
            'primera-division-group-1': 'Primera Federación Grupo 1',
            'copa-del-rey': 'Copa del Rey'
        }
        
        logging.info(f"🚀 FotMobScraper inicializado con mobfot v1.4.0 + headers mejorados")
        logging.info(f"🏆 Team ID: {self.castilla_team_id}")

    def _make_request(self, url, params=None, max_retries=3):
        """Hacer petición HTTP con reintentos y rate limiting"""
        for attempt in range(max_retries):
            try:
                # Delay entre peticiones para evitar rate limiting
                if attempt > 0:
                    delay = 2 ** attempt  # Exponential backoff
                    logging.info(f"⏰ Esperando {delay}s antes del intento {attempt + 1}")
                    time.sleep(delay)
                
                response = self.session.get(url, params=params, timeout=15)
                
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 401:
                    logging.warning(f"⚠️ Error 401 en intento {attempt + 1}")
                    continue
                elif response.status_code == 429:
                    logging.warning(f"⚠️ Rate limit en intento {attempt + 1}")
                    time.sleep(5)
                    continue
                else:
                    logging.warning(f"⚠️ HTTP {response.status_code} en intento {attempt + 1}")
                    continue
                    
            except Exception as e:
                logging.warning(f"⚠️ Error en petición intento {attempt + 1}: {e}")
                continue
        
        logging.error(f"❌ Falló después de {max_retries} intentos")
        return None

    def search_team_id(self, team_name="Real Madrid Castilla"):
        """Buscar el Team ID del Castilla con API manual"""
        try:
            # Usar API directa en lugar de mobfot para evitar 401
            url = "https://www.fotmob.com/api/searchapi"
            params = {'term': 'Real Madrid Castilla'}
            
            data = self._make_request(url, params)
            
            if data and 'teams' in data:
                for team in data['teams']:
                    if 'castilla' in team.get('name', '').lower():
                        team_id = str(team.get('id'))
                        logging.info(f"✅ Team ID encontrado: {team.get('name')} - ID: {team_id}")
                        return team_id
            
            # Fallback al ID conocido
            logging.info(f"🔄 Usando Team ID por defecto: {self.castilla_team_id}")
            return self.castilla_team_id
            
        except Exception as e:
            logging.warning(f"⚠️ Error verificando team ID: {e}")
            return self.castilla_team_id

    def get_team_fixtures(self, team_id=None):
        """Obtener partidos del equipo usando API directa"""
        if not team_id:
            team_id = self.castilla_team_id
            
        matches = []
        
        try:
            logging.info(f"📡 Obteniendo datos del team {team_id} con API directa...")
            
            # Usar API directa de FotMob
            url = "https://www.fotmob.com/api/teams"
            params = {
                'id': team_id,
                'tab': 'fixtures',
                'type': 'team'
            }
            
            team_data = self._make_request(url, params)
            
            if not team_data:
                logging.error("❌ No se obtuvieron datos del equipo")
                return self._get_fixtures_by_date_range()
            
            # Extraer fixtures
            fixtures = []
            
            # Buscar en diferentes estructuras de datos
            if 'fixtures' in team_data:
                fixtures_section = team_data['fixtures']
                
                if isinstance(fixtures_section, list):
                    fixtures.extend(fixtures_section)
                elif isinstance(fixtures_section, dict):
                    for key in ['allFixtures', 'fixtures', 'upcoming', 'recent']:
                        if key in fixtures_section:
                            section_data = fixtures_section[key]
                            if isinstance(section_data, list):
                                fixtures.extend(section_data)
                            elif isinstance(section_data, dict) and 'fixtures' in section_data:
                                fixtures.extend(section_data['fixtures'])
            
            logging.info(f"📊 Encontrados {len(fixtures)} fixtures en datos del equipo")
            
            # Si no hay fixtures, intentar búsqueda por fechas
            if not fixtures:
                logging.info("🔄 No se encontraron fixtures, probando búsqueda por fechas...")
                fixtures = self._get_fixtures_by_date_range()
            
            # Procesar cada fixture
            for fixture in fixtures:
                try:
                    match_data = self.parse_single_match(fixture)
                    if match_data:
                        matches.append(match_data)
                except Exception as e:
                    logging.warning(f"⚠️ Error procesando fixture: {e}")
                    continue
            
            # Ordenar por fecha
            matches.sort(key=lambda x: f"{x['date']} {x['time']}")
            
            logging.info(f"✅ {len(matches)} partidos del Castilla procesados")
            return matches
            
        except Exception as e:
            logging.error(f"❌ Error obteniendo fixtures: {e}")
            return matches

    def _get_fixtures_by_date_range(self):
        """Obtener fixtures buscando por rango de fechas con API directa"""
        fixtures = []
        
        try:
            # Buscar en un rango más corto para reducir peticiones
            start_date = datetime.now() - timedelta(days=7)
            end_date = datetime.now() + timedelta(days=30)
            
            current_date = start_date
            request_count = 0
            max_requests = 10  # Limitar peticiones
            
            while current_date <= end_date and request_count < max_requests:
                try:
                    date_str = current_date.strftime('%Y%m%d')
                    logging.info(f"🔍 Buscando matches para {date_str}...")
                    
                    # API directa para matches por fecha
                    url = "https://www.fotmob.com/api/matches"
                    params = {
                        'date': date_str,
                        'timezone': 'America/Guatemala'
                    }
                    
                    matches_data = self._make_request(url, params)
                    request_count += 1
                    
                    if matches_data and 'leagues' in matches_data:
                        for league in matches_data['leagues']:
                            league_matches = league.get('matches', [])
                            
                            for match in league_matches:
                                # Verificar si es del Castilla
                                home_team = match.get('home', {}).get('name', '')
                                away_team = match.get('away', {}).get('name', '')
                                
                                if ('castilla' in home_team.lower() or 
                                    'castilla' in away_team.lower()):
                                    fixtures.append(match)
                                    logging.info(f"🎯 Encontrado: {home_team} vs {away_team}")
                    
                    # Avanzar día con delay
                    current_date += timedelta(days=1)
                    time.sleep(0.5)  # Delay entre peticiones
                    
                except Exception as e:
                    logging.warning(f"⚠️ Error obteniendo fecha {date_str}: {e}")
                    current_date += timedelta(days=1)
                    continue
                    
        except Exception as e:
            logging.error(f"❌ Error en búsqueda por fechas: {e}")
        
        logging.info(f"📊 Encontrados {len(fixtures)} fixtures por búsqueda de fechas")
        return fixtures

    def parse_single_match(self, fixture):
        """Parsear un partido individual con todos los datos"""
        try:
            # Información básica
            match_id = str(fixture.get('id', f"direct-{int(time.time())}"))
            
            # Equipos
            home_team = fixture.get('home', {})
            away_team = fixture.get('away', {})
            
            home_name = home_team.get('name', home_team.get('shortName', 'Unknown'))
            away_name = away_team.get('name', away_team.get('shortName', 'Unknown'))
            
            # Solo partidos del Castilla
            if ('castilla' not in home_name.lower() and 
                'castilla' not in away_name.lower()):
                return None
            
            # Fecha y hora
            kick_off_time = fixture.get('status', {}).get('utcTime', 
                                      fixture.get('kickOffTime', 
                                                fixture.get('utcTime', '')))
            
            if not kick_off_time:
                logging.warning(f"⚠️ No se encontró hora para el match {match_id}")
                kick_off_time = datetime.now().isoformat() + 'Z'
            
            # Parsear tiempo UTC
            try:
                if kick_off_time.endswith('Z'):
                    kick_off_time = kick_off_time[:-1] + '+00:00'
                
                match_datetime = datetime.fromisoformat(kick_off_time)
                
                if match_datetime.tzinfo is None:
                    match_datetime = match_datetime.replace(tzinfo=pytz.UTC)
                
            except Exception as e:
                logging.warning(f"⚠️ Error parseando tiempo {kick_off_time}: {e}")
                match_datetime = datetime.now(pytz.UTC)
            
            # Convertir a zonas horarias
            madrid_time = match_datetime.astimezone(self.timezone_es)
            guatemala_time = match_datetime.astimezone(self.timezone_gt)
            
            # Status del partido
            status_info = fixture.get('status', {})
            
            if status_info.get('finished'):
                status = 'finished'
            elif status_info.get('started'):
                status = 'live'  
            else:
                status = 'scheduled'
            
            # Resultado
            result = None
            home_score = None  
            away_score = None
            
            if status in ['finished', 'live']:
                home_score = home_team.get('score')
                away_score = away_team.get('score')
                
                if home_score is not None and away_score is not None:
                    result = f"{home_score}-{away_score}"
            
            # Competición
            competition = fixture.get('leagueName', fixture.get('ccode', 'Primera Federación'))
            competition = self.competition_mapping.get(competition.lower(), competition)
            
            # Venue/Estadio
            venue = 'Por confirmar'
            if 'castilla' in home_name.lower():
                venue = 'Estadio Alfredo Di Stéfano'
            elif fixture.get('venue'):
                venue = fixture['venue'].get('name', venue)
            
            # Construir objeto del partido
            match_data = {
                'id': match_id,
                'date': guatemala_time.strftime('%Y-%m-%d'),
                'time': guatemala_time.strftime('%H:%M'),
                'madrid_time': madrid_time.strftime('%H:%M'),
                'home_team': home_name,
                'away_team': away_name,
                'competition': competition,
                'venue': venue,
                'status': status,
                'result': result,
                'home_score': home_score,
                'away_score': away_score,
                'source': 'mobfot-direct',
                
                # DATOS AVANZADOS
                'goalscorers': self._extract_goalscorers(fixture),
                'cards': self._extract_cards(fixture),
                'substitutions': self._extract_substitutions(fixture),
                'tv_broadcast': self._extract_tv_broadcast(fixture),
                'referee': self._extract_referee(fixture),
                'attendance': fixture.get('attendance', 0),
                'weather': self._extract_weather(fixture),
                'statistics': self._extract_statistics(fixture),
                
                # Metadata
                'match_url': f"https://www.fotmob.com/matches/{match_id}",
                'last_updated': datetime.now(self.timezone_gt).isoformat()
            }
            
            return match_data
            
        except Exception as e:
            logging.error(f"❌ Error parseando match: {e}")
            return None

    def _extract_goalscorers(self, fixture):
        """Extraer goleadores del fixture"""
        goalscorers = []
        
        try:
            events = fixture.get('events', [])
            for event in events:
                if event.get('type') == 'goal':
                    goal_info = {
                        'player_name': event.get('player', {}).get('name', 'Unknown'),
                        'minute': event.get('minute', 0),
                        'team': 'home' if event.get('isHome') else 'away',
                        'goal_type': event.get('goalType', 'normal'),
                        'assist_player': event.get('assist', {}).get('name') if event.get('assist') else None
                    }
                    goalscorers.append(goal_info)
                    
        except Exception as e:
            logging.warning(f"⚠️ Error extrayendo goleadores: {e}")
        
        return goalscorers

    def _extract_cards(self, fixture):
        """Extraer tarjetas del fixture"""
        cards = []
        
        try:
            events = fixture.get('events', [])
            for event in events:
                event_type = event.get('type', '')
                if 'card' in event_type.lower():
                    card_info = {
                        'player_name': event.get('player', {}).get('name', 'Unknown'),
                        'minute': event.get('minute', 0),
                        'team': 'home' if event.get('isHome') else 'away',
                        'card_type': 'yellow' if 'yellow' in event_type else 'red',
                        'reason': event.get('reason', '')
                    }
                    cards.append(card_info)
                    
        except Exception as e:
            logging.warning(f"⚠️ Error extrayendo tarjetas: {e}")
        
        return cards

    def _extract_substitutions(self, fixture):
        """Extraer cambios del fixture"""
        substitutions = []
        
        try:
            events = fixture.get('events', [])
            for event in events:
                if event.get('type') == 'substitution':
                    sub_info = {
                        'player_in': event.get('playerIn', {}).get('name', 'Unknown'),
                        'player_out': event.get('playerOut', {}).get('name', 'Unknown'), 
                        'minute': event.get('minute', 0),
                        'team': 'home' if event.get('isHome') else 'away',
                        'reason': event.get('reason', '')
                    }
                    substitutions.append(sub_info)
                    
        except Exception as e:
            logging.warning(f"⚠️ Error extrayendo cambios: {e}")
        
        return substitutions

    def _extract_tv_broadcast(self, fixture):
        """Extraer información de transmisión TV"""
        tv_broadcast = []
        
        try:
            tv_channels = fixture.get('tvChannels', [])
            
            for channel in tv_channels:
                broadcast_info = {
                    'channel_name': channel.get('name', channel.get('channelName', '')),
                    'country': channel.get('country', ''),
                    'language': channel.get('language', ''), 
                    'stream_url': channel.get('streamUrl', ''),
                    'is_free': channel.get('isFree', False)
                }
                tv_broadcast.append(broadcast_info)
                
        except Exception as e:
            logging.warning(f"⚠️ Error extrayendo TV: {e}")
        
        return tv_broadcast

    def _extract_referee(self, fixture):
        """Extraer árbitro"""
        try:
            referee_info = fixture.get('referee', {})
            if isinstance(referee_info, dict):
                return referee_info.get('name', '')
            return str(referee_info) if referee_info else ''
        except:
            return ''

    def _extract_weather(self, fixture):
        """Extraer información del clima"""
        try:
            weather = fixture.get('weather', {})
            return {
                'temperature': weather.get('temperature', ''),
                'condition': weather.get('condition', ''),
                'humidity': weather.get('humidity', '')
            }
        except:
            return {}

    def _extract_statistics(self, fixture):
        """Extraer estadísticas del partido"""
        statistics = {}
        
        try:
            stats = fixture.get('stats', fixture.get('statistics', []))
            
            if isinstance(stats, list):
                for stat in stats:
                    stat_name = stat.get('name', stat.get('title', ''))
                    home_value = stat.get('home', '')
                    away_value = stat.get('away', '')
                    
                    statistics[stat_name] = {
                        'home': home_value,
                        'away': away_value
                    }
            elif isinstance(stats, dict):
                statistics = stats
                
        except Exception as e:
            logging.warning(f"⚠️ Error extrayendo estadísticas: {e}")
        
        return statistics

    def get_detailed_match_info(self, match_id):
        """Obtener información detallada de un partido específico"""
        try:
            url = "https://www.fotmob.com/api/matchDetails"
            params = {'matchId': match_id}
            
            match_details = self._make_request(url, params)
            
            if match_details:
                logging.info(f"✅ Detalles del partido {match_id} obtenidos")
                return match_details
            else:
                logging.warning(f"⚠️ No se pudieron obtener detalles del partido {match_id}")
                return {}
                
        except Exception as e:
            logging.error(f"❌ Error obteniendo detalles del partido: {e}")
            return {}

    def get_team_stats(self, team_id=None):
        """Obtener estadísticas del equipo para la temporada"""
        if not team_id:
            team_id = self.castilla_team_id
            
        try:
            logging.info(f"📊 Obteniendo estadísticas del team {team_id}...")
            
            url = "https://www.fotmob.com/api/teams"
            params = {
                'id': team_id,
                'tab': 'overview',
                'type': 'team'
            }
            
            team_data = self._make_request(url, params)
            
            if not team_data:
                return {}
            
            # Extraer estadísticas de temporada
            season_stats = {
                'league_position': 0,
                'points': 0,
                'games_played': 0,
                'wins': 0,
                'draws': 0,
                'losses': 0,
                'goals_for': 0,
                'goals_against': 0,
                'goal_difference': 0,
                'form': [],
                'top_scorers': [],
                'next_opponent': '',
                'last_5_results': []
            }
            
            # Buscar datos de tabla
            if 'table' in team_data:
                table_data = team_data['table']
                season_stats.update({
                    'league_position': table_data.get('position', 0),
                    'points': table_data.get('points', 0),
                    'games_played': table_data.get('played', 0),
                    'wins': table_data.get('wins', 0),
                    'draws': table_data.get('draws', 0),
                    'losses': table_data.get('losses', 0),
                    'goals_for': table_data.get('goalsFor', 0),
                    'goals_against': table_data.get('goalsAgainst', 0),
                    'goal_difference': table_data.get('goalDifference', 0)
                })
            
            # Buscar forma reciente
            if 'form' in team_data:
                season_stats['form'] = team_data['form']
            
            # Top goleadores
            if 'squad' in team_data:
                top_scorers = []
                for player in team_data['squad'][:10]:
                    if player.get('goals', 0) > 0:
                        top_scorers.append({
                            'name': player.get('name', ''),
                            'goals': player.get('goals', 0),
                            'assists': player.get('assists', 0),
                            'games': player.get('games', 0)
                        })
                season_stats['top_scorers'] = top_scorers
            
            logging.info("✅ Estadísticas del equipo obtenidas")
            return season_stats
            
        except Exception as e:
            logging.error(f"❌ Error obteniendo estadísticas: {e}")
            return {}

    def test_connection(self):
        """Test de conexión con FotMob usando API directa"""
        try:
            logging.info("🧪 Testeando conexión con FotMob (API directa)...")
            
            # Test 1: Verificar Team ID
            team_id = self.search_team_id()
            logging.info(f"✅ Team ID verificado: {team_id}")
            
            # Test 2: Test simple de API
            url = "https://www.fotmob.com/api/teams"
            params = {'id': team_id}
            test_data = self._make_request(url, params)
            api_working = bool(test_data)
            
            # Test 3: Obtener fixtures 
            fixtures = self.get_team_fixtures(team_id)
            logging.info(f"✅ {len(fixtures)} partidos obtenidos")
            
            # Test 4: Estadísticas del equipo
            stats = self.get_team_stats(team_id)
            logging.info(f"✅ Estadísticas obtenidas: {len(stats)} campos")
            
            return {
                'success': api_working,
                'library': 'mobfot v1.4.0 + API directa',
                'team_id': team_id,
                'fixtures_count': len(fixtures),
                'sample_fixtures': fixtures[:2] if fixtures else [],
                'team_stats': stats,
                'api_working': api_working
            }
            
        except Exception as e:
            logging.error(f"❌ Error en test de conexión: {e}")
            return {
                'success': False,
                'library': 'mobfot v1.4.0 + API directa',
                'error': str(e)
            }

# Función principal de testing
if __name__ == "__main__":
    # Configurar logging
    logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    print("🏆 MOBFOT SCRAPER MEJORADO - REAL MADRID CASTILLA")
    print("=" * 60)
    
    # Crear scraper
    scraper = FotMobScraper()
    
    # Ejecutar test
    result = scraper.test_connection()
    
    if result['success']:
        print(f"\n✅ CONEXIÓN EXITOSA con {result['library']}")
        print(f"🏆 Team ID: {result['team_id']}")  
        print(f"⚽ Partidos encontrados: {result['fixtures_count']}")
        print(f"🔗 API funcionando: {result['api_working']}")
        
        if result['sample_fixtures']:
            print("\n📋 MUESTRA DE PARTIDOS:")
            for i, match in enumerate(result['sample_fixtures'], 1):
                print(f"\n{i}. {match['home_team']} vs {match['away_team']}")
                print(f"   📅 {match['date']} - {match['time']} GT")
                print(f"   🏆 {match['competition']}")
                print(f"   🏟️ {match['venue']}")
                print(f"   📊 Status: {match['status']}")
                print(f"   📡 Source: {match['source']}")
                if match.get('result'):
                    print(f"   ⚽ Resultado: {match['result']}")
                if match.get('goalscorers'):
                    print(f"   🥅 Goleadores: {len(match['goalscorers'])}")
                if match.get('tv_broadcast'):
                    print(f"   📺 TV: {len(match['tv_broadcast'])} canales")
        
        if result['team_stats']:
            stats = result['team_stats']
            print(f"\n📊 ESTADÍSTICAS DEL EQUIPO:")
            print(f"   🏆 Posición: {stats.get('league_position', 'N/A')}")
            print(f"   ⚽ PJ: {stats.get('games_played', 0)} - Pts: {stats.get('points', 0)}")
            print(f"   📈 V:{stats.get('wins', 0)} E:{stats.get('draws', 0)} D:{stats.get('losses', 0)}")
            print(f"   🥅 GF:{stats.get('goals_for', 0)} GC:{stats.get('goals_against', 0)}")
            
    else:
        print(f"\n❌ ERROR DE CONEXIÓN: {result['error']}")
        print(f"📚 Librería utilizada: {result['library']}")
    
    print("\n🎉 Test completado!")
    print("👑 ¡Hala Madrid y nada más!")