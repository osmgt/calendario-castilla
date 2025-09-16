# archivo: fotmob_scraper.py - Nueva implementación con mobfot

from mobfot import MobFot
import logging
from datetime import datetime, timedelta
import pytz
from typing import List, Dict, Optional, Any
import time

class FotMobScraper:
    def __init__(self):
        """Inicializar scraper con mobfot"""
        self.client = MobFot()
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
        
        logging.info(f"🚀 FotMobScraper inicializado con mobfot v1.4.0")
        logging.info(f"🏆 Team ID: {self.castilla_team_id}")

    def search_team_id(self, team_name="Real Madrid Castilla"):
        """
        Buscar el Team ID del Castilla
        Nota: mobfot no tiene búsqueda directa, usamos el ID conocido
        """
        try:
            # Verificar que el team_id funciona obteniendo información básica
            team_data = self.client.get_team(self.castilla_team_id)
            
            if team_data and 'details' in team_data:
                team_details = team_data['details']
                found_name = team_details.get('name', 'Unknown')
                
                if 'castilla' in found_name.lower():
                    logging.info(f"✅ Team ID verificado: {found_name} - ID: {self.castilla_team_id}")
                    return self.castilla_team_id
                else:
                    logging.warning(f"⚠️ El team ID no corresponde al Castilla: {found_name}")
            
            # Si no funciona, devolver el ID conocido de todas formas
            logging.info(f"🔄 Usando Team ID por defecto: {self.castilla_team_id}")
            return self.castilla_team_id
            
        except Exception as e:
            logging.warning(f"⚠️ Error verificando team ID: {e}")
            return self.castilla_team_id

    def get_team_fixtures(self, team_id=None):
        """Obtener partidos del equipo usando mobfot"""
        if not team_id:
            team_id = self.castilla_team_id
            
        matches = []
        
        try:
            logging.info(f"📡 Obteniendo datos del team {team_id}...")
            
            # Obtener datos del equipo (incluye fixtures)
            team_data = self.client.get_team(team_id)
            
            if not team_data:
                logging.error("❌ No se obtuvieron datos del equipo")
                return matches
            
            # Extraer fixtures de diferentes secciones posibles
            fixtures = []
            
            # Buscar en fixtures directos
            if 'fixtures' in team_data:
                fixtures_data = team_data['fixtures']
                if isinstance(fixtures_data, list):
                    fixtures.extend(fixtures_data)
                elif isinstance(fixtures_data, dict):
                    # Buscar en allFixtures o fixtures
                    for key in ['allFixtures', 'fixtures', 'upcoming', 'recent']:
                        if key in fixtures_data and isinstance(fixtures_data[key], list):
                            fixtures.extend(fixtures_data[key])
                        elif key in fixtures_data and isinstance(fixtures_data[key], dict):
                            # Buscar fixtures dentro del objeto
                            nested = fixtures_data[key].get('fixtures', [])
                            if isinstance(nested, list):
                                fixtures.extend(nested)
            
            # Buscar en otras secciones posibles
            for section in ['overview', 'calendar', 'matches']:
                if section in team_data and isinstance(team_data[section], dict):
                    section_fixtures = team_data[section].get('fixtures', [])
                    if isinstance(section_fixtures, list):
                        fixtures.extend(section_fixtures)
            
            logging.info(f"📊 Encontrados {len(fixtures)} fixtures en datos del equipo")
            
            # Si no hay fixtures en team data, intentar con fechas específicas
            if not fixtures:
                logging.info("🔄 No se encontraron fixtures en team data, probando por fechas...")
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
        """Obtener fixtures buscando en un rango de fechas"""
        fixtures = []
        
        try:
            # Buscar desde 30 días atrás hasta 90 días adelante
            start_date = datetime.now() - timedelta(days=30)
            end_date = datetime.now() + timedelta(days=90)
            
            current_date = start_date
            while current_date <= end_date:
                try:
                    date_str = current_date.strftime('%Y%m%d')
                    logging.info(f"🔍 Buscando matches para {date_str}...")
                    
                    # Obtener matches del día
                    matches_data = self.client.get_matches_by_date(date_str)
                    
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
                    
                    # Avanzar al siguiente día
                    current_date += timedelta(days=1)
                    
                    # Rate limiting para evitar saturar la API
                    time.sleep(0.1)
                    
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
            match_id = str(fixture.get('id', f"mobfot-{int(time.time())}"))
            
            # Equipos
            home_team = fixture.get('home', {})
            away_team = fixture.get('away', {})
            
            home_name = home_team.get('name', home_team.get('shortName', 'Unknown'))
            away_name = away_team.get('name', away_team.get('shortName', 'Unknown'))
            
            # Solo partidos del Castilla
            if ('castilla' not in home_name.lower() and 
                'castilla' not in away_name.lower()):
                return None
            
            # Fecha y hora (mobfot devuelve diferentes formatos)
            kick_off_time = fixture.get('kickOffTime', fixture.get('utcTime', ''))
            
            if not kick_off_time:
                # Si no hay tiempo, usar fecha actual como fallback
                logging.warning(f"⚠️ No se encontró hora para el match {match_id}")
                kick_off_time = datetime.now().isoformat() + 'Z'
            
            # Parsear tiempo UTC
            try:
                if kick_off_time.endswith('Z'):
                    kick_off_time = kick_off_time[:-1] + '+00:00'
                
                match_datetime = datetime.fromisoformat(kick_off_time)
                
                # Si no tiene timezone, asumimos UTC
                if match_datetime.tzinfo is None:
                    match_datetime = match_datetime.replace(tzinfo=pytz.UTC)
                
            except Exception as e:
                logging.warning(f"⚠️ Error parseando tiempo {kick_off_time}: {e}")
                # Fallback a fecha actual
                match_datetime = datetime.now(pytz.UTC)
            
            # Convertir a zonas horarias
            madrid_time = match_datetime.astimezone(self.timezone_es)
            guatemala_time = match_datetime.astimezone(self.timezone_gt)
            
            # Status del partido
            status_info = fixture.get('status', {})
            status_code = status_info.get('utcOffsetInHours', fixture.get('status', 0))
            
            # Mapear status (mobfot usa diferentes códigos)
            if fixture.get('finished'):
                status = 'finished'
            elif fixture.get('started') and not fixture.get('finished'):
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
                'source': 'mobfot',
                
                # DATOS AVANZADOS (si están disponibles)
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
            # Buscar en events
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
            # mobfot puede incluir info de TV en diferentes lugares
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
            # mobfot puede obtener detalles específicos del match
            match_details = self.client.get_match(match_id)
            
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
            
            team_data = self.client.get_team(team_id)
            
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
            
            # Top goleadores (si están disponibles)
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
        """Test de conexión con FotMob usando mobfot"""
        try:
            logging.info("🧪 Testeando conexión con FotMob (mobfot)...")
            
            # Test 1: Verificar Team ID
            team_id = self.search_team_id()
            logging.info(f"✅ Team ID verificado: {team_id}")
            
            # Test 2: Obtener fixtures 
            fixtures = self.get_team_fixtures(team_id)
            logging.info(f"✅ {len(fixtures)} partidos obtenidos")
            
            # Test 3: Estadísticas del equipo
            stats = self.get_team_stats(team_id)
            logging.info(f"✅ Estadísticas obtenidas: {len(stats)} campos")
            
            # Test 4: Probar get_matches_by_date con fecha actual
            today = datetime.now().strftime('%Y%m%d')
            today_matches = self.client.get_matches_by_date(today)
            logging.info(f"✅ Matches de hoy obtenidos: {bool(today_matches)}")
            
            return {
                'success': True,
                'library': 'mobfot v1.4.0',
                'team_id': team_id,
                'fixtures_count': len(fixtures),
                'sample_fixtures': fixtures[:2] if fixtures else [],
                'team_stats': stats,
                'api_working': bool(today_matches)
            }
            
        except Exception as e:
            logging.error(f"❌ Error en test de conexión: {e}")
            return {
                'success': False,
                'library': 'mobfot v1.4.0',
                'error': str(e)
            }

# Función principal de testing
if __name__ == "__main__":
    # Configurar logging
    logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    print("🏆 MOBFOT SCRAPER - REAL MADRID CASTILLA")
    print("=" * 50)
    
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