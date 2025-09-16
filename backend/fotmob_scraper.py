# archivo: fotmob_scraper.py

import requests
import json
import logging
from datetime import datetime, timedelta
import pytz
from typing import List, Dict, Optional

class FotMobScraper:
    def __init__(self):
        self.base_url = "https://www.fotmob.com/api"
        self.timezone_gt = pytz.timezone('America/Guatemala')
        self.timezone_es = pytz.timezone('Europe/Madrid')
        
        # Headers para parecer navegador real
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Referer': 'https://www.fotmob.com/',
            'Origin': 'https://www.fotmob.com',
            'Connection': 'keep-alive',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin'
        }
        
        # ID del Real Madrid Castilla en FotMob
        self.castilla_team_id = "8367"  # Verificar este ID
        
    def search_team_id(self, team_name="Real Madrid Castilla"):
        """Buscar ID del equipo en FotMob"""
        try:
            url = f"{self.base_url}/searchapi/"
            params = {'term': team_name}
            
            response = requests.get(url, headers=self.headers, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                # Buscar en equipos
                for team in data.get('teams', []):
                    if 'castilla' in team.get('name', '').lower():
                        logging.info(f"✅ Encontrado: {team.get('name')} - ID: {team.get('id')}")
                        return team.get('id')
                        
            logging.warning("⚠️ No se encontró ID del Castilla, usando ID por defecto")
            return self.castilla_team_id
            
        except Exception as e:
            logging.error(f"❌ Error buscando team ID: {e}")
            return self.castilla_team_id
    
    def get_team_fixtures(self, team_id=None):
        """Obtener partidos del equipo"""
        if not team_id:
            team_id = self.castilla_team_id
            
        try:
            url = f"{self.base_url}/teams"
            params = {
                'id': team_id,
                'tab': 'fixtures',
                'type': 'team',
                'timeZone': 'America/Guatemala'
            }
            
            response = requests.get(url, headers=self.headers, params=params, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                logging.info("✅ Datos de FotMob obtenidos correctamente")
                return self.parse_fixtures_data(data)
            else:
                logging.error(f"❌ Error FotMob API: {response.status_code}")
                return []
                
        except Exception as e:
            logging.error(f"❌ Error obteniendo fixtures: {e}")
            return []
    
    def parse_fixtures_data(self, data):
        """Parsear datos de partidos de FotMob"""
        matches = []
        
        try:
            # Buscar fixtures en diferentes secciones
            fixtures_sections = [
                data.get('fixtures', {}).get('allFixtures', {}).get('fixtures', []),
                data.get('overview', {}).get('fixtures', []),
                data.get('allFixtures', [])
            ]
            
            all_fixtures = []
            for section in fixtures_sections:
                if isinstance(section, list):
                    all_fixtures.extend(section)
                elif isinstance(section, dict):
                    for key, value in section.items():
                        if isinstance(value, list):
                            all_fixtures.extend(value)
            
            logging.info(f"📊 Procesando {len(all_fixtures)} fixtures encontrados")
            
            for fixture in all_fixtures:
                try:
                    match_data = self.parse_single_match(fixture)
                    if match_data:
                        matches.append(match_data)
                        
                except Exception as e:
                    logging.warning(f"⚠️ Error procesando fixture individual: {e}")
                    continue
            
            logging.info(f"✅ {len(matches)} partidos procesados exitosamente")
            return matches
            
        except Exception as e:
            logging.error(f"❌ Error parseando fixtures: {e}")
            return []
    
    def parse_single_match(self, fixture):
        """Parsear un partido individual con todos los datos"""
        try:
            # Información básica
            match_id = fixture.get('id', f"fotmob-{fixture.get('matchId', 'unknown')}")
            
            # Equipos
            home_team = fixture.get('home', {})
            away_team = fixture.get('away', {})
            
            home_name = home_team.get('name', home_team.get('shortName', 'Unknown'))
            away_name = away_team.get('name', away_team.get('shortName', 'Unknown'))
            
            # Solo partidos del Castilla
            if 'castilla' not in home_name.lower() and 'castilla' not in away_name.lower():
                return None
            
            # Fecha y hora
            utc_time = fixture.get('utcTime', fixture.get('kickOffTime', ''))
            if not utc_time:
                return None
                
            # Convertir tiempo
            match_datetime = datetime.fromisoformat(utc_time.replace('Z', '+00:00'))
            madrid_time = match_datetime.astimezone(self.timezone_es)
            guatemala_time = match_datetime.astimezone(self.timezone_gt)
            
            # Status del partido
            status_info = fixture.get('status', {})
            status_code = status_info.get('code', 0)
            status_text = status_info.get('displayName', '')
            
            # Mapear status
            if status_code == 0 or 'fixture' in status_text.lower():
                status = 'scheduled'
            elif status_code == 1 or 'live' in status_text.lower():
                status = 'live'
            elif status_code == 3 or 'finished' in status_text.lower():
                status = 'finished'
            else:
                status = 'scheduled'
            
            # Resultado
            result = None
            home_score = None
            away_score = None
            
            if status == 'finished' or status == 'live':
                home_score = home_team.get('score')
                away_score = away_team.get('score')
                
                if home_score is not None and away_score is not None:
                    result = f"{home_score}-{away_score}"
            
            # Competición
            competition_info = fixture.get('leagueName', fixture.get('ccode', ''))
            if not competition_info:
                competition_info = fixture.get('parentLeagueName', 'Primera Federación')
            
            # Venue/Estadio
            venue = 'Por confirmar'
            if fixture.get('venue'):
                venue = fixture['venue'].get('name', venue)
            elif 'castilla' in home_name.lower():
                venue = 'Estadio Alfredo Di Stéfano'
            
            # DATOS AVANZADOS
            advanced_data = self.get_advanced_match_data(match_id, fixture)
            
            # Construir objeto completo
            match_data = {
                'id': str(match_id),
                'date': guatemala_time.strftime('%Y-%m-%d'),
                'time': guatemala_time.strftime('%H:%M'),
                'madrid_time': madrid_time.strftime('%H:%M'),
                'home_team': home_name,
                'away_team': away_name,
                'competition': competition_info,
                'venue': venue,
                'status': status,
                'result': result,
                'home_score': home_score,
                'away_score': away_score,
                'source': 'fotmob',
                
                # DATOS AVANZADOS
                'goalscorers': advanced_data.get('goalscorers', []),
                'cards': advanced_data.get('cards', []),
                'substitutions': advanced_data.get('substitutions', []),
                'tv_broadcast': advanced_data.get('tv_broadcast', []),
                'referee': advanced_data.get('referee', ''),
                'attendance': advanced_data.get('attendance', 0),
                'weather': advanced_data.get('weather', {}),
                'statistics': advanced_data.get('statistics', {}),
                'lineups': advanced_data.get('lineups', {}),
                'head_to_head': advanced_data.get('head_to_head', {}),
                
                # Metadata
                'match_url': f"https://www.fotmob.com/matches/{match_id}",
                'last_updated': datetime.now(self.timezone_gt).isoformat(),
                'raw_data': fixture  # Para debugging
            }
            
            return match_data
            
        except Exception as e:
            logging.error(f"❌ Error parseando match individual: {e}")
            return None
    
    def get_advanced_match_data(self, match_id, fixture):
        """Obtener datos avanzados del partido"""
        advanced = {
            'goalscorers': [],
            'cards': [],
            'substitutions': [],
            'tv_broadcast': [],
            'referee': '',
            'attendance': 0,
            'weather': {},
            'statistics': {},
            'lineups': {},
            'head_to_head': {}
        }
        
        try:
            # Goleadores
            if fixture.get('events'):
                for event in fixture['events']:
                    event_type = event.get('type', '')
                    
                    if event_type == 'goal':
                        scorer_info = {
                            'player': event.get('player', {}).get('name', 'Unknown'),
                            'minute': event.get('minute', 0),
                            'team': event.get('teamId') == fixture.get('home', {}).get('id') and 'home' or 'away',
                            'type': event.get('goalType', 'normal')  # normal, penalty, own_goal
                        }
                        advanced['goalscorers'].append(scorer_info)
                    
                    elif event_type in ['yellow_card', 'red_card']:
                        card_info = {
                            'player': event.get('player', {}).get('name', 'Unknown'),
                            'minute': event.get('minute', 0),
                            'team': event.get('teamId') == fixture.get('home', {}).get('id') and 'home' or 'away',
                            'type': event_type
                        }
                        advanced['cards'].append(card_info)
                    
                    elif event_type == 'substitution':
                        sub_info = {
                            'player_in': event.get('playerIn', {}).get('name', 'Unknown'),
                            'player_out': event.get('playerOut', {}).get('name', 'Unknown'),
                            'minute': event.get('minute', 0),
                            'team': event.get('teamId') == fixture.get('home', {}).get('id') and 'home' or 'away'
                        }
                        advanced['substitutions'].append(sub_info)
            
            # TV/Broadcast info
            if fixture.get('tvChannels'):
                for channel in fixture['tvChannels']:
                    broadcast_info = {
                        'channel': channel.get('name', ''),
                        'country': channel.get('country', ''),
                        'language': channel.get('language', ''),
                        'stream_url': channel.get('streamUrl', '')
                    }
                    advanced['tv_broadcast'].append(broadcast_info)
            
            # Árbitro
            if fixture.get('referee'):
                advanced['referee'] = fixture['referee'].get('name', '')
            
            # Asistencia
            if fixture.get('attendance'):
                advanced['attendance'] = fixture['attendance']
            
            # Clima (si está disponible)
            if fixture.get('weather'):
                advanced['weather'] = {
                    'temperature': fixture['weather'].get('temperature', ''),
                    'condition': fixture['weather'].get('condition', ''),
                    'humidity': fixture['weather'].get('humidity', '')
                }
            
            # Estadísticas del partido
            if fixture.get('stats'):
                stats = {}
                for stat in fixture['stats']:
                    stat_name = stat.get('name', '')
                    home_value = stat.get('home', '')
                    away_value = stat.get('away', '')
                    
                    stats[stat_name] = {
                        'home': home_value,
                        'away': away_value
                    }
                
                advanced['statistics'] = stats
            
            logging.info(f"📊 Datos avanzados obtenidos para partido {match_id}")
            
        except Exception as e:
            logging.warning(f"⚠️ Error obteniendo datos avanzados: {e}")
        
        return advanced
    
    def get_detailed_match_info(self, match_id):
        """Obtener información detallada de un partido específico"""
        try:
            url = f"{self.base_url}/matchDetails"
            params = {'matchId': match_id}
            
            response = requests.get(url, headers=self.headers, params=params, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                logging.info(f"✅ Detalles del partido {match_id} obtenidos")
                return data
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
            url = f"{self.base_url}/teams"
            params = {
                'id': team_id,
                'tab': 'overview',
                'type': 'team'
            }
            
            response = requests.get(url, headers=self.headers, params=params, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                
                season_stats = {
                    'league_position': data.get('table', {}).get('position', 0),
                    'points': data.get('table', {}).get('points', 0),
                    'games_played': data.get('table', {}).get('played', 0),
                    'wins': data.get('table', {}).get('wins', 0),
                    'draws': data.get('table', {}).get('draws', 0),
                    'losses': data.get('table', {}).get('losses', 0),
                    'goals_for': data.get('table', {}).get('goalsFor', 0),
                    'goals_against': data.get('table', {}).get('goalsAgainst', 0),
                    'goal_difference': data.get('table', {}).get('goalDifference', 0),
                    'form': data.get('form', []),
                    'top_scorers': [],
                    'next_opponent': '',
                    'last_5_results': []
                }
                
                # Top goleadores del equipo
                if data.get('squad'):
                    for player in data['squad'][:10]:  # Top 10
                        if player.get('goals', 0) > 0:
                            season_stats['top_scorers'].append({
                                'name': player.get('name', ''),
                                'goals': player.get('goals', 0),
                                'assists': player.get('assists', 0),
                                'games': player.get('games', 0)
                            })
                
                logging.info("✅ Estadísticas del equipo obtenidas")
                return season_stats
                
            else:
                logging.warning("⚠️ No se pudieron obtener estadísticas del equipo")
                return {}
                
        except Exception as e:
            logging.error(f"❌ Error obteniendo estadísticas: {e}")
            return {}
    
    def test_connection(self):
        """Test de conexión con FotMob"""
        try:
            logging.info("🧪 Testeando conexión con FotMob...")
            
            # Test 1: Búsqueda de equipo
            team_id = self.search_team_id()
            logging.info(f"✅ Team ID encontrado: {team_id}")
            
            # Test 2: Obtener fixtures
            fixtures = self.get_team_fixtures(team_id)
            logging.info(f"✅ {len(fixtures)} partidos obtenidos")
            
            # Test 3: Estadísticas del equipo
            stats = self.get_team_stats(team_id)
            logging.info(f"✅ Estadísticas obtenidas: {len(stats)} campos")
            
            return {
                'success': True,
                'team_id': team_id,
                'fixtures_count': len(fixtures),
                'sample_fixtures': fixtures[:2] if fixtures else [],
                'team_stats': stats
            }
            
        except Exception as e:
            logging.error(f"❌ Error en test de conexión: {e}")
            return {
                'success': False,
                'error': str(e)
            }

# Función principal de testing
if __name__ == "__main__":
    # Configurar logging
    logging.basicConfig(level=logging.INFO, 
                       format='%(asctime)s - %(levelname)s - %(message)s')
    
    print("🏆 FOTMOB SCRAPER - REAL MADRID CASTILLA")
    print("=" * 50)
    
    # Crear scraper
    scraper = FotMobScraper()
    
    # Ejecutar test
    result = scraper.test_connection()
    
    if result['success']:
        print("\n✅ CONEXIÓN EXITOSA")
        print(f"📍 Team ID: {result['team_id']}")
        print(f"⚽ Partidos encontrados: {result['fixtures_count']}")
        
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
    
    print("\n🎉 Test completado!")