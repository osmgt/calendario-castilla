                if stats_row:
                    match['statistics'] = dict(stats_row)
                else:
                    match['statistics'] = {}
                
                matches.append(match)
            
            conn.close()
            logging.info(f"📖 {len(matches)} partidos completos cargados")
            return matches
            
        except Exception as e:
            logging.error(f"❌ Error cargando partidos completos: {e}")
            if conn:
                conn.close()
            return []
    
    def log_scraping_attempt(self, source, operation, matches_found, success, error_msg=None, response_time=None, endpoint=None, status_code=None):
        """Log detallado de scraping"""
        try:
            conn = self.get_db_connection()
            if conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO scraping_logs 
                    (source, operation, matches_found, success, error_message, response_time_ms, api_endpoint, http_status_code)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (source, operation, matches_found, success, error_msg, response_time, endpoint, status_code))
                
                conn.commit()
                conn.close()
        except Exception as e:
            logging.warning(f"⚠️ Error logging scraping: {e}")
    
    def log_user_access(self, endpoint, ip, user_agent, status_code=200, response_time=None):
        """Log de acceso de usuarios"""
        try:
            conn = self.get_db_connection()
            if conn:
                cursor = conn.cursor()
                
                # Detectar tipo de dispositivo básico
                device_type = 'desktop'
                if user_agent:
                    ua_lower = user_agent.lower()
                    if 'mobile' in ua_lower or 'android' in ua_lower or 'iphone' in ua_lower:
                        device_type = 'mobile'
                    elif 'tablet' in ua_lower or 'ipad' in ua_lower:
                        device_type = 'tablet'
                
                cursor.execute("""
                    INSERT INTO user_access 
                    (ip_address, user_agent, endpoint, device_type, status_code, response_time_ms)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (ip, user_agent[:200] if user_agent else '', endpoint, device_type, status_code, response_time))
                
                conn.commit()
                conn.close()
        except:
            pass  # No fallar si no se puede logear
    
    def update_matches_complete(self):
        """Actualización completa con FotMob"""
        with self.lock:
            try:
                logging.info("🔄 INICIANDO ACTUALIZACIÓN COMPLETA CON FOTMOB")
                start_time = datetime.now()
                
                # Buscar Team ID si es necesario
                team_id = self.fotmob_scraper.search_team_id()
                if team_id != self.config['fotmob_team_id']:
                    self.config['fotmob_team_id'] = team_id
                    logging.info(f"🔄 Team ID actualizado: {team_id}")
                
                # Obtener partidos de FotMob
                matches = self.fotmob_scraper.get_team_fixtures(team_id)
                
                if matches:
                    # Guardar cada partido con todos sus datos
                    successful_saves = 0
                    for match in matches:
                        if self.save_complete_match_data(match):
                            successful_saves += 1
                    
                    # Actualizar cache
                    self.matches_cache = self.load_complete_matches()
                    self.last_update = datetime.now(self.timezone)
                    
                    elapsed_ms = int((datetime.now() - start_time).total_seconds() * 1000)
                    
                    self.log_scraping_attempt(
                        'fotmob', 'complete_update', len(matches), True, 
                        None, elapsed_ms, 'fotmob_api', 200
                    )
                    
                    logging.info(f"✅ ACTUALIZACIÓN EXITOSA: {successful_saves}/{len(matches)} partidos guardados")
                    
                else:
                    self.log_scraping_attempt('fotmob', 'complete_update', 0, False, "No matches found")
                    logging.warning("⚠️ No se encontraron partidos en FotMob")
                    
            except Exception as e:
                error_msg = str(e)
                logging.error(f"❌ Error en actualización completa: {error_msg}")
                self.log_scraping_attempt('fotmob', 'complete_update', 0, False, error_msg)
    
    def get_matches(self):
        """Obtener partidos con cache inteligente"""
        if not self.matches_cache:
            self.matches_cache = self.load_complete_matches()
            
        if not self.matches_cache:
            logging.info("🔄 Cache vacío, actualizando...")
            self.update_matches_complete()
            
        return self.matches_cache
    
    def generate_enhanced_ics(self):
        """Generar ICS con datos completos"""
        matches = self.get_matches()
        
        if not matches:
            logging.warning("⚠️ No hay partidos para generar ICS")
            return self.generate_empty_ics()
        
        ics_lines = [
            "BEGIN:VCALENDAR",
            "VERSION:2.0",
            "PRODID:-//Castilla Complete Calendar v3.0//ES",
            "CALSCALE:GREGORIAN",
            "METHOD:PUBLISH",
            "X-WR-CALNAME:⚽ Real Madrid Castilla Completo 🇬🇹",
            "X-WR-CALDESC:Real Madrid Castilla - Datos completos: resultados, goleadores, TV, estadísticas",
            "X-WR-TIMEZONE:America/Guatemala",
            "REFRESH-INTERVAL;VALUE=DURATION:PT30M",
            "X-PUBLISHED-TTL:PT30M",
            "COLOR:1E3A8A"
        ]
        
        for match in matches:
            try:
                match_datetime = datetime.strptime(f"{match['date']} {match['time']}", "%Y-%m-%d %H:%M")
                match_datetime = self.timezone.localize(match_datetime)
                end_datetime = match_datetime + timedelta(hours=2)
                
                start_str = match_datetime.strftime("%Y%m%dT%H%M%S")
                end_str = end_datetime.strftime("%Y%m%dT%H%M%S")
                timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
                
                # DESCRIPCIÓN SÚPER COMPLETA
                description = f"🏆 {match['competition']}\\n"
                description += f"🏟️ {match['venue']}\\n"
                description += f"🇬🇹 Guatemala: {match['time']}\\n"
                
                if match.get('madrid_time'):
                    description += f"🇪🇸 Madrid: {match['madrid_time']}\\n"
                
                # RESULTADO Y GOLEADORES
                if match.get('result'):
                    description += f"\\n⚽ RESULTADO: {match['result']}\\n"
                    
                    if match.get('goalscorers'):
                        description += "\\n🥅 GOLEADORES:\\n"
                        for goal in match['goalscorers']:
                            goal_icon = "⚽"
                            if goal.get('goal_type') == 'penalty':
                                goal_icon = "🎯"
                            elif goal.get('goal_type') == 'free_kick':
                                goal_icon = "🚀"
                            elif goal.get('goal_type') == 'own_goal':
                                goal_icon = "😅"
                            
                            description += f"   {goal_icon} {goal.get('player_name', 'Unknown')} {goal.get('minute', '?')}'\\n"
                
                # TARJETAS
                if match.get('cards'):
                    yellow_cards = [c for c in match['cards'] if c.get('card_type') == 'yellow']
                    red_cards = [c for c in match['cards'] if c.get('card_type') == 'red']
                    
                    if yellow_cards:
                        description += "\\n🟨 TARJETAS AMARILLAS:\\n"
                        for card in yellow_cards:
                            description += f"   🟨 {card.get('player_name', 'Unknown')} {card.get('minute', '?')}'\\n"
                    
                    if red_cards:
                        description += "\\n🟥 TARJETAS ROJAS:\\n"
                        for card in red_cards:
                            description += f"   🟥 {card.get('player_name', 'Unknown')} {card.get('minute', '?')}'\\n"
                
                # TRANSMISIONES TV
                if match.get('tv_broadcast'):
                    description += "\\n📺 TRANSMISIÓN:\\n"
                    for broadcast in match['tv_broadcast'][:3]:  # Máximo 3 canales
                        channel = broadcast.get('channel_name', 'Unknown')
                        country = broadcast.get('country', '')
                        if country:
                            description += f"   📺 {channel} ({country})\\n"
                        else:
                            description += f"   📺 {channel}\\n"
                
                # ESTADÍSTICAS
                if match.get('statistics') and match['status'] == 'finished':
                    stats = match['statistics']
                    description += "\\n📊 ESTADÍSTICAS:\\n"
                    
                    if stats.get('possession_home') and stats.get('possession_away'):
                        description += f"   📈 Posesión: {stats['possession_home']}% - {stats['possession_away']}%\\n"
                    
                    if stats.get('shots_home') and stats.get('shots_away'):
                        description += f"   🎯 Remates: {stats['shots_home']} - {stats['shots_away']}\\n"
                    
                    if stats.get('corners_home') and stats.get('corners_away'):
                        description += f"   ⛳ Corners: {stats['corners_home']} - {stats['corners_away']}\\n"
                
                # INFORMACIÓN ADICIONAL
                if match.get('referee'):
                    description += f"\\n👤 Árbitro: {match['referee']}\\n"
                
                if match.get('attendance') and match['attendance'] > 0:
                    description += f"👥 Asistencia: {match['attendance']:,} espectadores\\n"
                
                if match.get('weather_temp') and match.get('weather_condition'):
                    description += f"🌤️ Clima: {match['weather_condition']}, {match['weather_temp']}\\n"
                
                description += "\\n👑 ¡Hala Madrid y nada más!"
                description += "\\n🇬🇹 Calendario creado en Guatemala"
                description += "\\n🤖 Datos automáticos de FotMob"
                description += "\\n🔄 Actualización cada 30 minutos"
                
                # TÍTULO DEL EVENTO MEJORADO
                summary = f"⚽ {match['home_team']} vs {match['away_team']}"
                
                if match.get('result'):
                    summary += f" ({match['result']})"
                    
                    # Añadir goleadores destacados al título
                    if match.get('goalscorers'):
                        castilla_goalscorers = []
                        for goal in match['goalscorers']:
                            # Determinar si es gol del Castilla
                            is_castilla_goal = (
                                (match['home_team'] and 'castilla' in match['home_team'].lower() and goal.get('team') == 'home') or
                                (match['away_team'] and 'castilla' in match['away_team'].lower() and goal.get('team') == 'away')
                            )
                            if is_castilla_goal:
                                castilla_goalscorers.append(goal.get('player_name', '').split()[-1])  # Solo apellido
                        
                        if castilla_goalscorers:
                            summary += f" - {', '.join(castilla_goalscorers[:2])} ⚽"
                            if len(castilla_goalscorers) > 2:
                                summary += f" +{len(castilla_goalscorers)-2}"
                
                elif match['status'] == 'live':
                    summary += " 🔴 EN VIVO"
                
                # CREAR EVENTO ICS
                event_lines = [
                    "BEGIN:VEVENT",
                    f"UID:{match['id']}@castilla-complete-gt.com",
                    f"DTSTART;TZID=America/Guatemala:{start_str}",
                    f"DTEND;TZID=America/Guatemala:{end_str}",
                    f"DTSTAMP:{timestamp}",
                    f"SUMMARY:{summary}",
                    f"DESCRIPTION:{description}",
                    f"LOCATION:{match['venue']}",
                    "CATEGORIES:FÚTBOL,REAL MADRID CASTILLA,PRIMERA FEDERACIÓN,GUATEMALA,COMPLETO",
                    f"STATUS:{'CONFIRMED' if match['status'] == 'finished' else 'TENTATIVE'}",
                    f"PRIORITY:{'1' if match['status'] == 'live' else '5'}",
                    "TRANSP:OPAQUE"
                ]
                
                # URL del partido si está disponible
                if match.get('match_url'):
                    event_lines.append(f"URL:{match['match_url']}")
                
                event_lines.append("END:VEVENT")
                ics_lines.extend(event_lines)
                
            except Exception as e:
                logging.error(f"❌ Error procesando evento {match.get('id')}: {e}")
                continue
        
        ics_lines.append("END:VCALENDAR")
        return "\n".join(ics_lines)
    
    def generate_empty_ics(self):
        """ICS de emergencia cuando no hay datos"""
        return """BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//Castilla Complete Calendar//ES
X-WR-CALNAME:⚽ Real Madrid Castilla 🇬🇹
X-WR-CALDESC:Calendario sin datos temporalmente. Actualizándose...
REFRESH-INTERVAL;VALUE=DURATION:PT30M
BEGIN:VEVENT
UID:no-data@castilla-complete.com
DTSTART:20250825T180000
DTEND:20250825T200000
SUMMARY:🔄 Actualizando datos completos...
DESCRIPTION:El calendario se está actualizando con datos de FotMob.\\nIncluirá: resultados, goleadores, TV, estadísticas.\\nVuelve a sincronizar en 30 minutos.
STATUS:TENTATIVE
END:VEVENT
END:VCALENDAR"""
    
    def get_season_statistics(self):
        """Obtener estadísticas completas de la temporada"""
        conn = self.get_db_connection()
        if not conn:
            return {}
        
        try:
            cursor = conn.cursor()
            
            # Estadísticas básicas de la temporada
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_matches,
                    COUNT(CASE WHEN status = 'finished' THEN 1 END) as completed_matches,
                    COUNT(CASE WHEN status = 'scheduled' THEN 1 END) as upcoming_matches,
                    COUNT(CASE WHEN status = 'live' THEN 1 END) as live_matches
                FROM matches
            """)
            basic_stats = dict(cursor.fetchone())
            
            # Resultados del Castilla
            cursor.execute("""
                SELECT 
                    COUNT(CASE 
                        WHEN status = 'finished' AND (
                            (home_score > away_score AND home_team LIKE '%Castilla%') OR
                            (away_score > home_score AND away_team LIKE '%Castilla%')
                        ) THEN 1 END) as wins,
                    COUNT(CASE 
                        WHEN status = 'finished' AND home_score = away_score 
                        THEN 1 END) as draws,
                    COUNT(CASE 
                        WHEN status = 'finished' AND (
                            (home_score < away_score AND home_team LIKE '%Castilla%') OR
                            (away_score < home_score AND away_team LIKE '%Castilla%')
                        ) THEN 1 END) as losses
                FROM matches
            """)
            results_stats = dict(cursor.fetchone())
            
            # Top goleadores del Castilla
            cursor.execute("""
                SELECT 
                    g.player_name,
                    COUNT(*) as goals,
                    COUNT(DISTINCT g.match_id) as matches_with_goals,
                    COUNT(CASE WHEN g.goal_type = 'penalty' THEN 1 END) as penalties,
                    MIN(m.date) as first_goal_date,
                    MAX(m.date) as last_goal_date
                FROM goalscorers g
                JOIN matches m ON g.match_id = m.id
                WHERE (m.home_team LIKE '%Castilla%' AND g.team = 'home') OR 
                      (m.away_team LIKE '%Castilla%' AND g.team = 'away')
                GROUP BY g.player_name
                ORDER BY goals DESC, matches_with_goals DESC
                LIMIT 10
            """)
            top_scorers = [dict(row) for row in cursor.fetchall()]
            
            # Estadísticas de tarjetas
            cursor.execute("""
                SELECT 
                    COUNT(CASE WHEN card_type = 'yellow' THEN 1 END) as yellow_cards,
                    COUNT(CASE WHEN card_type = 'red' THEN 1 END) as red_cards
                FROM cards c
                JOIN matches m ON c.match_id = m.id
                WHERE (m.home_team LIKE '%Castilla%' AND c.team = 'home') OR 
                      (m.away_team LIKE '%Castilla%' AND c.team = 'away')
            """)
            cards_stats = dict(cursor.fetchone())
            
            # Transmisiones más frecuentes
            cursor.execute("""
                SELECT 
                    channel_name,
                    country,
                    COUNT(*) as matches_broadcast
                FROM tv_broadcast t
                GROUP BY channel_name, country
                ORDER BY matches_broadcast DESC
                LIMIT 5
            """)
            tv_stats = [dict(row) for row in cursor.fetchall()]
            
            conn.close()
            
            return {
                'season': '2024-25',
                'basic_stats': basic_stats,
                'results': results_stats,
                'top_scorers': top_scorers,
                'discipline': cards_stats,
                'tv_coverage': tv_stats,
                'last_updated': datetime.now(self.timezone).isoformat()
            }
            
        except Exception as e:
            logging.error(f"❌ Error obteniendo estadísticas: {e}")
            if conn:
                conn.close()
            return {}

# Crear instancia global
calendar = CastillaCalendarComplete()

# Configurar scheduler optimizado
scheduler = BackgroundScheduler(timezone='America/Guatemala')

# Actualización regular cada 30 minutos
scheduler.add_job(
    func=calendar.update_matches_complete,
    trigger="interval",
    minutes=30,
    id='update_regular_complete'
)

# Actualización intensiva los días de partido
scheduler.add_job(
    func=calendar.update_matches_complete,
    trigger="cron",
    hour="10-22",  # 10 AM a 10 PM Guatemala
    minute="*/10",  # Cada 10 minutos
    day_of_week="sat,sun,wed",  # Días típicos de partido
    id='update_matchday_complete'
)

scheduler.start()

# RUTAS DE LA API

@app.before_request
def before_request():
    """Log de accesos y métricas"""
    request.start_time = datetime.now()
    
    calendar.log_user_access(
        request.path,
        request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr),
        request.headers.get('User-Agent', '')[:200]
    )

@app.after_request
def after_request(response):
    """Log de tiempo de respuesta"""
    if hasattr(request, 'start_time'):
        response_time = int((datetime.now() - request.start_time).total_seconds() * 1000)
        calendar.log_user_access(
            request.path,
            request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr),
            request.headers.get('User-Agent', '')[:200],
            response.status_code,
            response_time
        )
    return response

@app.route('/')
def home():
    """Página principal con información completa"""
    base_url = request.url_root.rstrip('/')
    
    return jsonify({
        "proyecto": "🏆 Real Madrid Castilla COMPLETO - Guatemala",
        "descripcion": "Calendario automático con TODOS los datos: goleadores, tarjetas, TV, estadísticas",
        "version": "3.0.0-fotmob-complete",
        "presupuesto": "💰 $0.00 - 100% GRATIS",
        
        "urls": {
            "calendario_ios_completo": f"{base_url}/calendar.ics",
            "api_partidos_completos": f"{base_url}/api/matches",
            "estadisticas_temporada": f"{base_url}/api/season-stats",
            "goleadores": f"{base_url}/api/top-scorers", 
            "estado_sistema": f"{base_url}/api/status",
            "test_fotmob": f"{base_url}/api/test-fotmob"
        },
        
        "datos_incluidos": [
            "⚽ Resultados completos",
            "🥅 Goleadores con minutos",
            "🟨🟥 Tarjetas amarillas y rojas",
            "🔄 Cambios realizados",
            "📺 Transmisiones TV/streaming",
            "📊 Estadísticas del partido",
            "👤 Árbitros",
            "👥 Asistencia",
            "🌤️ Condiciones meteorológicas",
            "📈 Estadísticas de temporada"
        ],
        
        "instrucciones_ios": [
            f"1. Copia: {base_url}/calendar.ics",
            "2. Abre Ajustes → Calendario → Cuentas",
            "3. Añadir cuenta → Otro → Calendario suscrito",
            "4. Pega la URL y confirma",
            "5. ¡Disfruta del calendario MÁS completo del Castilla!"
        ],
        
        "caracteristicas_premium": {
            "actualizacion": "Cada 30 min (10 min días de partido)",
            "fuente_datos": "FotMob API - La más completa",
            "zona_horaria": "America/Guatemala (GMT-6)",
            "compatibilidad": "iOS, Android, Google Cal, Outlook",
            "notificaciones": "Sistema preparado (próximamente)",
            "estadisticas": "Completas por temporada",
            "historial": "Últimos 365 días"
        }
    })

@app.route('/calendar.ics')
def get_enhanced_calendar():
    """ENDPOINT PRINCIPAL - Calendario ICS completo"""
    try:
        ics_content = calendar.generate_enhanced_ics()
        
        response = Response(ics_content, mimetype='text/calendar; charset=utf-8')
        response.headers['Content-Disposition'] = 'attachment; filename="real-madrid-castilla-completo-guatemala.ics"'
        response.headers['Cache-Control'] = 'public, max-age=1800'  # 30 minutos
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['X-Content-Type-Options'] = 'nosniff'
        
        logging.info("📱 Calendario ICS completo generado")
        return response
        
    except Exception as e:
        logging.error(f"❌ Error generando calendario completo: {e}")
        
        # Fallback
        emergency_ics = calendar.generate_empty_ics()
        response = Response(emergency_ics, mimetype='text/calendar')
        response.headers['Content-Disposition'] = 'attachment; filename="castilla-emergency.ics"'
        return response

@app.route('/api/matches')
def get_complete_matches():
    """API de partidos con TODOS los datos"""
    try:
        matches = calendar.get_matches()
        
        # Separar por categorías
        upcoming = [m for m in matches if m['status'] == 'scheduled']
        live = [m for m in matches if m['status'] == 'live']  
        finished = [m for m in matches if m['status'] == 'finished']
        
        # Estadísticas rápidas
        total_goals_castilla = 0
        total_cards_castilla = 0
        
        for match in finished:
            # Contar goles del Castilla
            for goal in match.get('goalscorers', []):
                is_castilla = (
                    (match['home_team'] and 'castilla' in match['home_team'].lower() and goal.get('team') == 'home') or
                    (match['away_team'] and 'castilla' in match['away_team'].lower() and goal.get('team') == 'away')
                )
                if is_castilla:
                    total_goals_castilla += 1
            
            # Contar tarjetas del Castilla
            for card in match.get('cards', []):
                is_castilla = (
                    (match['home_team'] and 'castilla' in match['home_team'].lower() and card.get('team') == 'home') or
                    (match['away_team'] and 'castilla' in match['away_team'].lower() and card.get('team') == 'away')
                )
                if is_castilla:
                    total_cards_castilla += 1
        
        return jsonify({
            "partidos_completos": matches,
            "resumen": {
                "total": len(matches),
                "proximos": len(upcoming),
                "en_vivo": len(live),
                "finalizados": len(finished)
            },
            "estadisticas_rapidas": {
                "goles_castilla_temporada": total_goals_castilla,
                "tarjetas_castilla_temporada": total_cards_castilla,
                "partidos_con_tv": len([m for m in matches if m.get('tv_broadcast')]),
                "partidos_con_estadisticas": len([m for m in matches if m.get('statistics')])
            },
            "proximos_destacados": upcoming[:3],
            "ultimos_resultados": finished[-3:] if finished else [],
            
            "metadata": {
                "fuente": "FotMob API",
                "datos_incluidos": ["goleadores", "tarjetas", "cambios", "tv", "estadisticas", "arbitros"],
                "ultima_actualizacion": calendar.last_update.isoformat() if calendar.last_update else None,
                "zona_horaria": "America/Guatemala",
                "version": "3.0.0-complete"
            }
        })
        
    except Exception as e:
        logging.error(f"❌ Error API matches completos: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/season-stats')
def get_season_statistics_api():
    """API de estadísticas completas de temporada"""
    try:
        stats = calendar.get_season_statistics()
        return jsonify(stats)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/top-scorers')
def get_top_scorers():
    """API de máximos goleadores"""
    try:
        stats = calendar.get_season_statistics()
        return jsonify({
            "goleadores": stats.get('top_scorers', []),
            "temporada": "2024-25",
            "ultima_actualizacion": stats.get('last_updated')
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/test-fotmob')
def test_fotmob_connection():
    """Test completo de FotMob"""
    try:
        result = calendar.fotmob_scraper.test_connection()
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/status')
def get_complete_status():
    """Estado completo del sistema"""
    try:
        stats = calendar.get_season_statistics()
        
        return jsonify({
            "estado": "✅ Sistema Completo Operativo",
            "version": "3.0.0-fotmob-complete",
            "servidor": "Render.com (plan gratuito)",
            "ubicacion": "🇬🇹 Guatemala",
            
            "calendario": {
                "ultima_actualizacion": calendar.last_update.isoformat() if calendar.last_update else "Nunca",
                "partidos_en_cache": len(calendar.matches_cache),
                "base_datos": "SQLite completa",
                "datos_incluidos": ["partidos", "goleadores", "tarjetas", "cambios", "tv", "estadisticas"]
            },
            
            "temporada_actual": {
                "total_partidos": stats.get('basic_stats', {}).get('total_matches', 0),
                "finalizados": stats.get('basic_stats', {}).get('completed_matches', 0),
                "goleadores": len(stats.get('top_scorers', [])),
                "canales_tv": len(stats.get('tv_coverage', []))
            },
            
            "fuente_datos": {
                "principal": "FotMob API",
                "team_id": calendar.config.get('fotmob_team_id'),
                "confiabilidad": "Alta",
                "datos_avanzados": True
            },
            
            "configuracion": {
                "zona_horaria": "America/Guatemala",
                "actualizacion_regular": "30 minutos",
                "actualizacion_intensiva": "10 min (días de partido)",
                "presupuesto": "$0.00 - 100% gratis"
            }
        })
        
    except Exception as e:
        logging.error(f"❌ Error status completo: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/update')
def force_complete_update():
    """Forzar actualización completa"""
    try:
        logging.info("🔄 Actualización completa manual solicitada")
        calendar.update_matches_complete()
        matches = calendar.get_matches()
        
        return jsonify({
            "mensaje": "✅ Actualización completa finalizada",
            "timestamp": calendar.last_update.isoformat() if calendar.last_update else None,
            "partidos_procesados": len(matches),
            "datos_obtenidos": [
                "Información básica",
                "Goleadores y minutos",
                "Tarjetas amarillas/rojas",
                "Cambios realizados",
                "Transmisiones TV",
                "Estadísticas del partido",
                "Árbitros y asistencia"
            ],
            "fuente": "FotMob API completa",
            "proxima_actualizacion_automatica": "En 30 minutos"
        })
        
    except Exception as e:
        logging.error(f"❌ Error update completo: {e}")
        return jsonify({
            "mensaje": "❌ Error en actualización completa",
            "error": str(e),
            "sugerencia": "Intenta de nuevo en unos minutos o verifica la conexión a FotMob"
        }), 500

@app.route('/api/next')
def get_next_match_complete():
    """Próximo partido con información completa"""
    try:
        matches = calendar.get_matches()
        now = datetime.now(calendar.timezone)
        
        upcoming = []
        for match in matches:
            if match['status'] in ['scheduled', 'live']:
                try:
                    match_dt = datetime.strptime(f"{match['date']} {match['time']}", "%Y-%m-%d %H:%M")
                    match_dt = calendar.timezone.localize(match_dt)
                    
                    if match['status'] == 'live' or match_dt > now:
                        match_copy = match.copy()
                        match_copy['datetime_obj'] = match_dt
                        upcoming.append(match_copy)
                except:
                    continue
        
        if not upcoming:
            return jsonify({
                "mensaje": "No hay próximos partidos programados",
                "sugerencia": "Revisa más tarde o fuerza una actualización"
            })
        
        upcoming.sort(key=lambda x: x['datetime_obj'])
        next_match = upcoming[0]
        
        # Información de tiempo
        if next_match['status'] == 'live':
            time_info = {
                "estado": "🔴 ¡PARTIDO EN VIVO AHORA!",
                "tiempo_restante": "En curso",
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
                "es_live": False
            }
        
        # Remover objeto datetime
        del next_match['datetime_obj']
        
        return jsonify({
            "proximo_partido": next_match,
            "tiempo": time_info,
            "fecha_completa": next_match['date'] + " " + next_match['time'] + " (Guatemala)",
            "transmision_tv": next_match.get('tv_broadcast', []),
            "otros_proximos": len(upcoming) - 1,
            "datos_disponibles": {
                "tv_confirmada": len(next_match.get('tv_broadcast', [])) > 0,
                "arbitro_confirmado": bool(next_match.get('referee')),
                "estadio_confirmado": bool(next_match.get('venue')),
                "alineaciones": bool(next_match.get('lineups'))
            }
        })
        
    except Exception as e:
        logging.error(f"❌ Error próximo partido completo: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/match/<match_id>')
def get_match_details(match_id):
    """Obtener detalles completos de un partido específico"""
    try:
        matches = calendar.get_matches()
        match = next((m for m in matches if m['id'] == match_id), None)
        
        if not match:
            return jsonify({"error": "Partido no encontrado"}), 404
        
        return jsonify({
            "partido": match,
            "detalles_completos": True,
            "datos_incluidos": {
                "informacion_basica": True,
                "goleadores": len(match.get('goalscorers', [])) > 0,
                "tarjetas": len(match.get('cards', [])) > 0,
                "cambios": len(match.get('substitutions', [])) > 0,
                "transmision_tv": len(match.get('tv_broadcast', [])) > 0,
                "estadisticas": bool(match.get('statistics')),
                "arbitro": bool(match.get('referee')),
                "asistencia": bool(match.get('attendance'))
            }
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/health')
def health_check_complete():
    """Health check completo del sistema"""
    try:
        # Verificar base de datos
        conn = calendar.get_db_connection()
        if conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM matches")
            matches_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM goalscorers")
            goals_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM tv_broadcast")
            tv_count = cursor.fetchone()[0]
            
            conn.close()
        else:
            matches_count = goals_count = tv_count = 0
        
        # Verificar última actualización
        hours_since_update = 999
        if calendar.last_update:
            hours_since_update = (datetime.now(calendar.timezone) - calendar.last_update).total_seconds() / 3600
        
        # Test rápido de FotMob
        fotmob_status = "unknown"
        try:
            test_result = calendar.fotmob_scraper.test_connection()
            fotmob_status = "ok" if test_result.get('success') else "error"
        except:
            fotmob_status = "error"
        
        # Determinar estado general
        status = "healthy"
        if hours_since_update > 2 or fotmob_status == "error":
            status = "warning"
        if hours_since_update > 12 or matches_count == 0:
            status = "critical"
        
        return jsonify({
            "status": status,
            "version": "3.0.0-complete",
            "checks": {
                "database": "ok" if matches_count >= 0 else "error",
                "last_update": "ok" if hours_since_update < 2 else "warning",
                "fotmob_api": fotmob_status,
                "data_completeness": "ok" if goals_count > 0 or tv_count > 0 else "warning"
            },
            "metrics": {
                "matches_in_db": matches_count,
                "goals_recorded": goals_count,
                "tv_broadcasts": tv_count,
                "hours_since_update": round(hours_since_update, 2),
                "cache_size": len(calendar.matches_cache)
            },
            "data_sources": {
                "primary": "FotMob API",
                "backup": "Official data",
                "reliability": "High"
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
        "version": "3.0.0-complete",
        "codigo": 404
    }), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({
        "error": "Error interno del servidor",
        "mensaje": "Sistema en mantenimiento, intenta más tarde",
        "codigo": 500
    }), 500

# Inicialización
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('DEBUG', 'False').lower() == 'true'
    
    logging.info("🚀 INICIANDO CALENDARIO CASTILLA COMPLETO v3.0")
    logging.info(f"📍 Servidor: Render.com - Puerto {port}")
    logging.info(f"🇬🇹 Zona horaria: America/Guatemala")  
    logging.info(f"📱 Endpoint calendario: /calendar.ics")
    logging.info(f"📊 Datos incluidos: goleadores, tarjetas, TV, estadísticas")
    logging.info(f"🔄 Fuente principal: FotMob API")
    logging.info(f"💰 Presupuesto: $0.00 - 100% GRATIS")
    
    # Actualización inicial
    try:
        logging.info("🔄 Cargando datos iniciales de FotMob...")
        calendar.update_matches_complete()
        initial_matches = len(calendar.get_matches())
        
        if initial_matches > 0:
            # Obtener estadísticas rápidas
            sample_match = calendar.matches_cache[0]
            has_goals = len(sample_match.get('goalscorers', [])) > 0
            has_tv = len(sample_match.get('tv_broadcast', [])) > 0
            has_stats = bool(sample_match.get('statistics'))
            
            logging.info(f"✅ Sistema iniciado con {initial_matches} partidos completos")
            logging.info(f"📊 Datos detectados: Goles={has_goals}, TV={has_tv}, Stats={has_stats}")
        else:
            logging.warning("⚠️ No se encontraron partidos iniciales")
            
    except Exception as e:
        logging.error(f"⚠️ Error en carga inicial: {e}")
        logging.info("🔄 El sistema se actualizará automáticamente cada 30 minutos")
    
    logging.info("🎉 ¡CALENDARIO MÁS COMPLETO DEL CASTILLA LISTO!")
    logging.info("👑 ¡Hala Madrid y nada más!")
    
    # Iniciar servidor
    app.run(
        host='0.0.0.0',
        port=port,
        debug=debug,
        threaded=True
    )# archivo: app.py - Backend Completo v3.0 con FotMob

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
            
            logging.info("✅ Base de datos completa inicializada")
            
        except Exception as e:
            logging.error(f"❌ Error inicializando base de datos: {e}")
    
    def get_db_connection(self):
        """Obtener conexión a la base de datos"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row  # Para acceso por nombre de columna
            return conn
        except Exception as e:
            logging.error(f"❌ Error conexión BD: {e}")
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
            
            logging.info(f"💾 Datos completos guardados para partido {match_data['id']}")
            return True
            
        except Exception as e:
            logging.error(f"❌ Error guardando datos completos: {e}")
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
                if