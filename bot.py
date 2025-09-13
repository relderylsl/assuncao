import os
import time
import aiohttp
import asyncio
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from telegram import Bot
import re
import json
from typing import Dict, List, Optional, Tuple
import logging

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constantes
BOT_TOKEN = "6569266928:AAHm7pOJVsd3WKzJEgdVDez4ZYdCAlRoYO8"
CHAT_ID = "-1001981134607"
OLD_LIVE_API_URL = "https://caveira-proxy.onrender.com/api/matches/live"
NEW_LIVE_API_URL = "https://esoccer.dev3.caveira.tips/v1/esoccer/inplay"
ENDED_API_URL = "https://api-v2.green365.com.br/api/v2/sport-events"
H2H_API_URL = "https://caveira-proxy.onrender.com/api/v1/historico/confronto/{player1}/{player2}?page=1&limit=10"
AUTH_TOKEN = "Bearer oat_MTEyNTEx.aS1EdDJaNWw2dUkzREpqOGI3Mmo1eHdVeUZOZmZyQmZkclR2bE1SODM0ODg3NzEzODQ"

# Timezone de Manaus
MANAUS_TZ = timezone(timedelta(hours=-4))

# Cache global para H2H
h2h_cache = {}
h2h_cache_expiry = {}
CACHE_DURATION = 3600  # 1 hora em segundos

# Dados globais
sent_tips = []
last_summary = None
last_league_summary = None
league_stats = {}
last_league_message_id = None

class OptimizedSession:
    """Sessão HTTP otimizada com reutilização de conexões"""
    def __init__(self):
        self.session = None
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
            "Connection": "keep-alive"
        }
    
    async def __aenter__(self):
        connector = aiohttp.TCPConnector(
            limit=100,  # Limite total de conexões
            limit_per_host=30,  # Limite por host
            ttl_dns_cache=300,  # Cache DNS por 5 minutos
            use_dns_cache=True,
            keepalive_timeout=60
        )
        timeout = aiohttp.ClientTimeout(total=10, connect=5)
        self.session = aiohttp.ClientSession(
            connector=connector, 
            timeout=timeout, 
            headers=self.headers
        )
        return self.session
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

async def fetch_with_retry(session: aiohttp.ClientSession, url: str, 
                          headers: Dict = None, json_data: Dict = None, 
                          params: Dict = None, max_retries: int = 2) -> Optional[Dict]:
    """Função genérica para fazer requisições HTTP com retry"""
    for attempt in range(max_retries + 1):
        try:
            if json_data:
                async with session.post(url, headers=headers, json=json_data, params=params) as response:
                    if response.status == 200:
                        return await response.json()
                    logger.warning(f"HTTP {response.status} para {url}")
            else:
                async with session.get(url, headers=headers, params=params) as response:
                    if response.status == 200:
                        return await response.json()
                    logger.warning(f"HTTP {response.status} para {url}")
        except asyncio.TimeoutError:
            logger.warning(f"Timeout na tentativa {attempt + 1} para {url}")
        except Exception as e:
            logger.error(f"Erro na tentativa {attempt + 1} para {url}: {e}")
        
        if attempt < max_retries:
            await asyncio.sleep(1 * (attempt + 1))  # Backoff exponencial
    
    return None

async def fetch_all_data(session: aiohttp.ClientSession) -> Tuple[List, Dict, List]:
    """Busca todos os dados necessários de forma paralela"""
    tasks = [
        fetch_old_live_matches(session),
        fetch_bet365_ids(session),
        fetch_ended_matches(session)
    ]
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    old_matches = results[0] if not isinstance(results[0], Exception) else []
    bet365_dict = results[1] if not isinstance(results[1], Exception) else {}
    ended_matches = results[2] if not isinstance(results[2], Exception) else []
    
    return old_matches, bet365_dict, ended_matches

async def fetch_old_live_matches(session: aiohttp.ClientSession) -> List:
    """Busca partidas ao vivo da API antiga"""
    try:
        data = await fetch_with_retry(session, OLD_LIVE_API_URL)
        if not data:
            return []
        
        matches = data.get('data', [])
        valid_matches = [
            m for m in matches 
            if isinstance(m, dict) and all(m.get(key) for key in ['id', 'league', 'home', 'away'])
        ]
        logger.info(f"{len(matches)} partidas da API antiga; {len(valid_matches)} válidas")
        return valid_matches
    except Exception as e:
        logger.error(f"fetch_old_live_matches: {e}")
        return []

async def fetch_bet365_ids(session: aiohttp.ClientSession) -> Dict:
    """Busca IDs da bet365"""
    try:
        headers = {
            "Authorization": AUTH_TOKEN,
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
        data = await fetch_with_retry(session, NEW_LIVE_API_URL, headers=headers, json_data={})
        if not data:
            return {}
        
        matches = data.get('data', [])
        logger.info(f"{len(matches)} partidas da API nova (links)")
        
        mapping = {}
        for m in matches:
            try:
                key = (m['player_home_name'].lower(), m['player_away_name'].lower())
                mapping[key] = m.get('bet365_ev_id')
            except KeyError:
                continue
        
        return mapping
    except Exception as e:
        logger.error(f"fetch_bet365_ids: {e}")
        return {}

async def fetch_ended_matches(session: aiohttp.ClientSession) -> List:
    """Busca partidas finalizadas"""
    try:
        headers = {
            "Authorization": AUTH_TOKEN,
            "Referer": "https://green365.com.br/",
            "Origin": "https://green365.com.br"
        }
        params = {"page": 1, "limit": 200, "sport": "esoccer", "status": "ended"}
        
        data = await fetch_with_retry(session, ENDED_API_URL, headers=headers, params=params)
        if not data:
            return []
        
        items = data.get('items', [])
        logger.info(f"Finalizadas retornadas: {len(items)}")
        return items
    except Exception as e:
        logger.error(f"fetch_ended_matches: {e}")
        return []

async def fetch_h2h_data_cached(session: aiohttp.ClientSession, player1: str, player2: str) -> Optional[Dict]:
    """Busca dados H2H com cache"""
    cache_key = f"{player1.lower()}_{player2.lower()}"
    current_time = time.time()
    
    # Verifica cache
    if (cache_key in h2h_cache and 
        cache_key in h2h_cache_expiry and 
        current_time < h2h_cache_expiry[cache_key]):
        return h2h_cache[cache_key]
    
    try:
        url = H2H_API_URL.format(player1=player1, player2=player2)
        data = await fetch_with_retry(session, url)
        
        # Armazena no cache
        if data:
            h2h_cache[cache_key] = data
            h2h_cache_expiry[cache_key] = current_time + CACHE_DURATION
        
        return data
    except Exception as e:
        logger.error(f"fetch_h2h_data {player1} vs {player2}: {e}")
        return None

def get_match_time_in_minutes(match: Dict) -> float:
    """Calcula o tempo da partida em minutos"""
    timer = match.get('timer', {})
    if not timer:
        return 0.0
    
    tm = timer.get('tm', 0)
    ts = timer.get('ts', 0)
    
    try:
        return float(tm) + float(ts) / 60.0
    except (TypeError, ValueError):
        return float(tm) if isinstance(tm, (int, float)) else 0.0

def is_first_half(match: Dict, league_name: str) -> bool:
    """Verifica se é o primeiro tempo"""
    minutes = get_match_time_in_minutes(match)
    
    thresholds = {
        "8 mins play": 4,
        "6 mins play": 3,
        "12 mins play": 6,
        "10 mins play": 6
    }
    
    for key, threshold in thresholds.items():
        if key in league_name:
            return minutes < threshold
    
    return False

def calculate_dangerous_attacks_rate(match: Dict, current_time: float) -> float:
    """Calcula a taxa de ataques perigosos"""
    if current_time <= 0:
        return 0.0
    
    stats = match.get('stats')
    if not isinstance(stats, dict):
        return 0.0
    
    da = stats.get('dangerous_attacks', [0, 0])
    try:
        total_da = int(da[0]) + int(da[1])
        return total_da / current_time
    except (TypeError, ValueError, ZeroDivisionError):
        return 0.0

def calculate_h2h_metrics(h2h_data: Dict, league_name: str) -> Optional[Dict]:
    """Calcula métricas H2H de forma otimizada"""
    if not h2h_data or 'matches' not in h2h_data:
        return None

    matches = h2h_data['matches']
    if not matches:
        return None

    # Inicialização de contadores
    counters = {
        'over_0_5_ht': 0, 'over_1_5_ht': 0, 'over_2_5_ht': 0, 'btts_ht': 0,
        'over_2_5_ft': 0, 'over_3_5_ft': 0, 'over_4_5_ft': 0, 'btts_ft': 0,
        'player1_wins': 0, 'player2_wins': 0,
        'player1_total_goals': 0, 'player2_total_goals': 0,
        'player1_total_ht_goals': 0, 'player2_total_ht_goals': 0,
        'total_ht_goals': 0, 'total_ft_goals': 0
    }

    # Campos possíveis para score
    score_fields = [
        ('final_score_home', 'final_score_away'),
        ('score_home', 'score_away'),
        ('home_score', 'away_score'),
        ('ft_score_home', 'ft_score_away'),
        ('home_goals', 'away_goals')
    ]

    for match in matches:
        # Processar primeiro tempo
        ht_home = match.get('halftime_score_home', 0) or 0
        ht_away = match.get('halftime_score_away', 0) or 0
        ht_goals = ht_home + ht_away
        
        counters['total_ht_goals'] += ht_goals
        counters['player1_total_ht_goals'] += ht_home
        counters['player2_total_ht_goals'] += ht_away
        
        if ht_goals > 0: counters['over_0_5_ht'] += 1
        if ht_goals > 1: counters['over_1_5_ht'] += 1
        if ht_goals > 2: counters['over_2_5_ht'] += 1
        if ht_home > 0 and ht_away > 0: counters['btts_ht'] += 1

        # Processar tempo final
        ft_home = ft_away = None
        
        # Tentar diferentes campos para o score
        for home_field, away_field in score_fields:
            if ft_home is None:
                ft_home = match.get(home_field)
                ft_away = match.get(away_field)
        
        # Tentar objetos aninhados
        if ft_home is None:
            for obj_key in ['score', 'final_score', 'result']:
                score_obj = match.get(obj_key)
                if isinstance(score_obj, dict):
                    ft_home = score_obj.get('home') or score_obj.get('home_score')
                    ft_away = score_obj.get('away') or score_obj.get('away_score')
                    if ft_home is not None:
                        break
        
        # Tentar string formatada
        if ft_home is None:
            score_str = match.get('score')
            if isinstance(score_str, str) and '-' in score_str:
                try:
                    parts = score_str.split('-')
                    if len(parts) == 2:
                        ft_home = int(parts[0].strip())
                        ft_away = int(parts[1].strip())
                except (ValueError, IndexError):
                    pass
        
        ft_home = int(ft_home) if ft_home is not None else 0
        ft_away = int(ft_away) if ft_away is not None else 0
        ft_goals = ft_home + ft_away

        counters['total_ft_goals'] += ft_goals
        counters['player1_total_goals'] += ft_home
        counters['player2_total_goals'] += ft_away

        if ft_goals > 2: counters['over_2_5_ft'] += 1
        if ft_goals > 3: counters['over_3_5_ft'] += 1
        if ft_goals > 4: counters['over_4_5_ft'] += 1
        if ft_home > 0 and ft_away > 0: counters['btts_ft'] += 1

        if ft_home > ft_away:
            counters['player1_wins'] += 1
        elif ft_away > ft_home:
            counters['player2_wins'] += 1

    total = len(matches)
    
    # Calcular métricas finais
    return {
        'player1_win_percentage': (counters['player1_wins'] / total) * 100.0,
        'player2_win_percentage': (counters['player2_wins'] / total) * 100.0,
        'player1_avg_goals': counters['player1_total_goals'] / total,
        'player2_avg_goals': counters['player2_total_goals'] / total,
        'player1_avg_ht_goals': counters['player1_total_ht_goals'] / total,
        'player2_avg_ht_goals': counters['player2_total_ht_goals'] / total,
        'avg_ht_goals': counters['total_ht_goals'] / total,
        'avg_ft_goals': counters['total_ft_goals'] / total,
        'over_0_5_ht_percentage': (counters['over_0_5_ht'] / total) * 100.0,
        'over_1_5_ht_percentage': (counters['over_1_5_ht'] / total) * 100.0,
        'over_2_5_ht_percentage': (counters['over_2_5_ht'] / total) * 100.0,
        'over_2_5_ft_percentage': (counters['over_2_5_ft'] / total) * 100.0,
        'over_3_5_ft_percentage': (counters['over_3_5_ft'] / total) * 100.0,
        'over_4_5_ft_percentage': (counters['over_4_5_ft'] / total) * 100.0,
        'btts_ht_percentage': (counters['btts_ht'] / total) * 100.0,
        'btts_ft_percentage': (counters['btts_ft'] / total) * 100.0
    }

def format_message(match: Dict, h2h_metrics: Dict, strategy: str, bet365_ev_id: str) -> str:
    """Formata mensagem para envio"""
    league = match['league']['name']
    home = match['home']['name']
    away = match['away']['name']
    player1 = home.split('(')[-1].rstrip(')') if '(' in home else home
    player2 = away.split('(')[-1].rstrip(')') if '(' in away else away
    timer = match.get('timer', {})
    minutes = timer.get('tm', 0)
    seconds = timer.get('ts', 0)
    game_time = f"{minutes}:{int(seconds):02d}"
    ss = match.get('ss', '0-0')
    
    msg = f"\n\n<b>🏆 {league}</b>\n\n<b>🎯 {strategy}</b>\n\n⏳ Tempo: {game_time}\n\n"
    msg += f"🎮 {player1} vs {player2}\n"
    msg += f"⚽ Placar: {ss}\n"
    
    if h2h_metrics:
        msg += (
            f"🏅 <i>{h2h_metrics.get('player1_win_percentage', 0):.0f}% vs "
            f"{h2h_metrics.get('player2_win_percentage', 0):.0f}%</i>\n\n"
            f"💠 Média gols: <i>{h2h_metrics.get('player1_avg_goals', 0):.2f}</i> vs <i>{h2h_metrics.get('player2_avg_goals', 0):.2f}</i>\n\n"
            f"<b>📊 H2H HT (últimos 10 jogos):</b>\n\n"
            f"⚽ +0.5: <i>{h2h_metrics.get('over_0_5_ht_percentage', 0):.0f}%</i> | +1.5: <i>{h2h_metrics.get('over_1_5_ht_percentage', 0):.0f}%</i> | +2.5: <i>{h2h_metrics.get('over_2_5_ht_percentage', 0):.0f}%</i>\n\n"
            f"⚽ BTTS HT: <i>{h2h_metrics.get('btts_ht_percentage', 0):.0f}%</i>\n"
        )
    else:
        msg += "📊 H2H: <i>não disponível</i>"
    
    if bet365_ev_id:
        msg += f"\n\n🌐 <a href='https://www.bet365.bet.br/#/IP/EV{bet365_ev_id}'>🔗Bet365</a>\n\n"
    
    return msg

async def send_message(bot: Bot, match_id: int, message: str, sent_matches: set, strategy: str):
    """Envia mensagem via bot"""
    if match_id in sent_matches:
        return
    
    try:
        message_obj = await bot.send_message(
            chat_id=CHAT_ID, 
            text=message, 
            parse_mode="HTML", 
            disable_web_page_preview=True
        )
        sent_matches.add(match_id)
        logger.info(f"Enviado match_id={match_id} ({strategy})")
        
        sent_tips.append({
            'match_id': int(match_id),
            'strategy': strategy,
            'sent_time': datetime.now(MANAUS_TZ),
            'status': 'pending',
            'message_id': message_obj.message_id,
            'message_text': message
        })
    except Exception as e:
        logger.error(f"send_message {match_id}: {e}")

def format_thermometer(perc: float) -> str:
    """Formata termômetro visual"""
    bars = 10
    green_count = round(perc / 10)
    bar = '🟩' * green_count + '🟥' * (bars - green_count)
    return f"{bar} {perc:.0f}%  {(100 - perc):.0f}%"

async def process_match_strategies(session: aiohttp.ClientSession, match: Dict, 
                                 bet365_dict: Dict, sent_matches: set, bot: Bot):
    """Processa estratégias para uma partida específica"""
    try:
        match_id = match.get('id', 'unknown')
        league_name = (match.get('league') or {}).get('name', 'Desconhecida')
        home = (match.get('home') or {}).get('name', 'Desconhecido')
        away = (match.get('away') or {}).get('name', 'Desconhecido')
        
        # Placar atual
        ss = match.get('ss')
        if ss:
            try:
                home_goals, away_goals = map(int, ss.split('-'))
            except Exception:
                home_goals, away_goals = 0, 0
        else:
            home_goals, away_goals = 0, 0
        
        current_time = get_match_time_in_minutes(match)

        # Jogadores
        player1 = home.split('(')[-1].rstrip(')') if '(' in home else home
        player2 = away.split('(')[-1].rstrip(')') if '(' in away else away
        key = (player1.lower(), player2.lower())
        bet365_ev_id = bet365_dict.get(key)
        
        # H2H
        h2h_data = await fetch_h2h_data_cached(session, player1, player2)
        h2h_metrics = calculate_h2h_metrics(h2h_data, league_name)

        if not h2h_metrics:
            return

        # Aplicar estratégias baseadas na liga
        await apply_league_strategies(
            match, h2h_metrics, league_name, current_time, 
            home_goals, away_goals, player1, player2,
            bet365_ev_id, sent_matches, bot
        )
        
    except Exception as e:
        logger.error(f"process_match_strategies: {e}")

async def apply_league_strategies(match, h2h_metrics, league_name, current_time, 
                                home_goals, away_goals, player1, player2,
                                bet365_ev_id, sent_matches, bot):
    """Aplica estratégias específicas por liga"""
    match_id = match.get('id')
    
    # Estratégias para ligas de 8 mins
    if league_name in ["Esoccer H2H GG League - 8 mins play", "Esoccer Battle - 8 mins play"]:
        await apply_8mins_strategies(
            match, h2h_metrics, current_time, home_goals, away_goals,
            player1, player2, bet365_ev_id, sent_matches, bot
        )
    
    # Estratégias para Volta 6 mins
    elif league_name == "Esoccer Battle Volta - 6 mins play":
        await apply_volta_6mins_strategies(
            match, h2h_metrics, current_time, home_goals, away_goals,
            bet365_ev_id, sent_matches, bot
        )
    
    # Estratégias para GT Leagues 12 mins
    elif league_name == "Esoccer GT Leagues – 12 mins play":
        await apply_gt_12mins_strategies(
            match, h2h_metrics, current_time, home_goals, away_goals,
            player1, player2, bet365_ev_id, sent_matches, bot
        )

async def apply_8mins_strategies(match, h2h_metrics, current_time, home_goals, away_goals,
                               player1, player2, bet365_ev_id, sent_matches, bot):
    """Aplica estratégias para ligas de 8 minutos"""
    match_id = match.get('id')
    league_name = match['league']['name']
    
    # Extrair métricas
    avg_ht_goals = h2h_metrics['avg_ht_goals']
    btts_ht = h2h_metrics['btts_ht_percentage']
    over_2_5_ht = h2h_metrics['over_2_5_ht_percentage']
    over_1_5_ht = h2h_metrics['over_1_5_ht_percentage']
    over_0_5_ht = h2h_metrics['over_0_5_ht_percentage']
    over_2_5_ft = h2h_metrics['over_2_5_ft_percentage']
    over_3_5_ft = h2h_metrics['over_3_5_ft_percentage']
    p1_avg = h2h_metrics['player1_avg_goals']
    p2_avg = h2h_metrics['player2_avg_goals']
    p1_win = h2h_metrics['player1_win_percentage']
    p2_win = h2h_metrics['player2_win_percentage']

    if is_first_half(match, league_name):
        # Estratégias HT
        conditions = [
            (avg_ht_goals >= 3.0 and btts_ht >= 100 and over_2_5_ht == 100 and 
             current_time > 1 and current_time <= 3 and home_goals == 0 and away_goals == 0,
             "⚽ +2.5 GOLS HT"),
            
            (avg_ht_goals >= 2.5 and btts_ht >= 80 and over_1_5_ht == 100 and 
             current_time > 1 and current_time <= 3 and home_goals == 0 and away_goals == 0,
             "⚽ +1.5 GOLS HT"),
             
            # Adicionar mais condições conforme necessário
        ]
        
        for condition, strategy in conditions:
            if condition:
                msg = format_message(match, h2h_metrics, strategy, bet365_ev_id)
                await send_message(bot, match_id, msg, sent_matches, strategy)
                break

    # Estratégias FT para jogadores específicos
    player_conditions = [
        (p1_avg >= 3.0 and over_2_5_ft == 100 and current_time >= 2 and 
         current_time < 6 and home_goals == 0 and p1_win >= 60.0,
         f"⚽ +1.5 GOLS {player1}"),
         
        (p2_avg >= 3.0 and over_2_5_ft == 100 and current_time >= 2 and 
         current_time < 6 and away_goals == 0 and p2_win >= 60.0,
         f"⚽ +1.5 GOLS {player2}"),
    ]
    
    for condition, strategy in player_conditions:
        if condition:
            msg = format_message(match, h2h_metrics, strategy, bet365_ev_id)
            await send_message(bot, match_id, msg, sent_matches, strategy)
            break

async def apply_volta_6mins_strategies(match, h2h_metrics, current_time, home_goals, away_goals,
                                     bet365_ev_id, sent_matches, bot):
    """Aplica estratégias para Volta 6 mins"""
    match_id = match.get('id')
    
    avg_ft_goals = h2h_metrics['avg_ft_goals']
    btts_ft = h2h_metrics['btts_ft_percentage']
    over_4_5_ft = h2h_metrics['over_4_5_ft_percentage']
    over_3_5_ft = h2h_metrics['over_3_5_ft_percentage']

    conditions = [
        (avg_ft_goals >= 5.5 and btts_ft == 100 and over_4_5_ft == 100 and 
         current_time >= 1 and current_time < 3 and home_goals == 0 and away_goals == 0,
         "⚽ +4.5 GOLS FT"),
         
        (avg_ft_goals >= 3.5 and btts_ft == 100 and over_3_5_ft == 100 and 
         current_time >= 1 and current_time < 3 and home_goals == 0 and away_goals == 0,
         "⚽ +3.5 GOLS FT"),
    ]
    
    for condition, strategy in conditions:
        if condition:
            msg = format_message(match, h2h_metrics, strategy, bet365_ev_id)
            await send_message(bot, match_id, msg, sent_matches, strategy)
            break

async def apply_gt_12mins_strategies(match, h2h_metrics, current_time, home_goals, away_goals,
                                   player1, player2, bet365_ev_id, sent_matches, bot):
    """Aplica estratégias para GT Leagues 12 mins"""
    match_id = match.get('id')
    league_name = match['league']['name']
    
    # Extrair métricas
    avg_ht_goals = h2h_metrics['avg_ht_goals']
    btts_ht = h2h_metrics['btts_ht_percentage']
    over_2_5_ht = h2h_metrics['over_2_5_ht_percentage']
    over_1_5_ht = h2h_metrics['over_1_5_ht_percentage']
    over_0_5_ht = h2h_metrics['over_0_5_ht_percentage']
    over_4_5_ft = h2h_metrics['over_4_5_ft_percentage']
    over_3_5_ft = h2h_metrics['over_3_5_ft_percentage']
    p1_avg = h2h_metrics['player1_avg_goals']
    p2_avg = h2h_metrics['player2_avg_goals']
    p1_win = h2h_metrics['player1_win_percentage']
    p2_win = h2h_metrics['player2_win_percentage']
    
    # Calcular dangerous attacks rate
    da_rate = calculate_dangerous_attacks_rate(match, current_time)

    if is_first_half(match, league_name):
        # Estratégias HT para GT Leagues
        ht_conditions = [
            (avg_ht_goals >= 3.5 and da_rate >= 1.0 and current_time >= 1 and 
             current_time < 6 and home_goals == 0 and away_goals == 0 and 
             btts_ht >= 100 and over_2_5_ht == 100,
             "⚽ +2.5 GOLS HT"),
             
            (avg_ht_goals >= 2.5 and da_rate >= 1.0 and current_time >= 1 and 
             current_time < 6 and home_goals == 0 and away_goals == 0 and 
             btts_ht >= 90 and over_1_5_ht == 100,
             "⚽ +1.5 GOLS HT"),
             
            (avg_ht_goals >= 2.0 and da_rate >= 1.0 and current_time >= 3 and 
             current_time < 6 and home_goals == 0 and away_goals == 0 and 
             over_0_5_ht == 100,
             "⚽ +0.5 GOL HT"),
             
            (avg_ht_goals >= 2.5 and da_rate >= 1.0 and current_time >= 3 and 
             current_time < 6 and ((home_goals == 1 and away_goals == 0) or 
             (home_goals == 0 and away_goals == 1)) and over_1_5_ht == 100,
             "⚽ +1.5 GOLS HT"),
        ]
        
        for condition, strategy in ht_conditions:
            if condition:
                msg = format_message(match, h2h_metrics, strategy, bet365_ev_id)
                await send_message(bot, match_id, msg, sent_matches, strategy)
                break

    # Estratégias FT para jogadores específicos
    player_conditions = [
        (p1_avg >= 3.5 and over_4_5_ft == 100 and current_time >= 1 and 
         current_time < 9 and home_goals == 0 and p1_win >= 60.0,
         f"⚽ +2.5 GOLS {player1}"),
         
        (p1_avg >= 2.5 and over_3_5_ft == 100 and current_time >= 1 and 
         current_time < 9 and home_goals == 0 and p1_win >= 60.0,
         f"⚽ +1.5 GOLS {player1}"),
         
        (p2_avg >= 3.5 and over_4_5_ft == 100 and current_time >= 1 and 
         current_time < 9 and away_goals == 0 and p2_win >= 60.0,
         f"⚽ +2.5 GOLS {player2}"),
         
        (p2_avg >= 2.5 and over_3_5_ft == 100 and current_time >= 1 and 
         current_time < 9 and away_goals == 0 and p2_win >= 60.0,
         f"⚽ +1.5 GOLS {player2}"),
    ]
    
    for condition, strategy in player_conditions:
        if condition:
            msg = format_message(match, h2h_metrics, strategy, bet365_ev_id)
            await send_message(bot, match_id, msg, sent_matches, strategy)
            break

async def periodic_check(bot: Bot):
    """Verificação periódica otimizada"""
    global last_summary, league_stats, last_league_summary, last_league_message_id

    while True:
        try:
            async with OptimizedSession() as session:
                logger.info("Verificando status das tips...")
                ended = await fetch_ended_matches(session)
                
                # Criar dicionário otimizado
                ended_dict = {
                    int(m['eventID']): m for m in ended 
                    if m.get('eventID') and str(m['eventID']).isdigit()
                }
                
                today = datetime.now(MANAUS_TZ).date()
                stats = {'greens': 0, 'reds': 0, 'refunds': 0}
                
                # Processar tips do dia atual
                today_tips = [tip for tip in sent_tips if tip['sent_time'].date() == today]
                
                for tip in today_tips:
                    if tip['status'] == 'pending':
                        match_ended = ended_dict.get(tip['match_id'])
                        if match_ended and match_ended.get('status') == 'ended':
                            await process_tip_result(tip, match_ended, bot)
                    
                    # Contar estatísticas
                    if tip['status'] in stats:
                        stats[tip['status']] += 1

                # Enviar indicador se houver mudanças
                await send_performance_indicator(bot, stats)
                
                # Processar estatísticas das ligas
                await process_league_statistics(ended, bot)

        except Exception as e:
            logger.error(f"periodic_check: {e}")
        
        await asyncio.sleep(120)  # 2 minutos

async def process_tip_result(tip: Dict, match_ended: Dict, bot: Bot):
    """Processa resultado de uma tip específica"""
    try:
        ht_goals = (match_ended.get('scoreHT', {}).get('home', 0) or 0) + \
                  (match_ended.get('scoreHT', {}).get('away', 0) or 0)
        ft_goals = (match_ended.get('score', {}).get('home', 0) or 0) + \
                  (match_ended.get('score', {}).get('away', 0) or 0)
        home_ft_goals = match_ended.get('score', {}).get('home', 0) or 0
        away_ft_goals = match_ended.get('score', {}).get('away', 0) or 0

        strategy = tip['strategy']
        
        # Extrair informações da mensagem
        message_text = tip['message_text']
        player1 = ''
        player2 = ''
        
        if '🎮 ' in message_text:
            try:
                players_line = message_text.split('🎮 ')[1].split('\n')[0]
                if ' vs ' in players_line:
                    player1, player2 = players_line.split(' vs ', 1)
                    player1 = player1.strip()
                    player2 = player2.strip()
            except (IndexError, AttributeError):
                pass

        # Determinar resultado baseado na estratégia
        tip['status'] = determine_tip_result(
            strategy, ht_goals, ft_goals, home_ft_goals, 
            away_ft_goals, player1, player2
        )

        logger.info(f"Tip {tip['match_id']} ⇒ {tip['status']}")
        
        # Editar mensagem
        await update_tip_message(bot, tip)
        
    except Exception as e:
        logger.error(f"process_tip_result: {e}")

def determine_tip_result(strategy: str, ht_goals: int, ft_goals: int, 
                        home_ft_goals: int, away_ft_goals: int, 
                        player1: str, player2: str) -> str:
    """Determina o resultado de uma tip baseado na estratégia"""
    match = re.search(r'\+(\d+\.?\d*)\s+GOLS?', strategy)
    if not match:
        return 'red'  # Se não conseguir extrair a linha, considera como red
    
    line = float(match.group(1))
    
    # Determinar qual métrica usar baseado na estratégia
    if player1 and player1 in strategy:
        actual_goals = home_ft_goals
    elif player2 and player2 in strategy:
        actual_goals = away_ft_goals
    elif 'HT' in strategy:
        actual_goals = ht_goals
    else:  # Assume FT para total de gols
        actual_goals = ft_goals
    
    return 'green' if actual_goals > line else 'red'

async def update_tip_message(bot: Bot, tip: Dict):
    """Atualiza mensagem da tip com resultado"""
    try:
        if tip['status'] in ['green', 'red']:
            emoji = "✅✅✅✅✅" if tip['status'] == 'green' else "❌❌❌❌❌"
            new_text = tip['message_text'] + emoji
            
            await bot.edit_message_text(
                chat_id=CHAT_ID, 
                message_id=tip['message_id'], 
                text=new_text,
                parse_mode="HTML", 
                disable_web_page_preview=True
            )
            logger.info(f"Mensagem {tip['message_id']} editada para {tip['status']}")
    except Exception as e:
        logger.error(f"update_tip_message {tip['message_id']}: {e}")

async def send_performance_indicator(bot: Bot, stats: Dict):
    """Envia indicador de performance"""
    global last_summary
    
    total_resolved = stats['greens'] + stats['reds']
    total = sum(stats.values())
    
    if total > 0:
        perc = (stats['greens'] / total_resolved * 100.0) if total_resolved > 0 else 0.0
        current_summary = (
            f"\n\n<b>👑 ʀᴡ ᴛɪᴘs - ғɪғᴀ 🎮</b>\n\n"
            f"<b>✅ Green [{stats['greens']}]</b>\n"
            f"<b>❌ Red [{stats['reds']}]</b>\n"
            f"<b>♻️ Push [{stats['refunds']}]</b>\n"
            f"📊 <i>Desempenho: {perc:.2f}%</i>\n\n"
        )
        
        if current_summary != last_summary:
            try:
                await bot.send_message(chat_id=CHAT_ID, text=current_summary, parse_mode="HTML")
                last_summary = current_summary
                logger.info("Indicador enviado.")
            except Exception as e:
                logger.error(f"send_performance_indicator: {e}")

async def process_league_statistics(ended_matches: List, bot: Bot):
    """Processa estatísticas das ligas de forma otimizada"""
    global league_stats, last_league_summary, last_league_message_id
    
    try:
        league_games = defaultdict(list)
        
        for match in ended_matches:
            league = (match.get('competition', {}).get('name') or 
                     match.get('tournamentName') or 
                     match.get('leagueName') or 'Unknown')
            
            if league == 'Unknown':
                continue
                
            start_time_str = (match.get('startTime') or '').rstrip('Z')
            try:
                start_time = datetime.fromisoformat(start_time_str).replace(tzinfo=timezone.utc)
            except:
                continue
                
            ft_home = match.get('score', {}).get('home', 0) or 0
            ft_away = match.get('score', {}).get('away', 0) or 0
            ft_goals = ft_home + ft_away
            ht_home = match.get('scoreHT', {}).get('home', 0) or 0
            ht_away = match.get('scoreHT', {}).get('away', 0) or 0
            ht_goals = ht_home + ht_away
            
            league_games[league].append({
                'start_time': start_time, 
                'ht_goals': ht_goals, 
                'ft_goals': ft_goals
            })

        # Calcular estatísticas
        stats = {}
        for league, games in league_games.items():
            games.sort(key=lambda x: x['start_time'], reverse=True)
            last5 = games[:5]
            
            if len(last5) < 5:
                continue
                
            ht_over = {
                '0.5': sum(g['ht_goals'] > 0 for g in last5) / 5 * 100,
                '1.5': sum(g['ht_goals'] > 1 for g in last5) / 5 * 100,
                '2.5': sum(g['ht_goals'] > 2 for g in last5) / 5 * 100,
            }
            ft_over = {
                '0.5': sum(g['ft_goals'] > 0 for g in last5) / 5 * 100,
                '1.5': sum(g['ft_goals'] > 1 for g in last5) / 5 * 100,
                '2.5': sum(g['ft_goals'] > 2 for g in last5) / 5 * 100,
                '3.5': sum(g['ft_goals'] > 3 for g in last5) / 5 * 100,
                '4.5': sum(g['ft_goals'] > 4 for g in last5) / 5 * 100,
            }
            stats[league] = {'ht': ht_over, 'ft': ft_over}

        league_stats = stats
        await send_league_summary(bot, stats)
        
    except Exception as e:
        logger.error(f"process_league_statistics: {e}")

async def send_league_summary(bot: Bot, stats: Dict):
    """Envia resumo das ligas"""
    global last_league_summary, last_league_message_id
    
    if not stats:
        return
        
    try:
        # Calcular médias OVER para cada liga
        league_over_avg = {}
        for league, s in stats.items():
            ft_over_values = [s['ft'][threshold] for threshold in ['0.5', '1.5', '2.5', '3.5', '4.5']]
            avg_over = sum(ft_over_values) / len(ft_over_values) if ft_over_values else 0
            league_over_avg[league] = avg_over

        # Encontrar extremos
        most_over_league = max(league_over_avg.items(), key=lambda x: x[1], default=("Nenhuma", 0))
        most_under_league = min(league_over_avg.items(), key=lambda x: x[1], default=("Nenhuma", 0))

        # Construir mensagem
        summary_msg = "<b>Resumo das Ligas (últimos 5 jogos)</b>\n\n"
        for league, s in stats.items():
            summary_msg += f"<b>{league}</b>\n"
            for line in ['0.5', '1.5', '2.5', '3.5', '4.5']:
                p = s['ft'][line]
                thermo = format_thermometer(p)
                summary_msg += f"OVER {line}\n{thermo}\n"
            summary_msg += "\n"

        # Adicionar destaques
        summary_msg += f"<b>🏆 Liga mais OVER: {most_over_league[0]} (Média OVER: {most_over_league[1]:.1f}%)</b>\n"
        summary_msg += f"<b>🚫 Evitar: {most_under_league[0]} (Média OVER: {most_under_league[1]:.1f}%)</b>\n"

        if summary_msg != last_league_summary:
            # Deletar mensagem anterior se existir
            if last_league_message_id is not None:
                try:
                    await bot.delete_message(chat_id=CHAT_ID, message_id=last_league_message_id)
                    logger.info(f"Mensagem anterior de resumo das ligas deletada.")
                except Exception as e:
                    logger.error(f"Falha ao deletar mensagem anterior: {e}")

            # Enviar nova mensagem
            message_obj = await bot.send_message(chat_id=CHAT_ID, text=summary_msg, parse_mode="HTML")
            last_league_summary = summary_msg
            last_league_message_id = message_obj.message_id
            logger.info("Resumo das ligas enviado.")
            
    except Exception as e:
        logger.error(f"send_league_summary: {e}")

async def initialize_league_data(session: aiohttp.ClientSession) -> bool:
    """Inicializa dados das ligas"""
    global league_stats
    
    try:
        logger.info("Inicializando dados das ligas...")
        ended = await fetch_ended_matches(session)
        league_games = defaultdict(list)
        
        for match in ended:
            league = (match.get('competition', {}).get('name') or 
                     match.get('tournamentName') or 
                     match.get('leagueName') or 'Unknown')
            
            if league == 'Unknown':
                continue
                
            start_time_str = (match.get('startTime') or '').rstrip('Z')
            try:
                start_time = datetime.fromisoformat(start_time_str).replace(tzinfo=timezone.utc)
            except:
                continue
                
            ft_home = match.get('score', {}).get('home', 0) or 0
            ft_away = match.get('score', {}).get('away', 0) or 0
            ft_goals = ft_home + ft_away
            ht_home = match.get('scoreHT', {}).get('home', 0) or 0
            ht_away = match.get('scoreHT', {}).get('away', 0) or 0
            ht_goals = ht_home + ht_away
            
            league_games[league].append({
                'start_time': start_time, 
                'ht_goals': ht_goals, 
                'ft_goals': ft_goals
            })

        stats = {}
        for league, games in league_games.items():
            games.sort(key=lambda x: x['start_time'], reverse=True)
            last5 = games[:5]
            
            if len(last5) >= 5:
                ht_over = {
                    '0.5': sum(g['ht_goals'] > 0 for g in last5) / 5 * 100,
                    '1.5': sum(g['ht_goals'] > 1 for g in last5) / 5 * 100,
                    '2.5': sum(g['ht_goals'] > 2 for g in last5) / 5 * 100,
                }
                ft_over = {
                    '0.5': sum(g['ft_goals'] > 0 for g in last5) / 5 * 100,
                    '1.5': sum(g['ft_goals'] > 1 for g in last5) / 5 * 100,
                    '2.5': sum(g['ft_goals'] > 2 for g in last5) / 5 * 100,
                    '3.5': sum(g['ft_goals'] > 3 for g in last5) / 5 * 100,
                    '4.5': sum(g['ft_goals'] > 4 for g in last5) / 5 * 100,
                }
                stats[league] = {'ht': ht_over, 'ft': ft_over}
        
        league_stats = stats
        return bool(league_stats)
        
    except Exception as e:
        logger.error(f"initialize_league_data: {e}")
        return False

async def main():
    """Função principal otimizada"""
    global league_stats
    
    bot = Bot(token=BOT_TOKEN)
    sent_matches = set()
    
    # Inicialização com retry
    logger.info("Inicializando sistema...")
    max_init_attempts = 5
    
    for attempt in range(max_init_attempts):
        try:
            async with OptimizedSession() as session:
                if await initialize_league_data(session):
                    logger.info("Dados de liga inicializados com sucesso!")
                    break
                else:
                    logger.warning(f"Tentativa {attempt + 1} falhou. Tentando novamente em 5 segundos...")
                    await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"Erro na inicialização tentativa {attempt + 1}: {e}")
            if attempt < max_init_attempts - 1:
                await asyncio.sleep(5)
    
    if not league_stats:
        logger.error("Falha crítica: Não foi possível inicializar dados das ligas")
        return
    
    # Iniciar task de verificação periódica
    asyncio.create_task(periodic_check(bot))
    
    # Loop principal
    logger.info("Iniciando loop principal...")
    while True:
        cycle_start = time.time()
        logger.info(f"Ciclo às {datetime.now(MANAUS_TZ).strftime('%Y-%m-%d %H:%M:%S')}")
        
        try:
            async with OptimizedSession() as session:
                # Buscar dados de forma paralela
                old_matches, bet365_dict, _ = await fetch_all_data(session)
                
                if not old_matches:
                    logger.warning("Nenhuma partida encontrada")
                    await asyncio.sleep(10)
                    continue
                
                # Processar partidas em lotes para evitar sobrecarga
                batch_size = 5
                for i in range(0, len(old_matches), batch_size):
                    batch = old_matches[i:i + batch_size]
                    
                    # Processar lote atual
                    tasks = []
                    for match in batch:
                        if isinstance(match, dict) and match.get('id'):
                            task = process_match_strategies(
                                session, match, bet365_dict, sent_matches, bot
                            )
                            tasks.append(task)
                    
                    if tasks:
                        await asyncio.gather(*tasks, return_exceptions=True)
                    
                    # Pequena pausa entre lotes
                    await asyncio.sleep(0.5)
                
                cycle_time = time.time() - cycle_start
                logger.info(f"Ciclo completado em {cycle_time:.2f}s")
                
        except Exception as e:
            logger.error(f"Erro no loop principal: {e}")
        
        # Aguardar próximo ciclo
        await asyncio.sleep(10)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot interrompido pelo usuário")
    except Exception as e:
        logger.error(f"Erro crítico: {e}")
        raise