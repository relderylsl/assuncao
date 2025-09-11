import os
import time
import requests
import json
import asyncio
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from telegram import Bot  # Requires python-telegram-bot v20+: pip install python-telegram-bot
import re

BOT_TOKEN = "6569266928:AAHm7pOJVsd3WKzJEgdVDez4ZYdCAlRoYO8"
CHAT_ID = "-1001981134607"
OLD_LIVE_API_URL = "https://caveira-proxy.onrender.com/api/matches/live"
NEW_LIVE_API_URL = "https://esoccer.dev3.caveira.tips/v1/esoccer/inplay"
ENDED_API_URL = "https://api-v2.green365.com.br/api/v2/sport-events"
H2H_API_URL = "https://caveira-proxy.onrender.com/api/v1/historico/confronto/{player1}/{player2}?page=1&limit=10"
AUTH_TOKEN = "Bearer oat_MTEyNTEx.aS1EdDJaNWw2dUkzREpqOGI3Mmo1eHdVeUZOZmZyQmZkclR2bE1RODM0ODg3NzEzODQ"
                     

# Manaus timezone is UTC-4
MANAUS_TZ = timezone(timedelta(hours=-4))

sent_tips = []  # [{'match_id': int, 'strategy': str, 'sent_time': datetime, 'status': 'pending/green/red/refund', 'message_id': int, 'message_text': str}]
last_summary = None  # para não spammar o indicador
last_league_summary = None
league_stats = {}
last_league_message_id = None  # To track the last league summary message ID

def fetch_old_live_matches():
    try:
        response = requests.get(OLD_LIVE_API_URL, timeout=10)
        response.raise_for_status()
        data = response.json().get('data', [])
        valid_matches = [m for m in data if isinstance(m, dict) and m.get('id') and m.get('league') and m.get('home') and m.get('away')]
        print(f"[INFO] {len(data)} partidas da API antiga; {len(valid_matches)} válidas")
        return valid_matches
    except Exception as e:
        print(f"[ERROR] fetch_old_live_matches: {e}")
        return []

def fetch_bet365_ids():
    try:
        headers = {
            "Authorization": AUTH_TOKEN,
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        response = requests.post(NEW_LIVE_API_URL, headers=headers, json={}, timeout=10)
        response.raise_for_status()
        data = response.json().get('data', [])
        print(f"[INFO] {len(data)} partidas da API nova (links)")
        mapping = {}
        for m in data:
            try:
                k = (m['player_home_name'].lower(), m['player_away_name'].lower())
                mapping[k] = m.get('bet365_ev_id')
            except KeyError:
                continue
        return mapping
    except Exception as e:
        print(f"[ERROR] fetch_bet365_ids: {e}")
        return {}

def fetch_ended_matches():
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 OPR/120.0.0.0",
        "Accept": "application/json",
        "Referer": "https://green365.com.br/",
        "Origin": "https://green365.com.br",
        "Authorization": AUTH_TOKEN
    }
    items = []
    params = {"page": 1, "limit": 200, "sport": "esoccer", "status": "ended"}
    url = ENDED_API_URL
    print(f"[DEBUG] GET {url} params={params}")
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=5)
        if response.status_code != 200:
            print(f"[ERROR] ended_matches {response.status_code} {response.text}")
            return items
        data = response.json()
        for item in data.get('items', []):
            items.append(item)
        print(f"[DEBUG] Finalizadas retornadas: {len(items)}")
    except Exception as e:
        print(f"[ERROR] fetch_ended_matches: {e}")
    return items

def fetch_h2h_data(player1, player2):
    try:
        url = H2H_API_URL.format(player1=player1, player2=player2)
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        data = response.json()
        return data
    except Exception as e:
        print(f"[ERROR] fetch_h2h_data {player1} vs {player2}: {e}")
        return None

def get_match_time_in_minutes(match):
    timer = match.get('timer') or {}
    if not timer:
        print(f"[DEBUG] Partida {match.get('id','?')}: sem timer")
        return 0.0
    tm = timer.get('tm', 0)
    ts = timer.get('ts', 0)
    try:
        return float(tm) + float(ts) / 60.0
    except Exception:
        return float(tm) if isinstance(tm, (int, float)) else 0.0

def is_first_half(match, league_name):
    minutes = get_match_time_in_minutes(match)
    if "8 mins play" in league_name:
        is_first = minutes < 4
    elif "6 mins play" in league_name:
        is_first = minutes < 3
    elif "12 mins play" in league_name or "10 mins play" in league_name:
        is_first = minutes < 6
    else:
        is_first = False
    print(f"[DEBUG] {match.get('id','?')} ({league_name}): {minutes:.2f} min | 1ºT: {is_first}")
    return is_first

def calculate_dangerous_attacks_rate(match, current_time):
    if current_time <= 0:
        print(f"[DEBUG] {match.get('id','?')}: tempo=0 ⇒ da_rate=0")
        return 0.0
    stats = match.get('stats')
    if not isinstance(stats, dict):
        print(f"[DEBUG] {match.get('id','?')}: stats ausente/ inválido ⇒ da_rate=0")
        return 0.0
    da = stats.get('dangerous_attacks', [0, 0])
    try:
        total_da = int(da[0]) + int(da[1])
    except Exception:
        total_da = 0
    da_rate = total_da / current_time
    print(f"[DEBUG] {match.get('id','?')}: DA={total_da} | t={current_time:.2f} ⇒ da_rate={da_rate:.2f}")
    return da_rate

def calculate_h2h_metrics(h2h_data, league_name):
    if not h2h_data or 'matches' not in h2h_data:
        print("[DEBUG] H2H ausente/ inválido")
        return None

    matches = h2h_data['matches']
    total = len(matches)
    if total == 0:
        print("[DEBUG] H2H vazio")
        return None

    # Contadores para as estatísticas
    over_0_5_ht = 0
    over_1_5_ht = 0
    over_2_5_ht = 0
    btts_ht = 0
    player1_wins = 0
    player2_wins = 0
    player1_total_goals = 0
    player2_total_goals = 0
    total_ht_goals = 0
    total_ft_goals = 0
    player1_total_ht_goals = 0
    player2_total_ht_goals = 0
    over_2_5_ft = 0
    over_3_5_ft = 0
    over_4_5_ft = 0
    btts_ft = 0

    print(f"[DEBUG] Analisando {total} jogos H2H...")

    for i, m in enumerate(matches):
        # Resultados do primeiro tempo
        ht_home = m.get('halftime_score_home', 0)
        ht_away = m.get('halftime_score_away', 0)
        ht_goals = (ht_home or 0) + (ht_away or 0)
        total_ht_goals += ht_goals
        player1_total_ht_goals += (ht_home or 0)
        player2_total_ht_goals += (ht_away or 0)
        if ht_goals > 0: over_0_5_ht += 1
        if ht_goals > 1: over_1_5_ht += 1
        if ht_goals > 2: over_2_5_ht += 1
        if (ht_home or 0) > 0 and (ht_away or 0) > 0: btts_ht += 1

        # Resultados finais (múltiplas tentativas)
        ft_home = None
        ft_away = None
        
        if ft_home is None:
            ft_home = m.get('final_score_home')
            ft_away = m.get('final_score_away')
            
        if ft_home is None:
            ft_home = m.get('score_home')
            ft_away = m.get('score_away')
            
        if ft_home is None:
            ft_home = m.get('home_score')
            ft_away = m.get('away_score')
            
        if ft_home is None:
            score_obj = m.get('score') or m.get('final_score')
            if isinstance(score_obj, dict):
                ft_home = score_obj.get('home')
                ft_away = score_obj.get('away')
                
        if ft_home is None:
            result = m.get('result')
            if isinstance(result, dict):
                ft_home = result.get('home') or result.get('home_score')
                ft_away = result.get('away') or result.get('away_score')
        
        if ft_home is None:
            ft_home = m.get('ft_score_home')
            ft_away = m.get('ft_score_away')
            
        if ft_home is None:
            ft_home = m.get('home_goals')
            ft_away = m.get('away_goals')
            
        if ft_home is None:
            score_str = m.get('score')
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
        total_ft_goals += ft_goals

        if ft_goals > 2: over_2_5_ft += 1
        if ft_goals > 3: over_3_5_ft += 1
        if ft_goals > 4: over_4_5_ft += 1
        if ft_home > 0 and ft_away > 0: btts_ft += 1

        # Contagem de vitórias
        if ft_home > ft_away:
            player1_wins += 1
        elif ft_away > ft_home:
            player2_wins += 1

        # Contagem de gols
        player1_total_goals += ft_home
        player2_total_goals += ft_away

    # Calculando as porcentagens
    player1_win_percentage = (player1_wins / total) * 100.0 if total > 0 else 0.0
    player2_win_percentage = (player2_wins / total) * 100.0 if total > 0 else 0.0
    player1_avg_goals = player1_total_goals / total if total > 0 else 0.0
    player2_avg_goals = player2_total_goals / total if total > 0 else 0.0

    metrics = {
        'player1_win_percentage': player1_win_percentage,
        'player2_win_percentage': player2_win_percentage,
        'player1_avg_goals': player1_avg_goals,
        'player2_avg_goals': player2_avg_goals,
        'player1_avg_ht_goals': player1_total_ht_goals / total if total > 0 else 0.0,
        'player2_avg_ht_goals': player2_total_ht_goals / total if total > 0 else 0.0,
        'avg_ht_goals': total_ht_goals / total if total > 0 else 0.0,
        'avg_ft_goals': total_ft_goals / total if total > 0 else 0.0,
        'over_0_5_ht_percentage': (over_0_5_ht / total) * 100.0 if total > 0 else 0.0,
        'over_1_5_ht_percentage': (over_1_5_ht / total) * 100.0 if total > 0 else 0.0,
        'over_2_5_ht_percentage': (over_2_5_ht / total) * 100.0 if total > 0 else 0.0,
        'over_2_5_ft_percentage': (over_2_5_ft / total) * 100.0 if total > 0 else 0.0,
        'over_3_5_ft_percentage': (over_3_5_ft / total) * 100.0 if total > 0 else 0.0,
        'over_4_5_ft_percentage': (over_4_5_ft / total) * 100.0 if total > 0 else 0.0,
        'btts_ht_percentage': (btts_ht / total) * 100.0 if total > 0 else 0.0,
        'btts_ft_percentage': (btts_ft / total) * 100.0 if total > 0 else 0.0
    }

    print(
        f"[DEBUG] H2H FINAL {league_name} | Win1={player1_win_percentage:.1f}% Win2={player2_win_percentage:.1f}% "
        f"AvgG1={player1_avg_goals:.2f} AvgG2={player2_avg_goals:.2f} | "
        f"AvgHT={metrics['avg_ht_goals']:.2f} AvgFT={metrics['avg_ft_goals']:.2f} | "
        f"O0.5HT={metrics['over_0_5_ht_percentage']:.1f} O1.5HT={metrics['over_1_5_ht_percentage']:.1f} "
        f"O2.5HT={metrics['over_2_5_ht_percentage']:.1f} BTTSHT={metrics['btts_ht_percentage']:.1f} | "
        f"O2.5FT={metrics['over_2_5_ft_percentage']:.1f} O3.5FT={metrics['over_3_5_ft_percentage']:.1f} "
        f"O4.5FT={metrics['over_4_5_ft_percentage']:.1f} BTTSFT={metrics['btts_ft_percentage']:.1f}"
    )
    print(f"[DEBUG] Totais: P1_wins={player1_wins}, P2_wins={player2_wins}, P1_goals={player1_total_goals}, P2_goals={player2_total_goals}")

    return metrics

def format_message(match, h2h_metrics, strategy, bet365_ev_id):
    league = match['league']['name']
    home = match['home']['name']
    away = match['away']['name']
    player1 = home.split('(')[-1].rstrip(')') if '(' in home else home
    player2 = away.split('(')[-1].rstrip(')') if '(' in away else away
    timer = match.get('timer') or {}
    minutes = timer.get('tm', 0)
    seconds = timer.get('ts', 0)
    game_time = f"{minutes}:{int(seconds):02d}"
    ss = match.get('ss', '0-0')
    
    # Cabeçalho
    msg = f"\n\n<b>🏆 {league}</b>\n\n<b>🎯 {strategy}</b>\n\n⏳ Tempo: {game_time}\n\n"
    msg += f"🎮 {player1} vs {player2}\n"
    msg += f"⚽ Placar: {ss}\n"
    
    # H2H
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

async def send_message(bot, match_id, message, sent_matches, strategy):
    if match_id in sent_matches:
        return
    try:
        message_obj = await bot.send_message(chat_id=CHAT_ID, text=message, parse_mode="HTML", disable_web_page_preview=True)
        sent_matches.add(match_id)
        print(f"[INFO] Enviado match_id={match_id} ({strategy})")
        sent_tips.append({
            'match_id': int(match_id),
            'strategy': strategy,
            'sent_time': datetime.now(MANAUS_TZ),
            'status': 'pending',
            'message_id': message_obj.message_id,
            'message_text': message
        })
    except Exception as e:
        print(f"[ERROR] send_message {match_id}: {e}")

def format_thermometer(perc):
    bars = 10
    green_count = round(perc / 10)
    bar = '🟩' * green_count + '🟥' * (bars - green_count)
    return f"{bar} {perc:.0f}%  {(100 - perc):.0f}%"

async def periodic_check(bot):
    global last_summary, league_stats, last_league_summary, last_league_message_id

    while True:
        try:
            print("[INFO] Verificando status das tips...")
            ended = fetch_ended_matches()
            ended_dict = {}
            for m in ended:
                try:
                    ended_dict[int(m['eventID'])] = m
                except Exception:
                    continue
            
            today = datetime.now(MANAUS_TZ).date()
            greens = reds = refunds = 0
            
            for tip in sent_tips:
                if tip['sent_time'].date() != today:
                    continue
                
                if tip['status'] == 'pending':
                    m = ended_dict.get(tip['match_id'])
                    if m and m.get('status') == 'ended':
                        print(f"[DEBUG] Tip {tip['match_id']}: Partida finalizada (status=ended)")
                        
                        ht_goals = (m.get('scoreHT', {}).get('home', 0) or 0) + (m.get('scoreHT', {}).get('away', 0) or 0)
                        ft_goals = (m.get('score', {}).get('home', 0) or 0) + (m.get('score', {}).get('away', 0) or 0)
                        home_ft_goals = m.get('score', {}).get('home', 0) or 0
                        away_ft_goals = m.get('score', {}).get('away', 0) or 0

                        strat = tip['strategy']
                        player1 = tip['message_text'].split('🎮 ')[1].split(' vs ')[0].strip() if '🎮 ' in tip['message_text'] else ''
                        player2 = tip['message_text'].split(' vs ')[1].split('\n')[0].strip() if ' vs ' in tip['message_text'] else ''

                        match = re.search(r'\+(\d+\.?\d*)\s+GOLS', strat)
                        if match:
                            line = float(match.group(1))
                            threshold = line + 0.5  # e.g., +0.5 > 0.5, +1.5 > 1.5

                            if player1 in strat:
                                player_goals = home_ft_goals
                                tip['status'] = 'green' if player_goals > line else 'red'
                            elif player2 in strat:
                                player_goals = away_ft_goals
                                tip['status'] = 'green' if player_goals > line else 'red'
                            elif 'HT' in strat:
                                tip['status'] = 'green' if ht_goals > line else 'red'
                            elif 'FT' in strat:
                                tip['status'] = 'green' if ft_goals > line else 'red'
                            else:
                                # Assume FT for total goals
                                tip['status'] = 'green' if ft_goals > line else 'red'

                        print(f"[DEBUG] Tip {tip['match_id']} ⇒ {tip['status']}")
                        
                        # Editar a mensagem original
                        if tip['status'] in ['green', 'red']:
                            if tip['status'] == 'green':
                                emoji = "✅✅✅✅✅"
                            elif tip['status'] == 'red':
                                emoji = "❌❌❌❌❌"
                            new_text = tip['message_text'] + f"{emoji}"
                            try:
                                await bot.edit_message_text(chat_id=CHAT_ID, message_id=tip['message_id'], text=new_text,
                                                            parse_mode="HTML", disable_web_page_preview=True)
                                print(f"[INFO] Mensagem {tip['message_id']} editada para {tip['status']}")
                            except Exception as edit_e:
                                print(f"[ERROR] Erro ao editar mensagem {tip['message_id']}: {edit_e}")
                
                if tip['status'] == 'green': greens += 1
                if tip['status'] == 'red': reds += 1
                if tip['status'] == 'refund': refunds += 1
            
            total_resolved = greens + reds
            total = greens + reds + refunds
            if total > 0:
                perc = (greens / total_resolved * 100.0) if total_resolved > 0 else 0.0
                current_summary = (
                    f"\n\n<b>👑 ʀᴡ ᴛɪᴘs - ғɪғᴀ 🎮</b>\n\n"
                    f"<b>✅ Green [{greens}]</b>\n"
                    f"<b>❌ Red [{reds}]</b>\n"
                    f"<b>♻️ Push [{refunds}]</b>\n"
                    f"📊 <i>Desempenho: {perc:.2f}%</i>\n\n"
                )
                if current_summary != last_summary:
                    try:
                        await bot.send_message(chat_id=CHAT_ID, text=current_summary, parse_mode="HTML")
                        last_summary = current_summary
                        print("[INFO] Indicador enviado.")
                    except Exception as e:
                        print(f"[ERROR] indicador: {e}")
                else:
                    print("[INFO] Indicador igual ao anterior — não reenviado.")
            else:
                print("[INFO] Sem resultados para indicador.")

            # Computar estatísticas das ligas
            league_games = defaultdict(list)
            for m in ended:
                league = m.get('competition', {}).get('name') or m.get('tournamentName') or m.get('leagueName') or 'Unknown'
                print(f"[DEBUG] Processing league: {league}")
                if league == 'Unknown': continue
                start_time_str = (m.get('startTime') or '').rstrip('Z')
                try:
                    start_time = datetime.fromisoformat(start_time_str).replace(tzinfo=timezone.utc)
                except:
                    continue
                ft_home = m.get('score', {}).get('home', 0) or 0
                ft_away = m.get('score', {}).get('away', 0) or 0
                ft_goals = ft_home + ft_away
                ht_home = m.get('scoreHT', {}).get('home', 0) or 0
                ht_away = m.get('scoreHT', {}).get('away', 0) or 0
                ht_goals = ht_home + ht_away
                league_games[league].append({'start_time': start_time, 'ht_goals': ht_goals, 'ft_goals': ft_goals})

            stats = {}
            for league, games in league_games.items():
                games.sort(key=lambda x: x['start_time'], reverse=True)
                last5 = games[:5]
                if len(last5) < 5: continue
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

            # Enviar resumo das ligas
            if stats:
                # Calculate average over percentage for each league
                league_over_avg = {}
                for league, s in stats.items():
                    ft_over_values = [s['ft'][threshold] for threshold in ['0.5', '1.5', '2.5', '3.5', '4.5']]
                    avg_over = sum(ft_over_values) / len(ft_over_values) if ft_over_values else 0
                    league_over_avg[league] = avg_over

                # Find the most over-friendly and most under-friendly leagues
                most_over_league = max(league_over_avg.items(), key=lambda x: x[1], default=("Nenhuma", 0))
                most_under_league = min(league_over_avg.items(), key=lambda x: x[1], default=("Nenhuma", 0))

                # Build summary message
                summary_msg = "<b>Resumo das Ligas (últimos 5 jogos)</b>\n\n"
                for league, s in stats.items():
                    summary_msg += f"<b>{league}</b>\n"
                    for line in ['0.5', '1.5', '2.5', '3.5', '4.5']:
                        p = s['ft'][line]
                        thermo = format_thermometer(p)
                        summary_msg += f"OVER {line}\n{thermo}\n"
                    summary_msg += "\n"

                # Add highlights
                summary_msg += f"<b>🏆 Liga mais OVER: {most_over_league[0]} (Média OVER: {most_over_league[1]:.1f}%)</b>\n"
                summary_msg += f"<b>🚫 Evitar: {most_under_league[0]} (Média OVER: {most_under_league[1]:.1f}%)</b>\n"

                if summary_msg != last_league_summary:
                    # Delete the previous league summary message if it exists
                    if last_league_message_id is not None:
                        try:
                            await bot.delete_message(chat_id=CHAT_ID, message_id=last_league_message_id)
                            print(f"[INFO] Mensagem de resumo das ligas anterior ({last_league_message_id}) deletada.")
                        except Exception as delete_e:
                            print(f"[ERROR] Falha ao deletar mensagem anterior {last_league_message_id}: {delete_e}")

                    # Send the new league summary message
                    message_obj = await bot.send_message(chat_id=CHAT_ID, text=summary_msg, parse_mode="HTML")
                    last_league_summary = summary_msg
                    last_league_message_id = message_obj.message_id  # Update the stored message ID
                    print("[INFO] Resumo das ligas enviado.")
                else:
                    print("[INFO] Resumo das ligas igual ao anterior — não reenviado.")
        except Exception as e:
            print(f"[ERROR] periodic_check: {e}")
        await asyncio.sleep(120)  # 4 minutes

async def main():
    global league_stats  # Declare league_stats as global
    bot = Bot(token=BOT_TOKEN)
    sent_matches = set()
    
    # Wait for initial data population
    print("[INFO] Aguardando inicialização dos dados de liga...")
    while not league_stats:
        ended = fetch_ended_matches()
        league_games = defaultdict(list)
        for m in ended:
            league = m.get('competition', {}).get('name') or m.get('tournamentName') or m.get('leagueName') or 'Unknown'
            if league == 'Unknown': continue
            start_time_str = (m.get('startTime') or '').rstrip('Z')
            try:
                start_time = datetime.fromisoformat(start_time_str).replace(tzinfo=timezone.utc)
            except:
                continue
            ft_home = m.get('score', {}).get('home', 0) or 0
            ft_away = m.get('score', {}).get('away', 0) or 0
            ft_goals = ft_home + ft_away
            ht_home = m.get('scoreHT', {}).get('home', 0) or 0
            ht_away = m.get('scoreHT', {}).get('away', 0) or 0
            ht_goals = ht_home + ht_away
            league_games[league].append({'start_time': start_time, 'ht_goals': ht_goals, 'ft_goals': ft_goals})

        stats = {}
        for league, games in league_games.items():
            games.sort(key=lambda x: x['start_time'], reverse=True)
            last5 = games[:5]
            if len(last5) >= 5:  # Only include leagues with at least 5 games
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
        if league_stats:  # Break if any league has data
            print("[INFO] Dados de liga inicializados com sucesso!")
            break
        else:
            print("[INFO] Dados de liga ainda não disponíveis, aguardando 5 segundos...")
            await asyncio.sleep(5)
    
    asyncio.create_task(periodic_check(bot))
    
    while True:
        print(f"[INFO] Ciclo às {datetime.now(MANAUS_TZ).strftime('%Y-%m-%d %H:%M:%S')}")
        try:
            bet365_dict = fetch_bet365_ids()
            matches = fetch_old_live_matches()
            
            for match in matches:
                if not isinstance(match, dict):
                    print(f"[WARN] Match inválido: {match}")
                    continue
                
                match_id = match.get('id', 'unknown')
                league_name = (match.get('league') or {}).get('name', 'Desconhecida')
                home = (match.get('home') or {}).get('name', 'Desconhecido')
                away = (match.get('away') or {}).get('name', 'Desconhecido')
                
                print(f"[DEBUG] Match {match_id}: {home} vs {away} | {league_name}")
                
                # Placar atual
                ss = match.get('ss')
                if ss:
                    try:
                        home_goals, away_goals = map(int, ss.split('-'))
                    except Exception:
                        home_goals, away_goals = 0, 0
                else:
                    home_goals, away_goals = 0, 0
                
                total_goals = home_goals + away_goals
                current_time = get_match_time_in_minutes(match)

                # Jogadores (apenas nomes entre parênteses, se houver)
                player1 = home.split('(')[-1].rstrip(')') if '(' in home else home
                player2 = away.split('(')[-1].rstrip(')') if '(' in away else away
                k = (player1.lower(), player2.lower())
                bet365_ev_id = bet365_dict.get(k)
                
                # H2H
                h2h_data = fetch_h2h_data(player1, player2)
                h2h_metrics = calculate_h2h_metrics(h2h_data, league_name)

                # Calcular da_rate se necessário
                da_rate = calculate_dangerous_attacks_rate(match, current_time) if "GT Leagues" in league_name else 0.0

                if h2h_metrics:
                    # Estratégias para ligas de 8 mins
                    if league_name in ["Esoccer H2H GG League - 8 mins play", "Esoccer Battle - 8 mins play"]:
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
                            # HT strategies
                            if avg_ht_goals >= 3.0 and btts_ht >= 100 and over_2_5_ht == 100 and current_time > 1 and current_time <= 3 and home_goals == 0 and away_goals == 0:
                                strategy = "⚽ +2.5 GOLS HT"
                                msg = format_message(match, h2h_metrics, strategy, bet365_ev_id)
                                await send_message(bot, match_id, msg, sent_matches, strategy)

                            if avg_ht_goals >= 2.5 and btts_ht >= 80 and over_1_5_ht == 100 and current_time > 1 and current_time <= 3 and home_goals == 0 and away_goals == 0:
                                strategy = "⚽ +1.5 GOLS HT"
                                msg = format_message(match, h2h_metrics, strategy, bet365_ev_id)
                                await send_message(bot, match_id, msg, sent_matches, strategy)

                            if avg_ht_goals >= 2.0 and btts_ht >= 80 and over_0_5_ht == 100 and current_time > 1 and current_time <= 3 and home_goals == 0 and away_goals == 0:
                                strategy = "⚽ +0.5 GOL HT"
                                msg = format_message(match, h2h_metrics, strategy, bet365_ev_id)
                                await send_message(bot, match_id, msg, sent_matches, strategy)

                            if avg_ht_goals >= 3.0 and btts_ht >= 100 and over_2_5_ht == 100 and current_time >= 2 and current_time <= 3 and ((home_goals == 1 and away_goals == 0) or (home_goals == 0 and away_goals == 1)):
                                strategy = "⚽ +1.5 GOLS HT"
                                msg = format_message(match, h2h_metrics, strategy, bet365_ev_id)
                                await send_message(bot, match_id, msg, sent_matches, strategy)

                        # Player specific (FT)
                        if p1_avg >= 3.0 and over_2_5_ft == 100 and current_time >= 2 and current_time < 6 and home_goals == 0 and p1_win >= 60.0:
                            strategy = f"⚽ +1.5 GOLS {player1}"
                            msg = format_message(match, h2h_metrics, strategy, bet365_ev_id)
                            await send_message(bot, match_id, msg, sent_matches, strategy)

                        if p1_avg >= 4.0 and over_3_5_ft == 100 and current_time >= 2 and current_time < 6 and home_goals == 0 and p1_win >= 60.0:
                            strategy = f"⚽ +2.5 GOLS {player1}"
                            msg = format_message(match, h2h_metrics, strategy, bet365_ev_id)
                            await send_message(bot, match_id, msg, sent_matches, strategy)

                        if p2_avg >= 3.0 and over_2_5_ft == 100 and current_time >= 2 and current_time < 6 and away_goals == 0 and p2_win >= 60.0:
                            strategy = f"⚽ +1.5 GOLS {player2}"
                            msg = format_message(match, h2h_metrics, strategy, bet365_ev_id)
                            await send_message(bot, match_id, msg, sent_matches, strategy)

                        if p2_avg >= 4.0 and over_3_5_ft == 100 and current_time >= 2 and current_time < 6 and away_goals == 0 and p2_win >= 60.0:
                            strategy = f"⚽ +2.5 GOLS {player2}"
                            msg = format_message(match, h2h_metrics, strategy, bet365_ev_id)
                            await send_message(bot, match_id, msg, sent_matches, strategy)

                    # Estratégias para Volta 6 mins
                    elif league_name == "Esoccer Battle Volta - 6 mins play":
                        avg_ft_goals = h2h_metrics['avg_ft_goals']
                        btts_ft = h2h_metrics['btts_ft_percentage']
                        over_4_5_ft = h2h_metrics['over_4_5_ft_percentage']
                        over_3_5_ft = h2h_metrics['over_3_5_ft_percentage']

                        if avg_ft_goals >= 5.5 and btts_ft == 100 and over_4_5_ft == 100 and current_time >= 1 and current_time < 3 and home_goals == 0 and away_goals == 0:
                            strategy = "⚽ +4.5 GOLS FT"
                            msg = format_message(match, h2h_metrics, strategy, bet365_ev_id)
                            await send_message(bot, match_id, msg, sent_matches, strategy)

                        if avg_ft_goals >= 3.5 and btts_ft == 100 and over_3_5_ft == 100 and current_time >= 1 and current_time < 3 and home_goals == 0 and away_goals == 0:
                            strategy = "⚽ +3.5 GOLS FT"
                            msg = format_message(match, h2h_metrics, strategy, bet365_ev_id)
                            await send_message(bot, match_id, msg, sent_matches, strategy)

                    # Estratégias para GT Leagues 12 mins
                    elif league_name == "Esoccer GT Leagues – 12 mins play":
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

                        if is_first_half(match, league_name):
                            # HT strategies
                            if avg_ht_goals >= 3.5 and da_rate >= 1.0 and current_time >= 1 and current_time < 6 and home_goals == 0 and away_goals == 0 and btts_ht >= 100 and over_2_5_ht == 100:
                                strategy = "⚽ +2.5 GOLS HT"
                                msg = format_message(match, h2h_metrics, strategy, bet365_ev_id)
                                await send_message(bot, match_id, msg, sent_matches, strategy)

                            if avg_ht_goals >= 2.5 and da_rate >= 1.0 and current_time >= 1 and current_time < 6 and home_goals == 0 and away_goals == 0 and btts_ht >= 90 and over_1_5_ht == 100:
                                strategy = "⚽ +1.5 GOLS HT"
                                msg = format_message(match, h2h_metrics, strategy, bet365_ev_id)
                                await send_message(bot, match_id, msg, sent_matches, strategy)

                            if avg_ht_goals >= 2.0 and da_rate >= 1.0 and current_time >= 3 and current_time < 6 and home_goals == 0 and away_goals == 0 and over_0_5_ht == 100:
                                strategy = "⚽ +0.5 GOL HT"
                                msg = format_message(match, h2h_metrics, strategy, bet365_ev_id)
                                await send_message(bot, match_id, msg, sent_matches, strategy)

                            if avg_ht_goals >= 2.5 and da_rate >= 1.0 and current_time >= 3 and current_time < 6 and ((home_goals == 1 and away_goals == 0) or (home_goals == 0 and away_goals == 1)) and over_1_5_ht == 100:
                                strategy = "⚽ +1.5 GOLS HT"
                                msg = format_message(match, h2h_metrics, strategy, bet365_ev_id)
                                await send_message(bot, match_id, msg, sent_matches, strategy)

                        # Player specific (FT)
                        if p1_avg >= 3.5 and over_4_5_ft == 100 and current_time >= 1 and current_time < 9 and home_goals == 0 and p1_win >= 60.0:
                            strategy = f"⚽ +2.5 GOLS {player1}"
                            msg = format_message(match, h2h_metrics, strategy, bet365_ev_id)
                            await send_message(bot, match_id, msg, sent_matches, strategy)

                        if p1_avg >= 2.5 and over_3_5_ft == 100 and current_time >= 1 and current_time < 9 and home_goals == 0 and p1_win >= 60.0:
                            strategy = f"⚽ +1.5 GOLS {player1}"
                            msg = format_message(match, h2h_metrics, strategy, bet365_ev_id)
                            await send_message(bot, match_id, msg, sent_matches, strategy)

                        if p2_avg >= 3.5 and over_4_5_ft == 100 and current_time >= 1 and current_time < 9 and away_goals == 0 and p2_win >= 60.0:
                            strategy = f"⚽ +2.5 GOLS {player2}"
                            msg = format_message(match, h2h_metrics, strategy, bet365_ev_id)
                            await send_message(bot, match_id, msg, sent_matches, strategy)

                        if p2_avg >= 2.5 and over_3_5_ft == 100 and current_time >= 1 and current_time < 9 and away_goals == 0 and p2_win >= 60.0:
                            strategy = f"⚽ +1.5 GOLS {player2}"
                            msg = format_message(match, h2h_metrics, strategy, bet365_ev_id)
                            await send_message(bot, match_id, msg, sent_matches, strategy)

        except Exception as e:
            print(f"[ERROR] loop principal: {e}")
        
        await asyncio.sleep(10)  # a cada 10s

if __name__ == "__main__":
    asyncio.run(main())