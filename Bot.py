import discord
from discord import app_commands
from discord.ext import commands
import aiohttp
import asyncio
from functools import lru_cache
from collections import defaultdict
import logging
from dotenv import load_dotenv
import gc
from typing import Dict, Optional, Tuple, List, Set
from dataclasses import dataclass
import os
import json
from datetime import datetime

logger = logging.getLogger(__name__)

load_dotenv()
TOKEN = os.getenv('DISCORD_BOT_TOKEN')
ADMIN_PASSCODE = os.getenv('ADMIN_PASSCODE')

if not TOKEN:
    raise ValueError("DISCORD_BOT_TOKEN environment variable is required")
if not ADMIN_PASSCODE:
    raise ValueError("ADMIN_PASSCODE environment variable is required")


intents = discord.Intents.default()
intents.message_content = False
intents.presences = False
intents.typing = False
bot = commands.Bot(command_prefix="!", intents=intents)



FACTIONS = {
    1: "Chaos", 2: "Dark Eldar", 3: "Eldar", 4: "Imperial Guard", 5: "Necrons",
    6: "Orks", 7: "Sisters of Battle", 8: "Space Marines", 9: "Tau Empire"
}

RACE_MAP = {
    0: "Chaos", 1: "Dark Eldar", 2: "Eldar", 3: "Imperial Guard", 4: "Necrons",
    5: "Orks", 6: "Sisters of Battle", 7: "Space Marines", 8: "Tau Empire"
}

BASE_URL = 'https://dow-api.reliclink.com/community/leaderboard'



class ConnectionManager:
    __slots__ = ['_session', '_lock']

    def __init__(self):
        self._session = None
        self._lock = asyncio.Lock()

    async def get_session(self):
        if self._session is None or self._session.closed:
            async with self._lock:
                if self._session is None or self._session.closed:
                    await self._create_session()
        return self._session

    async def _create_session(self):
        if self._session and not self._session.closed:
            await self._session.close()

        timeout = aiohttp.ClientTimeout(total=30, connect=10)
        connector = aiohttp.TCPConnector(
            limit=10,
            limit_per_host=5,
            ttl_dns_cache=300,
            use_dns_cache=True,
            enable_cleanup_closed=True,
            keepalive_timeout=30
        )

        self._session = aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers={'User-Agent': 'Discord-Bot/2.0'}
        )

        logger.info("Created new HTTP session")

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
            logger.info("HTTP session closed")


connection_manager = ConnectionManager()



@dataclass
class MatchData:
    match_id: str
    map_name: str
    start_time: float
    completion_time: float
    player1_steamid: str
    player1_alias: str
    player1_race: str
    player1_old_elo: int
    player1_new_elo: int
    player2_steamid: str
    player2_alias: str
    player2_race: str
    player2_old_elo: int
    player2_new_elo: int
    winner_steamid: str
    winner_race: str

    def to_dict(self):
        return {
            "match_id": self.match_id,
            "map_name": self.map_name,
            "start_time": self.start_time,
            "completion_time": self.completion_time,
            "player1_steamid": self.player1_steamid,
            "player1_alias": self.player1_alias,
            "player1_race": self.player1_race,
            "player1_old_elo": self.player1_old_elo,
            "player1_new_elo": self.player1_new_elo,
            "player2_steamid": self.player2_steamid,
            "player2_alias": self.player2_alias,
            "player2_race": self.player2_race,
            "player2_old_elo": self.player2_old_elo,
            "player2_new_elo": self.player2_new_elo,
            "winner_steamid": self.winner_steamid,
            "winner_race": self.winner_race
        }

    @classmethod
    def from_dict(cls, data: dict):
        return cls(
            match_id=data["match_id"],
            map_name=data["map_name"],
            start_time=data["start_time"],
            completion_time=data["completion_time"],
            player1_steamid=data["player1_steamid"],
            player1_alias=data["player1_alias"],
            player1_race=data["player1_race"],
            player1_old_elo=data["player1_old_elo"],
            player1_new_elo=data["player1_new_elo"],
            player2_steamid=data["player2_steamid"],
            player2_alias=data["player2_alias"],
            player2_race=data["player2_race"],
            player2_old_elo=data["player2_old_elo"],
            player2_new_elo=data["player2_new_elo"],
            winner_steamid=data["winner_steamid"],
            winner_race=data["winner_race"]
        )



stored_matches: Dict[str, MatchData] = {}
processed_match_ids: Set[str] = set()
player_aliases: Dict[str, str] = {}






def save_aliases_to_file(filename: str = "player_aliases.json"):
    try:
        data = {
            "player_aliases": player_aliases,
            "last_updated": datetime.now().isoformat(),
            "total_aliases": len(player_aliases)
        }

        with open(filename, 'w') as f:
            json.dump(data, f, separators=(',', ':'))
        print(f"Player aliases saved to {filename} - {len(player_aliases)} aliases")
    except Exception as e:
        print(f"Error saving player aliases: {e}")


def load_aliases_from_file(filename: str = "player_aliases.json"):
    global player_aliases
    try:
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                data = json.load(f)


            loaded_aliases = data.get("player_aliases", {})
            player_aliases.clear()
            player_aliases.update(loaded_aliases)

            last_updated = data.get("last_updated", "Unknown")
            total_count = len(player_aliases)

            print(f"Player aliases loaded from {filename} - {total_count} aliases (last updated: {last_updated})")
        else:
            print(f"No existing alias file found at {filename}, starting with empty alias storage")
    except Exception as e:
        print(f"Error loading player aliases: {e}")

        player_aliases.clear()


def store_player_alias(steam_id: str, alias: str, save_immediately: bool = False):
    if not validate_steamid(steam_id) or not alias or alias == "Unknown Player":
        return False


    if steam_id not in player_aliases or player_aliases[steam_id] != alias:
        player_aliases[steam_id] = alias
        if save_immediately:
            save_aliases_to_file()
        return True
    return False


def get_player_alias(steam_id: str) -> str:
    return player_aliases.get(steam_id, "Unknown Player")


def batch_store_aliases_from_profiles(profiles: list, save_after: bool = True):
    stored_count = 0

    for profile in profiles:
        profile_name = profile.get('name', '')
        if profile_name.startswith('/steam/'):
            steam_id = profile_name.replace('/steam/', '')
            alias = profile.get('alias', 'Unknown Player')

            if store_player_alias(steam_id, alias, save_immediately=False):
                stored_count += 1

    if stored_count > 0 and save_after:
        save_aliases_to_file()
        print(f"Batch stored {stored_count} new/updated aliases")

    return stored_count





def load_match_data_from_file(filename: str = "match_data.json"):
    global stored_matches, processed_match_ids
    try:
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                data = json.load(f)


            loaded_matches = data.get("stored_matches", {})
            stored_matches.clear()

            for match_id, match_data in loaded_matches.items():
                stored_matches[match_id] = MatchData.from_dict(match_data)


            processed_match_ids.clear()
            processed_match_ids.update(set(data.get("processed_match_ids", [])))

            print(
                f"Match data loaded from {filename} - {len(stored_matches)} matches, {len(processed_match_ids)} processed IDs")
    except Exception as e:
        print(f"Error loading match data: {e}")


def save_match_data_to_file(filename: str = "match_data.json"):
    try:

        serializable_data = {
            match_id: match_data.to_dict()
            for match_id, match_data in stored_matches.items()
        }

        data = {
            "stored_matches": serializable_data,
            "processed_match_ids": list(processed_match_ids)
        }

        with open(filename, 'w') as f:
            json.dump(data, f, separators=(',', ':'))
        print(f"Match data saved to {filename}")
    except Exception as e:
        print(f"Error saving match data: {e}")


def store_match_from_history(match: dict, profiles: dict, batch_mode: bool = False) -> bool:
    try:
        match_id = str(match['id'])


        if match_id in processed_match_ids:
            return False


        if match.get('matchtype_id') != 1:
            return False


        members = match.get('matchhistorymember', [])
        if len(members) != 2:
            return False


        results_map = {
            result['profile_id']: result['resulttype']
            for result in match.get('matchhistoryreportresults', [])
        }


        players_data = []
        for member in members:
            profile_id = member['profile_id']


            steam_id = None
            alias = "Unknown Player"

            for profile in profiles:
                if profile['profile_id'] == profile_id:
                    profile_name = profile.get('name', '')
                    if profile_name.startswith('/steam/'):
                        steam_id = profile_name.replace('/steam/', '')
                        alias = profile.get('alias', 'Unknown Player')
                        break

            if not steam_id or not validate_steamid(steam_id):
                return False

            race_id = member['race_id']
            race_name = RACE_MAP.get(race_id, f"Race {race_id}")

            result_type = results_map.get(profile_id, -1)
            is_winner = result_type == 1

            players_data.append({
                'steam_id': steam_id,
                'alias': alias,
                'race_name': race_name,
                'old_elo': member['oldrating'],
                'new_elo': member['newrating'],
                'is_winner': is_winner
            })


        winner_data = next((p for p in players_data if p['is_winner']), None)
        if not winner_data:
            return False


        for player in players_data:
            store_player_alias(player['steam_id'], player['alias'], save_immediately=False)


        match_data = MatchData(
            match_id=match_id,
            map_name=match.get('mapname', 'Unknown Map'),
            start_time=match.get('startgametime', 0),
            completion_time=match.get('completiontime', 0),
            player1_steamid=players_data[0]['steam_id'],
            player1_alias=players_data[0]['alias'],
            player1_race=players_data[0]['race_name'],
            player1_old_elo=players_data[0]['old_elo'],
            player1_new_elo=players_data[0]['new_elo'],
            player2_steamid=players_data[1]['steam_id'],
            player2_alias=players_data[1]['alias'],
            player2_race=players_data[1]['race_name'],
            player2_old_elo=players_data[1]['old_elo'],
            player2_new_elo=players_data[1]['new_elo'],
            winner_steamid=winner_data['steam_id'],
            winner_race=winner_data['race_name']
        )


        stored_matches[match_id] = match_data
        processed_match_ids.add(match_id)

        if not batch_mode:
            save_match_data_to_file()
            save_aliases_to_file()
            print(f"Stored new match: {match_id} - {match_data.player1_alias} vs {match_data.player2_alias}")

        return True

    except Exception as e:
        print(f"Error storing match {match.get('id', 'unknown')}: {e}")
        return False


def get_stored_match_count() -> int:
    return len(stored_matches)

def get_map_race_statistics(map_name: str, min_elo: int = None, max_elo: int = None) -> dict:
   map_stats = {
       'map_name': map_name,
       'total_matches': 0,
       'race_stats': {},
       'elo_range': f"{min_elo or 'Any'}-{max_elo or 'Any'}"
   }



   for race in FACTIONS.values():
       map_stats['race_stats'][race] = {
           'wins': 0,
           'losses': 0,
           'total_games': 0,
           'winrate': 0.0
       }



   filtered_matches = filter_matches_by_elo_range(min_elo, max_elo)



   for match in filtered_matches:
       if match.map_name == map_name:
           map_stats['total_matches'] += 1



           winner_race = match.winner_race
           loser_race = match.player2_race if match.player1_race == winner_race else match.player1_race



           if winner_race in map_stats['race_stats']:
               map_stats['race_stats'][winner_race]['wins'] += 1
               map_stats['race_stats'][winner_race]['total_games'] += 1



           if loser_race in map_stats['race_stats']:
               map_stats['race_stats'][loser_race]['losses'] += 1
               map_stats['race_stats'][loser_race]['total_games'] += 1



   for race, stats in map_stats['race_stats'].items():
       if stats['total_games'] > 0:
           stats['winrate'] = (stats['wins'] / stats['total_games']) * 100


   return map_stats

def get_matches_by_player(steam_id: str, limit: int = 10) -> List[MatchData]:
   player_matches = []


   for match in stored_matches.values():
       if match.player1_steamid == steam_id or match.player2_steamid == steam_id:
           player_matches.append(match)



   player_matches.sort(key=lambda m: m.completion_time, reverse=True)
   return player_matches[:limit]





async def fetch_json(url: str) -> dict:
    try:
        session = await connection_manager.get_session()
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                return data
            else:
                logger.warning(f"HTTP {response.status} for {url}")
                return {}
    except Exception as e:
        logger.error(f"HTTP request failed: {e}")
        return {}

def get_race_specific_matchups(target_race: str, min_elo: int = None, max_elo: int = None) -> dict:
   race_stats = {
       'race': target_race,
       'total_matches': 0,
       'total_wins': 0,
       'total_losses': 0,
       'overall_winrate': 0.0,
       'matchups': {},
       'elo_range': f"{min_elo or 'Any'}-{max_elo or 'Any'}"
   }



   for race in FACTIONS.values():
       if race != target_race:
           race_stats['matchups'][race] = {
               'opponent': race,
               'wins': 0,
               'losses': 0,
               'total': 0,
               'winrate': 0.0
           }



   filtered_matches = filter_matches_by_elo_range(min_elo, max_elo)



   for match in filtered_matches:
       if target_race in [match.player1_race, match.player2_race]:

           if match.player1_race == target_race:
               opponent_race = match.player2_race
               target_won = match.winner_race == target_race
           else:
               opponent_race = match.player1_race
               target_won = match.winner_race == target_race


           race_stats['total_matches'] += 1


           if opponent_race in race_stats['matchups']:
               race_stats['matchups'][opponent_race]['total'] += 1


               if target_won:
                   race_stats['total_wins'] += 1
                   race_stats['matchups'][opponent_race]['wins'] += 1
               else:
                   race_stats['total_losses'] += 1
                   race_stats['matchups'][opponent_race]['losses'] += 1



   if race_stats['total_matches'] > 0:
       race_stats['overall_winrate'] = (race_stats['total_wins'] / race_stats['total_matches']) * 100


   for matchup in race_stats['matchups'].values():
       if matchup['total'] > 0:
           matchup['winrate'] = (matchup['wins'] / matchup['total']) * 100


   return race_stats

async def fetch_personal_stats_by_steamid(steam_id: str) -> dict:
    profile_name = f'"/steam/{steam_id}"'
    url = f'{BASE_URL}/getPersonalStat?&title=dow1-de&profile_names=[{profile_name}]'

    data = await fetch_json(url)
    return data if data.get('result', {}).get('code') == 0 else {}


async def fetch_personal_stats_by_alias(alias: str) -> dict:
    import urllib.parse
    encoded_alias = urllib.parse.quote(f'"{alias}"')
    url = f'{BASE_URL}/getPersonalStat?&title=dow1-de&aliases=[{encoded_alias}]'

    data = await fetch_json(url)
    return data if data.get('result', {}).get('code') == 0 else {}


def extract_player_info_from_personal_stats(data: dict) -> Tuple[str, str]:
    steam_id = None
    alias = "Unknown Player"


    stat_groups = data.get('statGroups', [])
    for stat_group in stat_groups:
        members = stat_group.get('members', [])
        for member in members:
            member_name = member.get('name', '')
            if member_name.startswith('/steam/'):
                steam_id = member_name.replace('/steam/', '')
                alias = member.get('alias', 'Unknown Player')
                break
        if steam_id:
            break

    return steam_id, alias


def process_leaderboard_stats(leaderboard_stats: list) -> dict:
    faction_data = {}

    for stat in leaderboard_stats:
        leaderboard_id = stat.get('leaderboard_id')


        if leaderboard_id in FACTIONS:
            wins = stat.get('wins', 0)
            losses = stat.get('losses', 0)
            drops = stat.get('drops', 0)
            rank = stat.get('rank', -1)
            rating = stat.get('rating', 0)


            actual_losses = max(losses - drops, 0)
            total_games = wins + actual_losses
            winrate = (wins / total_games * 100) if total_games > 0 else 0


            rank_display = rank if rank > 0 else None

            faction_data[leaderboard_id] = {
                'faction_name': FACTIONS[leaderboard_id],
                'wins': wins,
                'losses': actual_losses,
                'total_games': total_games,
                'winrate': winrate,
                'rank': rank_display,
                'rating': rating
            }

    return faction_data



async def fetch_leaderboard_data(leaderboard_id: int, start: int = 1, count: int = 20) -> dict:
    url = f'{BASE_URL}/getleaderboard2?count={count}&leaderboard_id={leaderboard_id}&start={start}&sortBy=1&title=dow1-de'
    return await fetch_json(url)


async def get_live_race_leaderboard(race_id: int, steam_id: str = None, limit: int = 10, max_pages: int = 20) -> list:

    race_players = []
    start = 1
    count = min(limit, 200)
    pages_scanned = 0

    while True:
        lb_data = await fetch_leaderboard_data(race_id, start, count)
        stat_groups = lb_data.get("statGroups", [])

        if not stat_groups:
            break

        for stat_group in stat_groups:
            members = stat_group.get("members", [])
            if not members:
                continue

            member = members[0]
            member_name = member.get("name", "")
            if not member_name.startswith("/steam/"):
                continue

            player_id = member_name.replace("/steam/", "")
            alias = member.get("alias", "Unknown Player")


            rating, rank = 0, -1
            for lb_stat in stat_group.get("leaderboardStats", []):
                if lb_stat.get("leaderboard_id") == race_id:
                    rating = lb_stat.get("rating", 0)
                    rank = lb_stat.get("rank", -1)
                    break

            player_data = {
                "steamid": player_id,
                "alias": alias,
                "rating": rating,
                "rank": rank if rank > 0 else None
            }


            if steam_id and player_id == steam_id:
                return [player_data]


            if not steam_id and len(race_players) < limit:
                race_players.append(player_data)


        if steam_id and pages_scanned >= max_pages:
            break
        if not steam_id and len(stat_groups) < count:
            break

        start += count
        pages_scanned += 1

    return race_players



@lru_cache(maxsize=64)
def validate_steamid(steam_id: str) -> bool:
    return steam_id.isdigit() and len(steam_id) == 17

def filter_matches_by_elo_range(min_elo: int = None, max_elo: int = None) -> List[MatchData]:
   if min_elo is None and max_elo is None:
       return list(stored_matches.values())


   filtered_matches = []


   for match in stored_matches.values():

       player1_elo = match.player1_old_elo
       player2_elo = match.player2_old_elo



       player1_in_range = True
       player2_in_range = True


       if min_elo is not None:
           player1_in_range = player1_in_range and player1_elo >= min_elo
           player2_in_range = player2_in_range and player2_elo >= min_elo


       if max_elo is not None:
           player1_in_range = player1_in_range and player1_elo <= max_elo
           player2_in_range = player2_in_range and player2_elo <= max_elo



       if player1_in_range and player2_in_range:
           filtered_matches.append(match)


   return filtered_matches


def get_all_race_matchups(min_elo: int = None, max_elo: int = None) -> dict:
   matchup_stats = {}



   races = list(FACTIONS.values())
   for i, race1 in enumerate(races):
       for j, race2 in enumerate(races):
           if i <= j:
               if race1 == race2:
                   key = f"{race1} vs {race2}"
               else:
                   key = f"{race1} vs {race2}"
               matchup_stats[key] = {
                   'race1': race1,
                   'race2': race2,
                   'race1_wins': 0,
                   'race2_wins': 0,
                   'total_matches': 0,
                   'race1_winrate': 0.0,
                   'race2_winrate': 0.0,
                   'elo_range': f"{min_elo or 'Any'}-{max_elo or 'Any'}"
               }



   filtered_matches = filter_matches_by_elo_range(min_elo, max_elo)



   for match in filtered_matches:
       race1 = match.player1_race
       race2 = match.player2_race



       if race1 == race2:
           key = f"{race1} vs {race2}"
       else:
           sorted_races = sorted([race1, race2])
           key = f"{sorted_races[0]} vs {sorted_races[1]}"


       if key in matchup_stats:
           matchup_stats[key]['total_matches'] += 1



           winner_race = match.winner_race
           if winner_race == race1:
               if race1 == matchup_stats[key]['race1']:
                   matchup_stats[key]['race1_wins'] += 1
               else:
                   matchup_stats[key]['race2_wins'] += 1
           elif winner_race == race2:
               if race2 == matchup_stats[key]['race1']:
                   matchup_stats[key]['race1_wins'] += 1
               else:
                   matchup_stats[key]['race2_wins'] += 1



   for stats in matchup_stats.values():
       total = stats['total_matches']
       if total > 0:
           stats['race1_winrate'] = (stats['race1_wins'] / total) * 100
           stats['race2_winrate'] = (stats['race2_wins'] / total) * 100


   return matchup_stats



@app_commands.command(name="racematchups", description="Show win/loss statistics for a specific race against all others")
@app_commands.describe(
   race="Choose a race to see its matchup statistics",
   min_elo="Minimum ELO rating (optional)",
   max_elo="Maximum ELO rating (optional)"
)
@app_commands.choices(race=[
   app_commands.Choice(name="Chaos", value="Chaos"),
   app_commands.Choice(name="Dark Eldar", value="Dark Eldar"),
   app_commands.Choice(name="Eldar", value="Eldar"),
   app_commands.Choice(name="Imperial Guard", value="Imperial Guard"),
   app_commands.Choice(name="Necrons", value="Necrons"),
   app_commands.Choice(name="Orks", value="Orks"),
   app_commands.Choice(name="Sisters of Battle", value="Sisters of Battle"),
   app_commands.Choice(name="Space Marines", value="Space Marines"),
   app_commands.Choice(name="Tau Empire", value="Tau Empire"),
])
async def slash_race_matchups(interaction: discord.Interaction, race: str, min_elo: int = None, max_elo: int = None):
   await interaction.response.defer()



   if min_elo is not None and min_elo < 0:
       await interaction.followup.send("Minimum ELO must be 0 or higher.", ephemeral=True)
       return


   if max_elo is not None and max_elo < 0:
       await interaction.followup.send("Maximum ELO must be 0 or higher.", ephemeral=True)
       return


   if min_elo is not None and max_elo is not None and min_elo > max_elo:
       await interaction.followup.send("Minimum ELO cannot be higher than maximum ELO.", ephemeral=True)
       return


   if get_stored_match_count() == 0:
       await interaction.followup.send(
           "No match data available yet. Use commands that fetch match history to populate data.", ephemeral=True)
       return

   race_stats = get_race_specific_matchups(race, min_elo, max_elo)


   if race_stats['total_matches'] == 0:
       elo_text = f" in ELO range {race_stats['elo_range']}" if min_elo or max_elo else ""
       await interaction.followup.send(f"No matches found for {race}{elo_text} in stored data.", ephemeral=True)
       return


   title = f"{race} Matchup Statistics"
   if min_elo or max_elo:
       title += f" (ELO: {race_stats['elo_range']})"


   embed = discord.Embed(title=title, color=0x9B59B6)


   embed.add_field(
       name="Overall Performance",
       value=f"**Total Matches:** {race_stats['total_matches']}\n"
             f"**Wins:** {race_stats['total_wins']}\n"
             f"**Losses:** {race_stats['total_losses']}\n"
             f"**Win Rate:** {race_stats['overall_winrate']:.1f}%",
       inline=False
   )


   good_matchups = []
   bad_matchups = []


   for opponent, stats in race_stats['matchups'].items():
       if stats['total'] > 0:
           matchup_text = f"vs {opponent}: {stats['wins']}-{stats['losses']} ({stats['winrate']:.1f}%) [{stats['total']} games]"


           if stats['winrate'] >= 50:
               good_matchups.append(matchup_text)
           else:
               bad_matchups.append(matchup_text)



   good_matchups.sort(key=lambda x: float(x.split('(')[1].split('%')[0]), reverse=True)
   bad_matchups.sort(key=lambda x: float(x.split('(')[1].split('%')[0]), reverse=True)


   if good_matchups:
       embed.add_field(
           name="Favorable Matchups (≥50% WR)",
           value="\n".join(good_matchups[:8]),
           inline=False
       )


   if bad_matchups:
       embed.add_field(
           name="Challenging Matchups (<50% WR)",
           value="\n".join(bad_matchups[:8]),
           inline=False
       )


   footer_text = f"Based on {race_stats['total_matches']} {race} matches from stored database"
   if min_elo or max_elo:
       footer_text += f" (ELO range: {race_stats['elo_range']})"


   embed.set_footer(text=footer_text)
   await interaction.followup.send(embed=embed)


async def slash_all_matchups(interaction: discord.Interaction, min_elo: int = None, max_elo: int = None):
   await interaction.response.defer()


   if min_elo is not None and min_elo < 0:
       await interaction.followup.send("Minimum ELO must be 0 or higher.", ephemeral=True)
       return


   if max_elo is not None and max_elo < 0:
       await interaction.followup.send("Maximum ELO must be 0 or higher.", ephemeral=True)
       return

   if min_elo is not None and max_elo is not None and min_elo > max_elo:
       await interaction.followup.send("Minimum ELO cannot be higher than maximum ELO.", ephemeral=True)
       return

   if get_stored_match_count() == 0:
       await interaction.followup.send(
           "No match data available yet. Use commands that fetch match history to populate data.", ephemeral=True)
       return


   all_matchups = get_all_race_matchups(min_elo, max_elo)


   active_matchups = {k: v for k, v in all_matchups.items() if v['total_matches'] > 0}


   if not active_matchups:
       elo_text = f" in ELO range {min_elo or 'Any'}-{max_elo or 'Any'}" if min_elo or max_elo else ""
       await interaction.followup.send(f"No matchup data found{elo_text} in stored matches.", ephemeral=True)
       return



   sorted_matchups = sorted(active_matchups.items(), key=lambda x: x[1]['total_matches'], reverse=True)


   title = "All Race Matchup Statistics"
   description = f"Win rates for all race combinations ({len(active_matchups)} active matchups"
   if min_elo or max_elo:
       title += f" (ELO: {min_elo or 'Any'}-{max_elo or 'Any'})"
       description += f", ELO range: {min_elo or 'Any'}-{max_elo or 'Any'}"
   description += ")"


   embed = discord.Embed(title=title, color=0xE74C3C)
   embed.description = description



   field_count = 0
   current_field_content = []


   for matchup_name, stats in sorted_matchups:
       if stats['total_matches'] >= 5:

           if stats['race1'] == stats['race2']:
               line = f"**{matchup_name}:** {stats['total_matches']} matches"
           else:
               line = f"**{stats['race1']}** vs **{stats['race2']}:** {stats['race1_winrate']:.0f}%-{stats['race2_winrate']:.0f}% ({stats['total_matches']} games)"


           current_field_content.append(line)

           if len(current_field_content) >= 8:
               field_count += 1
               embed.add_field(
                   name=f"Popular Matchups {field_count}",
                   value="\n".join(current_field_content),
                   inline=False
               )
               current_field_content = []

   if current_field_content:
       field_count += 1
       embed.add_field(
           name=f"Popular Matchups {field_count}" if field_count > 1 else "Popular Matchups",
           value="\n".join(current_field_content),
           inline=False
       )

   total_matchups = len(active_matchups)
   total_matches = sum(stats['total_matches'] for stats in active_matchups.values())

   summary_text = f"**Total Active Matchups:** {total_matchups}\n**Total Matches Analyzed:** {total_matches}\n**Minimum Games Shown:** 5"
   if min_elo or max_elo:
       summary_text += f"\n**ELO Range:** {min_elo or 'Any'}-{max_elo or 'Any'}"

   embed.add_field(
       name="Summary",
       value=summary_text,
       inline=False
   )

   footer_text = "Only showing matchups with 5+ games for statistical relevance"
   if min_elo or max_elo:
       footer_text += f" | ELO range: {min_elo or 'Any'}-{max_elo or 'Any'}"

   embed.set_footer(text=footer_text)
   await interaction.followup.send(embed=embed)

def find_steamid_by_alias(alias: str) -> Optional[str]:
    alias_lower = alias.lower()


    for steamid, stored_alias in player_aliases.items():
        if stored_alias.lower() == alias_lower:
            return steamid


    matches = []
    for steamid, stored_alias in player_aliases.items():
        if alias_lower in stored_alias.lower():
            matches.append(steamid)
            if len(matches) >= 50:
                break

    return matches[0] if len(matches) == 1 else None

def resolve_player_identifier(identifier: str) -> Tuple[Optional[str], str, List[str]]:
    if validate_steamid(identifier):
        alias = player_aliases.get(identifier, "Unknown Player")
        return identifier, alias, []

    steamid = find_steamid_by_alias(identifier)
    if steamid:
        alias = player_aliases.get(steamid, "Unknown Player")
        return steamid, alias, []


    identifier_lower = identifier.lower()
    similar_matches = []

    for stored_alias in player_aliases.values():
        if identifier_lower in stored_alias.lower():
            similar_matches.append(stored_alias)
            if len(similar_matches) >= 5:
                break

    return None, identifier, similar_matches

def create_embed_base(title: str, steam_id: str, alias: str, color: int) -> discord.Embed:
    description = f"`{alias}` (SteamID: {steam_id})" if alias != 'Unknown Player' else f"SteamID: {steam_id}"
    return discord.Embed(title=title, description=description, color=color)


@app_commands.command(name="mapstats", description="Show race win rates on a specific map")
@app_commands.describe(
   map_name="Choose a map to see race performance statistics",
   min_elo="Minimum ELO rating (optional)",
   max_elo="Maximum ELO rating (optional)"
)
async def slash_map_stats(interaction: discord.Interaction, map_name: str, min_elo: int = None,
                                  max_elo: int = None):
   await interaction.response.defer()



   if min_elo is not None and min_elo < 0:
       await interaction.followup.send("Minimum ELO must be 0 or higher.", ephemeral=True)
       return


   if max_elo is not None and max_elo < 0:
       await interaction.followup.send("Maximum ELO must be 0 or higher.", ephemeral=True)
       return


   if min_elo is not None and max_elo is not None and min_elo > max_elo:
       await interaction.followup.send("Minimum ELO cannot be higher than maximum ELO.", ephemeral=True)
       return


   if get_stored_match_count() == 0:
       await interaction.followup.send(
           "No match data available yet. Use commands that fetch match history to populate data.", ephemeral=True)
       return


   map_stats = get_map_race_statistics(map_name, min_elo, max_elo)


   if map_stats['total_matches'] == 0:
       elo_text = f" in ELO range {map_stats['elo_range']}" if min_elo or max_elo else ""
       await interaction.followup.send(f"No matches found for map '{map_name}'{elo_text} in stored data.",
                                       ephemeral=True)
       return


   title = f"Race Statistics on {map_name}"
   description = f"Win rates for each race on this map ({map_stats['total_matches']} total matches"
   if min_elo or max_elo:
       title += f" (ELO: {map_stats['elo_range']})"
       description += f", ELO range: {map_stats['elo_range']}"
   description += ")"


   embed = discord.Embed(title=title, color=0x3498DB)
   embed.description = description



   sorted_races = []
   for race, stats in map_stats['race_stats'].items():
       if stats['total_games'] > 0:
           sorted_races.append((race, stats))


   sorted_races.sort(key=lambda x: x[1]['winrate'], reverse=True)


   if not sorted_races:
       await interaction.followup.send(f"No race data found for map '{map_name}' in the specified ELO range.",
                                       ephemeral=True)
       return



   excellent_races = []
   good_races = []
   average_races = []
   poor_races = []


   for race, stats in sorted_races:
       race_text = f"**{race}:** {stats['winrate']:.1f}% ({stats['wins']}-{stats['losses']}, {stats['total_games']} games)"


       if stats['winrate'] >= 70:
           excellent_races.append(race_text)
       elif stats['winrate'] >= 50:
           good_races.append(race_text)
       elif stats['winrate'] >= 40:
           average_races.append(race_text)
       else:
           poor_races.append(race_text)



   if excellent_races:
       embed.add_field(
           name="Dominant Races (70%+ WR)",
           value="\n".join(excellent_races),
           inline=False
       )


   if good_races:
       embed.add_field(
           name="Strong Races (50-69% WR)",
           value="\n".join(good_races),
           inline=False
       )


   if average_races:
       embed.add_field(
           name="Average Races (40-49% WR)",
           value="\n".join(average_races),
           inline=False
       )


   if poor_races:
       embed.add_field(
           name="Struggling Races (<40% WR)",
           value="\n".join(poor_races),
           inline=False
       )



   total_games_all_races = sum(stats['total_games'] for _, stats in sorted_races)
   most_played_race = max(sorted_races, key=lambda x: x[1]['total_games'])
   best_performing_race = sorted_races[0]


   embed.add_field(
       name="Map Summary",
       value=f"**Total Race Appearances:** {total_games_all_races}\n"
             f"**Most Played Race:** {most_played_race[0]} ({most_played_race[1]['total_games']} games)\n"
             f"**Best Performing Race:** {best_performing_race[0]} ({best_performing_race[1]['winrate']:.1f}% WR)",
       inline=False
   )


   footer_text = f"Based on {map_stats['total_matches']} matches on this map"
   if min_elo or max_elo:
       footer_text += f" (ELO range: {map_stats['elo_range']})"


   embed.set_footer(text=footer_text)
   await interaction.followup.send(embed=embed)




@slash_map_stats.autocomplete('map_name')
async def map_name_autocomplete(
       interaction: discord.Interaction,
       current: str,
) -> List[app_commands.Choice[str]]:

   map_counts = defaultdict(int)

   for match in stored_matches.values():
       map_counts[match.map_name] += 1

   valid_maps = [(map_name, count) for map_name, count in map_counts.items() if count >= 3]

   if current:
       valid_maps = [(map_name, count) for map_name, count in valid_maps
                     if current.lower() in map_name.lower()]


   valid_maps.sort(key=lambda x: x[1], reverse=True)

   choices = []
   for map_name, count in valid_maps[:25]:
       display_name = map_name if len(map_name) <= 90 else map_name[:87] + "..."
       choices.append(app_commands.Choice(
           name=f"{display_name} ({count} matches)",
           value=map_name
       ))

   return choices




@app_commands.command(name="allmatchups", description="Show win rates for all race combinations")
@app_commands.describe(
   min_elo="Minimum ELO rating (optional)",
   max_elo="Maximum ELO rating (optional)"
)
async def slash_all_matchups(interaction: discord.Interaction, min_elo: int = None, max_elo: int = None):
   await interaction.response.defer()

   if min_elo is not None and min_elo < 0:
       await interaction.followup.send("Minimum ELO must be 0 or higher.", ephemeral=True)
       return


   if max_elo is not None and max_elo < 0:
       await interaction.followup.send("Maximum ELO must be 0 or higher.", ephemeral=True)
       return


   if min_elo is not None and max_elo is not None and min_elo > max_elo:
       await interaction.followup.send("Minimum ELO cannot be higher than maximum ELO.", ephemeral=True)
       return


   if get_stored_match_count() == 0:
       await interaction.followup.send(
           "No match data available yet. Use commands that fetch match history to populate data.", ephemeral=True)
       return


   all_matchups = get_all_race_matchups(min_elo, max_elo)
   active_matchups = {k: v for k, v in all_matchups.items() if v['total_matches'] > 0}


   if not active_matchups:
       elo_text = f" in ELO range {min_elo or 'Any'}-{max_elo or 'Any'}" if min_elo or max_elo else ""
       await interaction.followup.send(f"No matchup data found{elo_text} in stored matches.", ephemeral=True)
       return

   sorted_matchups = sorted(active_matchups.items(), key=lambda x: x[1]['total_matches'], reverse=True)


   title = "All Race Matchup Statistics"
   description = f"Win rates for all race combinations ({len(active_matchups)} active matchups"
   if min_elo or max_elo:
       title += f" (ELO: {min_elo or 'Any'}-{max_elo or 'Any'})"
       description += f", ELO range: {min_elo or 'Any'}-{max_elo or 'Any'}"
   description += ")"


   embed = discord.Embed(title=title, color=0xE74C3C)
   embed.description = description

   field_count = 0
   current_field_content = []


   for matchup_name, stats in sorted_matchups:
       if stats['total_matches'] >= 5:
           if stats['race1'] == stats['race2']:
               line = f"**{matchup_name}:** {stats['total_matches']} matches"
           else:
               line = f"**{stats['race1']}** vs **{stats['race2']}:** {stats['race1_winrate']:.0f}%-{stats['race2_winrate']:.0f}% ({stats['total_matches']} games)"


           current_field_content.append(line)

           if len(current_field_content) >= 8:
               field_count += 1
               embed.add_field(
                   name=f"Popular Matchups {field_count}",
                   value="\n".join(current_field_content),
                   inline=False
               )
               current_field_content = []

   if current_field_content:
       field_count += 1
       embed.add_field(
           name=f"Popular Matchups {field_count}" if field_count > 1 else "Popular Matchups",
           value="\n".join(current_field_content),
           inline=False
       )

   total_matchups = len(active_matchups)
   total_matches = sum(stats['total_matches'] for stats in active_matchups.values())


   summary_text = f"**Total Active Matchups:** {total_matchups}\n**Total Matches Analyzed:** {total_matches}\n**Minimum Games Shown:** 5"
   if min_elo or max_elo:
       summary_text += f"\n**ELO Range:** {min_elo or 'Any'}-{max_elo or 'Any'}"


   embed.add_field(
       name="Summary",
       value=summary_text,
       inline=False
   )


   footer_text = "Only showing matchups with 5+ games for statistical relevance"
   if min_elo or max_elo:
       footer_text += f" | ELO range: {min_elo or 'Any'}-{max_elo or 'Any'}"


   embed.set_footer(text=footer_text)
   await interaction.followup.send(embed=embed)



def calculate_winrate_stats(stats: list, lb_ids: range = range(1, 10)) -> dict:
    total_wins = 0
    total_losses = 0
    filtered_stats = []

    for entry in stats:
        if entry.get("leaderboard_id") in lb_ids:
            wins = entry.get("wins", 0)
            losses = max(entry.get("losses", 0) - entry.get("drops", 0), 0)
            total_wins += wins
            total_losses += losses
            filtered_stats.append(entry)

    total_games = total_wins + total_losses
    winrate = (total_wins / total_games * 100) if total_games > 0 else 0

    return {
        'wins': total_wins,
        'losses': total_losses,
        'games': total_games,
        'winrate': winrate,
        'stats': filtered_stats
    }

async def fetch_match_history(steam_id: str) -> dict:
    profile_name = f'"/steam/{steam_id}"'
    url = f'{BASE_URL}/getRecentMatchHistory?title=dow1-de&profile_names=[{profile_name}]'

    data = await fetch_json(url)
    return data if data.get('result', {}).get('code') == 0 else {}

async def fetch_match_history_alias(alias: str) -> dict:
    import urllib.parse
    encoded_alias = urllib.parse.quote(f'"{alias}"')
    url = f'{BASE_URL}/getRecentMatchHistory?title=dow1-de&aliases=[{encoded_alias}]'

    data = await fetch_json(url)
    return data if data.get('result', {}).get('code') == 0 else {}


def filter_1v1_matches(matches: list) -> list:
    filtered = (match for match in matches if match.get('matchtype_id') == 1)
    return sorted(filtered, key=lambda m: m['completiontime'], reverse=True)


def format_match_embed(match: dict, profiles: dict, is_latest: bool = False) -> discord.Embed:
    match_id = match['id']
    map_name = match['mapname']
    start_time = datetime.fromtimestamp(match['startgametime']).strftime('%Y-%m-%d %H:%M:%S')
    end_time = datetime.fromtimestamp(match['completiontime']).strftime('%Y-%m-%d %H:%M:%S')

    title = "Latest 1v1 Match" if is_latest else f"Match {match_id}"
    embed = discord.Embed(title=title, color=0xFF8C00)

    embed.add_field(name="Map", value=map_name, inline=True)
    embed.add_field(name="Match ID", value=match_id, inline=True)
    embed.add_field(name="Duration", value=f"{start_time} - {end_time}", inline=False)

    results_map = {
        result['profile_id']: {
            'resulttype': result['resulttype'],
            'xp': result['xpgained']
        }
        for result in match['matchhistoryreportresults']
    }

    player_info = []
    for member in match['matchhistorymember']:
        profile_id = member['profile_id']
        alias = profiles.get(profile_id, f"ID {profile_id}")
        race_name = RACE_MAP.get(member['race_id'], f"Race ID {member['race_id']}")
        old_rating = member['oldrating']
        new_rating = member['newrating']
        rating_change = new_rating - old_rating

        result_type = results_map.get(profile_id, {}).get('resulttype')
        outcome = 'Win' if result_type == 1 else 'Loss' if result_type == 0 else 'Unknown'

        rating_emoji = "📈" if rating_change > 0 else "📉" if rating_change < 0 else "➡️"
        outcome_emoji = "🏆" if outcome == 'Win' else "💀" if outcome == 'Loss' else "❓"

        player_text = f"{outcome_emoji} **{alias}** ({race_name})\n{rating_emoji} {old_rating} → {new_rating} ({rating_change:+d})"
        player_info.append(player_text)

    embed.add_field(name="Players", value="\n\n".join(player_info), inline=False)
    return embed


async def extract_elos(steam_id: str, batch_mode: bool = False):

    match_data = await fetch_match_history(steam_id)

    if not match_data:
        return

    profiles = match_data.get('profiles', [])
    player_profile_id = None
    player_alias = "Unknown Player"

    for profile in profiles:
        if profile.get('name') == f"/steam/{steam_id}":
            player_profile_id = profile['profile_id']
            player_alias = profile.get('alias', 'Unknown Player')
            break

    if not player_profile_id:
        return

    store_player_alias(steam_id, player_alias, save_immediately=False)

    matches = filter_1v1_matches(match_data.get('matchHistoryStats', []))
    if not matches:
        return

    matches_stored = 0

    for match in matches:
        if store_match_from_history(match, profiles, batch_mode=True):
            matches_stored += 1

    batch_store_aliases_from_profiles(profiles, save_after=not batch_mode)

    if not batch_mode and matches_stored > 0:
        save_match_data_to_file()
        print(f"Stored {matches_stored} new matches for {player_alias}")



async def bulk_scan_for_matches(update_progress):
    results = {
        'total_players_processed': 0,
        'new_players_found': 0,
        'total_matches_added': 0,
        'new_aliases_stored': 0,
        'errors': 0,
        'leaderboard_results': {}
    }

    known_leaderboards = [
        {'id': 1, 'name': 'Chaos 1v1'},
        {'id': 2, 'name': 'Dark Eldar 1v1'},
        {'id': 3, 'name': 'Eldar 1v1'},
        {'id': 4, 'name': 'Guard 1v1'},
        {'id': 5, 'name': 'Necron 1v1'},
        {'id': 6, 'name': 'Orc 1v1'},
        {'id': 7, 'name': 'Sisters 1v1'},
        {'id': 8, 'name': 'Space Marine 1v1'},
        {'id': 9, 'name': 'Tau 1v1'}
    ]

    try:
        await update_progress(f"🔍 Scanning {len(known_leaderboards)} leaderboards for match data")

        for lb_index, leaderboard in enumerate(known_leaderboards, 1):
            lb_id = leaderboard['id']
            lb_name = leaderboard['name']

            await update_progress(f"📊 Scanning {lb_name} ({lb_index}/{len(known_leaderboards)})")

            faction_name = FACTIONS.get(lb_id, f"Faction {lb_id}")
            results['leaderboard_results'][faction_name] = 0

            start = 1
            count = 200
            lb_players_processed = 0
            lb_aliases_stored = 0

            while True:
                try:
                    lb_data = await fetch_leaderboard_data(lb_id, start, count)
                    stat_groups = lb_data.get('statGroups', [])

                    if not stat_groups:
                        break

                    for stat_group in stat_groups:
                        try:
                            if not stat_group.get('members'):
                                continue

                            member = stat_group['members'][0]
                            member_name = member.get('name', '')

                            if not member_name.startswith('/steam/'):
                                continue

                            steam_id = member_name.replace('/steam/', '')
                            if not validate_steamid(steam_id):
                                continue

                            alias = member.get('alias', 'Unknown Player')

                            is_new_player = steam_id not in player_aliases
                            if is_new_player:
                                results['new_players_found'] += 1

                            if store_player_alias(steam_id, alias, save_immediately=False):
                                lb_aliases_stored += 1
                                results['new_aliases_stored'] += 1

                            try:
                                pre_match_count = get_stored_match_count()
                                await extract_elos(steam_id, batch_mode=True)
                                post_match_count = get_stored_match_count()
                                matches_added = post_match_count - pre_match_count
                                results['total_matches_added'] += matches_added
                                results['total_players_processed'] += 1
                                lb_players_processed += 1

                            except Exception as player_error:
                                print(f"Error processing player {steam_id}: {player_error}")
                                results['errors'] += 1

                            await asyncio.sleep(0.05)
                        except Exception:
                            results['errors'] += 1
                            continue

                    await update_progress(f"📊 {faction_name}: Processed {lb_players_processed} players, {lb_aliases_stored} new aliases")

                    if len(stat_groups) < count:
                        break
                    start += count
                    if start > 1000:
                        break

                except Exception:
                    results['errors'] += 1
                    break

            results['leaderboard_results'][faction_name] = lb_players_processed
            await asyncio.sleep(0.1)
            gc.collect()

        await update_progress(f"✅ Match scan complete! Processed {results['total_players_processed']} players")
        save_match_data_to_file()
        save_aliases_to_file()

    except Exception as e:
        await update_progress(f"❌ Critical error: {str(e)}")
        print(f"Critical error in bulk_scan_for_matches: {e}")
        results['errors'] += 1

    return results
async def bulk_scan_for_matches(update_progress):
    results = {
        'total_players_processed': 0,
        'new_players_found': 0,
        'total_matches_added': 0,
        'errors': 0,
        'leaderboard_results': {}
    }

    known_leaderboards = [
        {'id': 1, 'name': 'Chaos 1v1'},
        {'id': 2, 'name': 'Dark Eldar 1v1'},
        {'id': 3, 'name': 'Eldar 1v1'},
        {'id': 4, 'name': 'Guard 1v1'},
        {'id': 5, 'name': 'Necron 1v1'},
        {'id': 6, 'name': 'Orc 1v1'},
        {'id': 7, 'name': 'Sisters 1v1'},
        {'id': 8, 'name': 'Space Marine 1v1'},
        {'id': 9, 'name': 'Tau 1v1'}
    ]

    try:
        await update_progress(f"🔍 Scanning {len(known_leaderboards)} leaderboards for match data")

        for lb_index, leaderboard in enumerate(known_leaderboards, 1):
            lb_id = leaderboard['id']
            lb_name = leaderboard['name']

            await update_progress(f"📊 Scanning {lb_name} ({lb_index}/{len(known_leaderboards)})")

            faction_name = FACTIONS.get(lb_id, f"Faction {lb_id}")
            results['leaderboard_results'][faction_name] = 0

            start = 1
            count = 200
            lb_players_processed = 0

            while True:
                try:
                    lb_data = await fetch_leaderboard_data(lb_id, start, count)
                    stat_groups = lb_data.get('statGroups', [])

                    if not stat_groups:
                        break

                    for stat_group in stat_groups:
                        try:
                            if not stat_group.get('members'):
                                continue

                            member = stat_group['members'][0]
                            member_name = member.get('name', '')

                            if not member_name.startswith('/steam/'):
                                continue

                            steam_id = member_name.replace('/steam/', '')
                            if not validate_steamid(steam_id):
                                continue

                            alias = member.get('alias', 'Unknown Player')
                            is_new_player = steam_id not in player_aliases
                            if is_new_player:
                                results['new_players_found'] += 1

                            player_aliases[steam_id] = alias

                            try:
                                pre_match_count = get_stored_match_count()
                                await extract_elos(steam_id, batch_mode=True)
                                post_match_count = get_stored_match_count()

                                matches_added = post_match_count - pre_match_count
                                results['total_matches_added'] += matches_added
                                results['total_players_processed'] += 1
                                lb_players_processed += 1

                            except Exception as player_error:
                                print(f"Error processing player {steam_id}: {player_error}")
                                results['errors'] += 1

                            await asyncio.sleep(0.05)

                        except Exception:
                            results['errors'] += 1
                            continue

                    await update_progress(f"📊 {faction_name}: Processed {lb_players_processed} players")

                    if len(stat_groups) < count:
                        break

                    start += count

                    if start > 1000:
                        break

                except Exception:
                    results['errors'] += 1
                    break

            results['leaderboard_results'][faction_name] = lb_players_processed
            await asyncio.sleep(0.1)
            gc.collect()

        await update_progress(f"✅ Match scan complete! Processed {results['total_players_processed']} players")
        save_match_data_to_file()

    except Exception as e:
        await update_progress(f"❌ Critical error: {str(e)}")
        print(f"Critical error in bulk_scan_for_matches: {e}")
        results['errors'] += 1

    return results


async def fetch_leaderboard_data(leaderboard_id: int, start: int = 1, count: int = 20) -> dict:
    url = f'{BASE_URL}/getleaderboard2?count={count}&leaderboard_id={leaderboard_id}&start={start}&sortBy=1&title=dow1-de'
    return await fetch_json(url)



@app_commands.command(name="factions", description="Show live faction stats (winrate, rank, ELO) for a player")
@app_commands.describe(player="Enter a 17-digit SteamID64 or player alias/name")
async def slash_factions(interaction: discord.Interaction, player: str):
    await interaction.response.defer()

    personal_stats_data = None
    steam_id = None
    alias = "Unknown Player"


    if validate_steamid(player):

        steam_id = player
        personal_stats_data = await fetch_personal_stats_by_steamid(steam_id)
        if personal_stats_data:
            _, alias = extract_player_info_from_personal_stats(personal_stats_data)
    else:
        personal_stats_data = await fetch_personal_stats_by_alias(player)
        if personal_stats_data:
            steam_id, alias = extract_player_info_from_personal_stats(personal_stats_data)
    if not personal_stats_data:
        resolved_steamid, resolved_alias, similar_matches = resolve_player_identifier(player)
        if resolved_steamid:
            personal_stats_data = await fetch_personal_stats_by_steamid(resolved_steamid)
            if personal_stats_data:
                steam_id = resolved_steamid
                alias = resolved_alias

    if not personal_stats_data or not steam_id:
        if not validate_steamid(player):
            await interaction.followup.send(
                f"❌ Player '{player}' not found. Make sure the alias is spelled correctly or use a valid 17-digit SteamID64."
            )
        else:
            await interaction.followup.send(f"⚠️ No stats found for SteamID {player}.")
        return

    player_aliases[steam_id] = alias

    leaderboard_stats = personal_stats_data.get('leaderboardStats', [])
    if not leaderboard_stats:
        await interaction.followup.send(f"⚠️ No faction stats found for {alias}.")
        return

    faction_data = process_leaderboard_stats(leaderboard_stats)

    if not faction_data:
        await interaction.followup.send(f"⚠️ No 1v1 faction data found for {alias}.")
        return

    embed = create_embed_base("Live Faction Statistics", steam_id, alias, 0x2ECC71)

    for lb_id, name in FACTIONS.items():
        if lb_id in faction_data:
            data = faction_data[lb_id]

            rank_text = f"Rank #{data['rank']}" if data['rank'] is not None else "Unranked"

            field_value = (
                f"**Winrate:** {data['winrate']:.1f}% ({data['wins']}W / {data['losses']}L)\n"
                f"**ELO:** {data['rating']} | **{rank_text}**\n"
                f"**Games:** {data['total_games']}"
            )

            embed.add_field(
                name=name,
                value=field_value,
                inline=True
            )
        else:
            embed.add_field(name=name, value="No data", inline=True)

    embed.set_footer(text="Live data from Relic API")
    await interaction.followup.send(embed=embed)


@app_commands.command(name="1v1winrate", description="Show overall 1v1 winrate for a player")
@app_commands.describe(player="Enter a 17-digit SteamID64 or player alias/name")
async def slash_1v1winrate(interaction: discord.Interaction, player: str):
    await interaction.response.defer()

    steamid, alias, similar_matches = resolve_player_identifier(player)

    if not steamid:

        if not validate_steamid(player):
            personal_stats_data = await fetch_personal_stats_by_alias(player)
            if personal_stats_data:
                steamid, alias = extract_player_info_from_personal_stats(personal_stats_data)

    if not steamid:
        if similar_matches:
            suggestion_text = "\n".join(f"• {match}" for match in similar_matches)
            await interaction.followup.send(
                f"❌ Player '{player}' not found. Did you mean one of these?\n```\n{suggestion_text}\n```"
            )
        else:
            await interaction.followup.send(
                f"❌ Player '{player}' not found. Make sure the player exists or use a valid 17-digit SteamID64."
            )
        return


    personal_stats_data = await fetch_personal_stats_by_steamid(steamid)
    if not personal_stats_data:
        await interaction.followup.send(f"⚠️ No stats found for {alias}.")
        return


    _, fetched_alias = extract_player_info_from_personal_stats(personal_stats_data)
    display_alias = fetched_alias if fetched_alias != 'Unknown Player' else alias
    player_aliases[steamid] = display_alias

    leaderboard_stats = personal_stats_data.get('leaderboardStats', [])
    winrate_data = calculate_winrate_stats(leaderboard_stats)

    embed = create_embed_base("1v1 Overall Winrate", steamid, display_alias, 0x3498DB)
    embed.add_field(name="Wins", value=winrate_data['wins'], inline=True)
    embed.add_field(name="Losses", value=winrate_data['losses'], inline=True)
    embed.add_field(name="Winrate", value=f"{winrate_data['winrate']:.2f}%", inline=True)

    await interaction.followup.send(embed=embed)


async def get_faction_leaderboard(race_id: int, start_rank: int = 1, count: int = 50) -> list:

    try:
        lb_data = await fetch_leaderboard_data(race_id, start_rank, count)
        if not lb_data:
            print(f"No leaderboard data returned for race_id {race_id}")
            return []


        leaderboard_stats = lb_data.get("leaderboardStats", [])
        stat_groups = lb_data.get("statGroups", [])

        if not leaderboard_stats:
            print(f"No leaderboardStats found for race_id {race_id}")
            return []

        if not stat_groups:
            print(f"No statGroups found for race_id {race_id}")
            return []

        rank_lookup = {}
        for stat in leaderboard_stats:
            if stat.get("leaderboard_id") == race_id:
                statgroup_id = stat.get("statgroup_id")
                if statgroup_id:
                    rank_lookup[statgroup_id] = {
                        'rank': stat.get('rank'),
                        'rating': stat.get('rating', 0),
                        'wins': stat.get('wins', 0),
                        'losses': stat.get('losses', 0),
                        'drops': stat.get('drops', 0),
                        'streak': stat.get('streak', 0)
                    }

        player_lookup = {}
        for stat_group in stat_groups:
            group_id = stat_group.get("id")
            members = stat_group.get("members", [])

            if group_id and members:
                member = members[0]
                member_name = member.get("name", "")

                if member_name.startswith("/steam/"):
                    steam_id = member_name.replace("/steam/", "")
                    if validate_steamid(steam_id):
                        alias = member.get("alias", "Unknown Player")
                        player_lookup[group_id] = {
                            'steamid': steam_id,
                            'alias': alias
                        }

        print(f"Found {len(rank_lookup)} rank entries and {len(player_lookup)} player entries")
        faction_players = []
        matched_count = 0

        for statgroup_id, rank_info in rank_lookup.items():
            if statgroup_id in player_lookup:
                matched_count += 1
                player_info = player_lookup[statgroup_id]

                actual_losses = max(rank_info['losses'] - rank_info['drops'], 0)
                total_games = rank_info['wins'] + actual_losses
                winrate = (rank_info['wins'] / total_games * 100) if total_games > 0 else 0

                faction_players.append({
                    'steamid': player_info['steamid'],
                    'alias': player_info['alias'],
                    'rank': rank_info['rank'],
                    'rating': rank_info['rating'],
                    'wins': rank_info['wins'],
                    'losses': actual_losses,
                    'total_games': total_games,
                    'winrate': winrate,
                    'streak': rank_info['streak']
                })
            else:
                print(f"No player data found for statgroup_id: {statgroup_id}")

        print(f"Matched {matched_count}/{len(rank_lookup)} entries")


        faction_players.sort(key=lambda x: x['rank'] if x['rank'] is not None else float('inf'))

        print(f"Successfully processed {len(faction_players)} players for race_id {race_id}")
        return faction_players

    except Exception as e:
        print(f"Error in get_faction_leaderboard for race_id {race_id}: {e}")
        return []


@app_commands.command(name="race_leaderboard",
                      description="Show leaderboard for a specific faction")
@app_commands.describe(
    race="Choose a faction to see its leaderboard",
    start_rank="Starting rank position (1-200, default: 1)",
    count="Number of players to show (1-50, default: 50)"
)
@app_commands.choices(race=[
    app_commands.Choice(name="Chaos", value=1),
    app_commands.Choice(name="Dark Eldar", value=2),
    app_commands.Choice(name="Eldar", value=3),
    app_commands.Choice(name="Imperial Guard", value=4),
    app_commands.Choice(name="Necrons", value=5),
    app_commands.Choice(name="Orks", value=6),
    app_commands.Choice(name="Sisters of Battle", value=7),
    app_commands.Choice(name="Space Marines", value=8),
    app_commands.Choice(name="Tau Empire", value=9),
])
async def slash_race_leaderboard(interaction: discord.Interaction, race: int, start_rank: int = 1,
                                       count: int = 50):

    if not 1 <= start_rank <= 200:
        await interaction.response.send_message("Start rank must be between 1 and 200.", ephemeral=True)
        return

    if not 1 <= count <= 50:
        await interaction.response.send_message("Count must be between 1 and 50.", ephemeral=True)
        return

    if start_rank + count - 1 > 200:
        await interaction.response.send_message(
            "Rank range exceeds maximum of 200. Please adjust start rank or count.", ephemeral=True)
        return

    await interaction.response.defer()

    try:
        leaderboard = await get_faction_leaderboard(race, start_rank, count)

        if not leaderboard:
            race_name = FACTIONS.get(race, f"Race ID {race}")
            await interaction.followup.send(f"No leaderboard data found for {race_name}.", ephemeral=True)
            return

        race_name = FACTIONS.get(race, f"Race ID {race}")
        end_rank = start_rank + len(leaderboard) - 1

        title = f"{race_name} Leaderboard (Ranks {start_rank}-{end_rank})"
        embed = discord.Embed(title=title, color=0xE74C3C)
        embed.description = f"Live leaderboard data for {race_name}"

        leaderboard_text = []
        for player in leaderboard:
            rank = player['rank'] if player['rank'] is not None else "Unranked"


            if player['rank'] == 1:
                medal = "🥇"
            elif player['rank'] == 2:
                medal = "🥈"
            elif player['rank'] == 3:
                medal = "🥉"
            else:
                medal = f"#{rank}"

            line = (f"{medal} **{player['alias']}**\n"
                    f"ELO: {player['rating']} | WR: {player['winrate']:.1f}% ({player['wins']}W-{player['losses']}L)")

            leaderboard_text.append(line)


        chunks = []
        current_chunk = []
        current_length = 0

        for line in leaderboard_text:
            line_length = len(line) + 1
            if current_length + line_length > 1020:
                if current_chunk:
                    chunks.append("\n".join(current_chunk))
                current_chunk = [line]
                current_length = line_length
            else:
                current_chunk.append(line)
                current_length += line_length

        if current_chunk:
            chunks.append("\n".join(current_chunk))


        for i, chunk in enumerate(chunks):
            if len(chunks) == 1:
                field_name = "Rankings"
            else:
                field_name = f"Rankings (Part {i + 1}/{len(chunks)})"

            embed.add_field(name=field_name, value=chunk, inline=False)

        embed.set_footer(text=f"Live data from Relic API - Showing ranks {start_rank}-{end_rank}")
        await interaction.followup.send(embed=embed)

    except Exception as e:
        await interaction.followup.send(f"Error fetching leaderboard data: {str(e)}", ephemeral=True)
        print(f"Error in slash_race_leaderboard: {e}")

@app_commands.command(name="latestmatch", description="Show the latest 1v1 match for a player")
@app_commands.describe(player="Enter a 17-digit SteamID64 or player alias/name")
async def slash_latest_match(interaction: discord.Interaction, player: str):
    await interaction.response.defer()


    steamid, alias, similar_matches = resolve_player_identifier(player)

    if not steamid:
        if similar_matches:
            suggestion_text = "\n".join(f"• {match}" for match in similar_matches)
            await interaction.followup.send(
                f"❌ Player '{player}' not found. Did you mean one of these?\n```\n{suggestion_text}\n```"
            )
        else:
            await interaction.followup.send(
                f"❌ Player '{player}' not found. Make sure the player has been looked up before or use a valid 17-digit SteamID64."
            )
        return

    match_data = await fetch_match_history(steamid)

    if not match_data:
        await interaction.followup.send(f"⚠️ No match history found for {alias}.")
        return

    profiles = match_data.get('profiles', [])
    matches = filter_1v1_matches(match_data.get('matchHistoryStats', []))

    if not matches:
        await interaction.followup.send(f"⚠️ No 1v1 matches found for {alias}.")
        return

    matches_stored = 0
    for match in matches:
        if store_match_from_history(match, profiles, batch_mode=True):
            matches_stored += 1

    if matches_stored > 0:
        save_match_data_to_file()


    for profile in profiles:
        if profile.get('name') == f"/steam/{steamid}":
            player_aliases[steamid] = profile.get('alias', alias)
            alias = profile.get('alias', alias)
            break

    embed = format_match_embed(matches[0], {p['profile_id']: p['alias'] for p in profiles}, is_latest=True)
    embed.description = f"`{alias}` (SteamID: {steamid})" if alias != 'Unknown Player' else f"SteamID: {steamid}"

    if matches_stored > 0:
        embed.set_footer(text=f"Stored {matches_stored} new matches • {embed.footer.text if embed.footer else ''}")

    await interaction.followup.send(embed=embed)


@app_commands.command(name="matchhistory", description="Show recent 1v1 match history for a player")
@app_commands.describe(
    player="Enter a 17-digit SteamID64 or player alias/name",
    limit="Number of matches to show (1-10, default: 5)"
)
async def slash_match_history(interaction: discord.Interaction, player: str, limit: int = 5):
    if not 1 <= limit <= 10:
        await interaction.response.send_message("⚠️ Limit must be between 1 and 10.", ephemeral=True)
        return

    await interaction.response.defer()
    steamid, alias, similar_matches = resolve_player_identifier(player)
    match_data = None

    if steamid:
        match_data = await fetch_match_history(steamid)
    elif not validate_steamid(player):
        match_data = await fetch_match_history_alias(player)

        if match_data:
            profiles = match_data.get('profiles', [])
            for profile in profiles:
                profile_name = profile.get('name', '')
                if profile_name.startswith('/steam/'):
                    steamid = profile_name.replace('/steam/', '')
                    alias = profile.get('alias', player)
                    player_aliases[steamid] = alias
                    break

        if not match_data and similar_matches:
            suggestion_text = "\n".join(f"• {match}" for match in similar_matches)
            await interaction.followup.send(
                f"⚠️ Player '{player}' not found. Did you mean one of these?\n```\n{suggestion_text}\n```"
            )
            return
    else:
        steamid = player
        match_data = await fetch_match_history(steamid)
    if not match_data:
        await interaction.followup.send(f"⚠️ No match history found for {player}.")
        return

    await extract_elos(steamid)

    profiles = {p['profile_id']: p['alias'] for p in match_data.get('profiles', [])}
    matches = filter_1v1_matches(match_data.get('matchHistoryStats', []))

    if not matches:
        await interaction.followup.send(f"⚠️ No 1v1 matches found for {alias}.")
        return

    matches_to_show = matches[:limit]


    for profile_data in match_data.get('profiles', []):
        if profile_data.get('name') == f"/steam/{steamid}":
            alias = profile_data.get('alias', alias)
            player_aliases[steamid] = alias
            break

    embed = create_embed_base("Recent 1v1 Match History", steamid, alias, 0x3498DB)


    for i, match in enumerate(matches_to_show, 1):
        match_id = match['id']
        map_name = match['mapname']
        end_time = datetime.fromtimestamp(match['completiontime']).strftime('%m/%d %H:%M')

        results_map = {
            result['profile_id']: result['resulttype']
            for result in match['matchhistoryreportresults']
        }

        players_info = []
        for member in match['matchhistorymember']:
            profile_id = member['profile_id']
            player_alias = profiles.get(profile_id, f"ID {profile_id}")
            race_name = RACE_MAP.get(member['race_id'], f"Race ID {member['race_id']}")
            old_rating = member['oldrating']
            new_rating = member['newrating']
            rating_change = new_rating - old_rating
            result_type = results_map.get(profile_id)

            outcome_emoji = "🏆" if result_type == 1 else "💀" if result_type == 0 else "❓"
            rating_emoji = "📈" if rating_change > 0 else "📉" if rating_change < 0 else "➡️"

            player_line = f"{outcome_emoji} **{player_alias}** ({race_name})\n{rating_emoji} {old_rating} → {new_rating} ({rating_change:+d})"
            players_info.append(player_line)

        match_summary = f"**Map:** {map_name} | **Date:** {end_time}\n" + "\n".join(players_info)
        embed.add_field(name=f"⚔️ Match #{match_id}", value=match_summary, inline=False)

    await interaction.followup.send(embed=embed)


@app_commands.command(name="matchhistory", description="Show recent 1v1 match history for a player")
@app_commands.describe(
    player="Enter a 17-digit SteamID64 or player alias/name",
    limit="Number of matches to show (1-10, default: 5)"
)
async def slash_match_history(interaction: discord.Interaction, player: str, limit: int = 5):
    if not 1 <= limit <= 10:
        await interaction.response.send_message("⚠️ Limit must be between 1 and 10.", ephemeral=True)
        return

    await interaction.response.defer()


    steamid, alias, similar_matches = resolve_player_identifier(player)
    match_data = None

    if steamid:

        match_data = await fetch_match_history(steamid)
    elif not validate_steamid(player):

        match_data = await fetch_match_history_alias(player)

        if match_data:

            profiles = match_data.get('profiles', [])
            for profile in profiles:
                profile_alias = profile.get('alias', '')
                profile_name = profile.get('name', '')
                if profile_name.startswith('/steam/') and profile_alias.lower() == player.lower():
                    steamid = profile_name.replace('/steam/', '')
                    alias = profile_alias

                    player_aliases[steamid] = alias
                    break


            if not steamid:
                for profile in profiles:
                    profile_name = profile.get('name', '')
                    if profile_name.startswith('/steam/'):
                        steamid = profile_name.replace('/steam/', '')
                        alias = profile.get('alias', player)

                        player_aliases[steamid] = alias
                        break


        if not match_data and similar_matches:
            suggestion_text = "\n".join(f"• {match}" for match in similar_matches)
            await interaction.followup.send(
                f"⚠️ Player '{player}' not found. Did you mean one of these?\n```\n{suggestion_text}\n```"
            )
            return
    else:
        steamid = player
        match_data = await fetch_match_history(steamid)

    if not match_data:
        await interaction.followup.send(f"⚠️ No match history found for {player}.")
        return


    await extract_elos(steamid)

    profiles = {p['profile_id']: p['alias'] for p in match_data.get('profiles', [])}
    matches = filter_1v1_matches(match_data.get('matchHistoryStats', []))

    if not matches:
        await interaction.followup.send(f"⚠️ No 1v1 matches found for {alias}.")
        return

    matches_to_show = matches[:limit]


    for profile_data in match_data.get('profiles', []):
        if profile_data.get('name') == f"/steam/{steamid}":
            alias = profile_data.get('alias', alias)
            player_aliases[steamid] = alias
            break

    embed = create_embed_base("Recent 1v1 Match History", steamid, alias, 0x3498DB)


    for i, match in enumerate(matches_to_show, 1):
        match_id = match['id']
        map_name = match['mapname']
        end_time = datetime.fromtimestamp(match['completiontime']).strftime('%m/%d %H:%M')

        results_map = {
            result['profile_id']: result['resulttype']
            for result in match['matchhistoryreportresults']
        }

        players_info = []
        for member in match['matchhistorymember']:
            profile_id = member['profile_id']
            player_alias = profiles.get(profile_id, f"ID {profile_id}")
            race_name = RACE_MAP.get(member['race_id'], f"Race ID {member['race_id']}")
            old_rating = member['oldrating']
            new_rating = member['newrating']
            rating_change = new_rating - old_rating
            result_type = results_map.get(profile_id)

            outcome_emoji = "🏆" if result_type == 1 else "💀" if result_type == 0 else "❓"
            rating_emoji = "📈" if rating_change > 0 else "📉" if rating_change < 0 else "➡️"

            player_line = f"{outcome_emoji} **{player_alias}** ({race_name})\n{rating_emoji} {old_rating} → {new_rating} ({rating_change:+d})"
            players_info.append(player_line)

        match_summary = f"**Map:** {map_name} | **Date:** {end_time}\n" + "\n".join(players_info)
        embed.add_field(name=f"⚔️ Match #{match_id}", value=match_summary, inline=False)

    await interaction.followup.send(embed=embed)


@app_commands.command(name="debug_race_mapping", description="[ADMIN] Show race mapping and stored data")
async def debug_race_mapping(interaction: discord.Interaction):
    await interaction.response.defer()

    race_counts = defaultdict(int)
    for match in stored_matches.values():
        race_counts[match.player1_race] += 1
        race_counts[match.player2_race] += 1
        race_counts[match.winner_race] += 1

    embed = discord.Embed(title="Race Names in Database", color=0xFF0000)

    stored_races = []
    for race_name, count in sorted(race_counts.items(), key=lambda x: x[1], reverse=True):
        stored_races.append(f"{race_name}: {count} appearances")

    embed.add_field(
        name="All Race Names Found",
        value="\n".join(stored_races),
        inline=False
    )

    await interaction.followup.send(embed=embed)

@app_commands.command(name="scanmatches", description="[ADMIN] Scan leaderboards to collect match data")
@app_commands.describe(passcode="Admin passcode required")
async def slash_scan_matches(interaction: discord.Interaction, passcode: str):
    if passcode != ADMIN_PASSCODE:
        await interaction.response.send_message("❌ Invalid passcode.", ephemeral=True)
        return

    await interaction.response.defer()


    embed = discord.Embed(title="🔍 Match Data Scan Starting", color=0xFFA500)
    embed.description = "Scanning leaderboards to discover players and collect match histories..."
    embed.add_field(name="Status", value="Initializing...", inline=False)

    message = await interaction.followup.send(embed=embed)


    async def update_progress(status_text):
        try:
            embed.set_field_at(0, name="Status", value=status_text, inline=False)
            await message.edit(embed=embed)
        except:
            pass

    start_time = datetime.now()

    try:

        results = await bulk_scan_for_matches(update_progress)

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()


        results_embed = discord.Embed(title="✅ Match Scan Complete", color=0x00FF00)


        results_embed.add_field(
            name="📊 Summary",
            value=f"**Players Processed:** {results['total_players_processed']}\n"
                  f"**New Players Found:** {results['new_players_found']}\n"
                  f"**New Matches Added:** {results['total_matches_added']}\n"
                  f"**Errors:** {results['errors']}\n"
                  f"**Duration:** {duration:.1f} seconds",
            inline=False
        )


        if results['leaderboard_results']:
            leaderboard_text = []
            for faction, count in results['leaderboard_results'].items():
                leaderboard_text.append(f"{faction}: {count} players")

            results_embed.add_field(
                name="🏆 Players Processed per Faction",
                value="\n".join(leaderboard_text),
                inline=False
            )


        total_stored_matches = get_stored_match_count()
        total_known_players = len(player_aliases)

        results_embed.add_field(
            name="💾 Database Status",
            value=f"**Total Stored Matches:** {total_stored_matches}\n"
                  f"**Known Players:** {total_known_players}",
            inline=True
        )


        players_per_second = results['total_players_processed'] / duration if duration > 0 else 0
        matches_per_second = results['total_matches_added'] / duration if duration > 0 else 0

        results_embed.add_field(
            name="⚡ Performance",
            value=f"**Players/Second:** {players_per_second:.2f}\n"
                  f"**Matches/Second:** {matches_per_second:.2f}",
            inline=True
        )

        results_embed.set_footer(text=f"Scan completed at {end_time.strftime('%Y-%m-%d %H:%M:%S')}")

        await message.edit(embed=results_embed)

    except Exception as e:
        error_embed = discord.Embed(title="❌ Match Scan Failed", color=0xFF0000)
        error_embed.description = f"An error occurred during the scan: {str(e)}"
        await message.edit(embed=error_embed)


@app_commands.command(name="matchstats", description="Show statistics about stored match data")
async def slash_match_stats(interaction: discord.Interaction):
    await interaction.response.defer()

    total_matches = get_stored_match_count()
    total_known_players = len(player_aliases)

    if total_matches == 0:
        await interaction.followup.send(
            "⚠️ No match data found. Use `/scanmatches` to collect match data from leaderboards.",
            ephemeral=True)
        return


    race_wins = defaultdict(int)
    race_total_games = defaultdict(int)
    map_counts = defaultdict(int)

    for match in stored_matches.values():

        race_wins[match.winner_race] += 1


        race_total_games[match.player1_race] += 1
        race_total_games[match.player2_race] += 1


        map_counts[match.map_name] += 1

    embed = discord.Embed(title="📊 Match Database Statistics", color=0x3498DB)
    embed.description = f"Statistics from stored match database ({total_matches} matches)"

    embed.add_field(name="🎮 Total Matches", value=str(total_matches), inline=True)
    embed.add_field(name="👥 Known Players", value=str(total_known_players), inline=True)


    top_maps = sorted(map_counts.items(), key=lambda x: x[1], reverse=True)[:5]
    if top_maps:
        map_text = []
        for i, (map_name, count) in enumerate(top_maps, 1):
            map_text.append(f"{i}. {map_name}: {count} matches")

        embed.add_field(
            name="🗺️ Most Played Maps",
            value="\n".join(map_text),
            inline=False
        )


    if race_wins and race_total_games:
        race_winrates = []
        for race in race_wins.keys():
            wins = race_wins[race]
            total_games = race_total_games[race]
            if total_games > 0:
                winrate = (wins / total_games) * 100
                race_winrates.append((race, winrate, wins, total_games))


        sorted_winrates = sorted(race_winrates, key=lambda x: x[1], reverse=True)
        winrate_text = []
        for race, winrate, wins, total_games in sorted_winrates:
            winrate_text.append(f"{race}: {winrate:.1f}% ({wins}W-{total_games - wins}L)")

        embed.add_field(
            name="🏆 Race Win Rates",
            value="\n".join(winrate_text),
            inline=False
        )

    embed.set_footer(text=f"Use /scanmatches to collect more match data")
    await interaction.followup.send(embed=embed)


async def topelo(limit: int = 50, min_games: int = 10) -> list:

    all_players = {}
    faction_leaders = {}


    FACTION_SYMBOLS = {
        1: '😈',
        2: '🗡️',
        3: '✨',
        4: '🛡️',
        5: '🤖',
        6: '💪',
        7: '🔥',
        8: '⭐',
        9: '🎯'
    }

    known_leaderboards = [
        {'id': 1, 'name': 'Chaos 1v1'},
        {'id': 2, 'name': 'Dark Eldar 1v1'},
        {'id': 3, 'name': 'Eldar 1v1'},
        {'id': 4, 'name': 'Guard 1v1'},
        {'id': 5, 'name': 'Necron 1v1'},
        {'id': 6, 'name': 'Orc 1v1'},
        {'id': 7, 'name': 'Sisters 1v1'},
        {'id': 8, 'name': 'Space Marine 1v1'},
        {'id': 9, 'name': 'Tau 1v1'}
    ]

    for leaderboard in known_leaderboards:
        lb_id = leaderboard['id']
        faction_name = FACTIONS.get(lb_id, f"Faction {lb_id}")

        start = 1
        count = 100

        try:
            lb_data = await fetch_leaderboard_data(lb_id, start, count)

            if not lb_data:
                print(f"No leaderboard data returned for race_id {lb_id}")
                continue

            leaderboard_stats = lb_data.get("leaderboardStats", [])
            stat_groups = lb_data.get("statGroups", [])
            if not leaderboard_stats:
                print(f"No leaderboardStats found for race_id {lb_id}")
                continue
            if not stat_groups:
                print(f"No statGroups found for race_id {lb_id}")
                continue


            rank_lookup = {}
            for stat in leaderboard_stats:
                if stat.get("leaderboard_id") == lb_id:
                    statgroup_id = stat.get("statgroup_id")
                    if statgroup_id:
                        rank_lookup[statgroup_id] = {
                            'rank': stat.get('rank'),
                            'rating': stat.get('rating', 0),
                            'wins': stat.get('wins', 0),
                            'losses': stat.get('losses', 0),
                            'drops': stat.get('drops', 0),
                            'streak': stat.get('streak', 0)
                        }


            player_lookup = {}
            for stat_group in stat_groups:
                group_id = stat_group.get("id")
                members = stat_group.get("members", [])

                if group_id and members:
                    member = members[0]
                    member_name = member.get("name", "")

                    if member_name.startswith("/steam/"):
                        steam_id = member_name.replace("/steam/", "")
                        if validate_steamid(steam_id):
                            alias = member.get("alias", "Unknown Player")
                            player_lookup[group_id] = {
                                'steamid': steam_id,
                                'alias': alias
                            }

            print(f"Found {len(rank_lookup)} rank entries and {len(player_lookup)} player entries for {faction_name}")


            for statgroup_id, rank_info in rank_lookup.items():
                if statgroup_id in player_lookup:
                    player_info = player_lookup[statgroup_id]


                    actual_losses = max(rank_info['losses'] - rank_info['drops'], 0)
                    total_games = rank_info['wins'] + actual_losses


                    if rank_info['rank'] is None or rank_info['rank'] <= 0 or rank_info['rank'] > 100:
                        continue


                    if total_games < min_games:
                        continue

                    winrate = (rank_info['wins'] / total_games * 100) if total_games > 0 else 0
                    rating = rank_info['rating']


                    if rank_info['rank'] == 1:
                        steam_id = player_info['steamid']
                        if steam_id not in faction_leaders:
                            faction_leaders[steam_id] = []
                        faction_leaders[steam_id].append(lb_id)


                    player_data = {
                        'steam_id': player_info['steamid'],
                        'alias': player_info['alias'],
                        'rating': rating,
                        'faction': faction_name,
                        'faction_id': lb_id,
                        'wins': rank_info['wins'],
                        'losses': actual_losses,
                        'total_games': total_games,
                        'winrate': winrate,
                        'rank': rank_info['rank'] if rank_info['rank'] > 0 else None
                    }


                    steam_id = player_info['steamid']
                    if steam_id not in all_players or rating > all_players[steam_id]['rating']:
                        all_players[steam_id] = player_data

        except Exception as e:
            print(f"Error fetching leaderboard {faction_name}: {e}")
            continue


    top_players = list(all_players.values())


    for player in top_players:
        steam_id = player['steam_id']
        if steam_id in faction_leaders:

            led_factions = faction_leaders[steam_id]
            symbols = [FACTION_SYMBOLS.get(faction_id, '👑') for faction_id in led_factions]
            player['leader_symbols'] = ''.join(symbols)
            player['is_faction_leader'] = True
        else:
            player['leader_symbols'] = ''
            player['is_faction_leader'] = False

    top_players.sort(key=lambda x: x['rating'], reverse=True)

    print(f"Processed {len(all_players)} unique players, returning top {min(limit, len(top_players))}")


    return top_players[:limit]


async def format_topelo_embed(players: list, title: str = "🏆 Top ELO Players (All Factions)") -> discord.Embed:

    if not players:
        embed = discord.Embed(title=title, color=0xFF0000)
        embed.description = "No players found matching criteria."
        return embed

    embed = discord.Embed(title=title, color=0xFFD700)
    embed.description = (f"Top {len(players)} players ranked by highest ELO across all factions\n"
                         f"**Faction Leaders:** 😈Chaos 🗡️Dark Eldar ✨Eldar 🛡️Guard 🤖Necron 💪Orc 🔥Sisters ⭐Space Marine 🎯Tau")


    player_text = []
    for i, player in enumerate(players, 1):

        if i == 1:
            medal = "🥇"
        elif i == 2:
            medal = "🥈"
        elif i == 3:
            medal = "🥉"
        else:
            medal = f"#{i}"


        symbols = player.get('leader_symbols', '')
        symbol_text = f"{symbols} " if symbols else ""
        line = (f"{medal} {symbol_text}**{player['alias']}** ({player['faction']})\n"
                f"ELO: {player['rating']} | WR: {player['winrate']:.1f}% "
                f"({player['wins']}W-{player['losses']}L)")

        player_text.append(line)


    chunks = []
    current_chunk = []
    current_length = 0

    for line in player_text:
        line_length = len(line) + 1

        if current_length + line_length > 1020:
            if current_chunk:
                chunks.append("\n\n".join(current_chunk))
            current_chunk = [line]
            current_length = line_length
        else:
            current_chunk.append(line)
            current_length += line_length

    if current_chunk:
        chunks.append("\n\n".join(current_chunk))


    for i, chunk in enumerate(chunks):
        field_name = "Rankings" if len(chunks) == 1 else f"Rankings (Part {i + 1}/{len(chunks)})"
        embed.add_field(name=field_name, value=chunk, inline=False)

    return embed



@app_commands.command(name="topelo", description="Show top ranked players by ELO across all factions (Top 200 only)")
@app_commands.describe(
    limit="Number of top players to show (default: 10, max: 20)",
    min_games="Minimum games required to be ranked (default: 10)"
)
async def slash_topelo(interaction: discord.Interaction, limit: int = 10, min_games: int = 10):

    if limit < 1 or limit > 20:
        await interaction.response.send_message("Limit must be between 1 and 20.", ephemeral=True)
        return

    if min_games < 0:
        await interaction.response.send_message("Minimum games must be 0 or greater.", ephemeral=True)
        return

    await interaction.response.defer()

    try:

        top_players = await topelo(limit=limit, min_games=min_games)

        if not top_players:
            await interaction.followup.send("No top 200 ranked players found meeting the criteria.")
            return


        title = f"🏆 Top {len(top_players)} ELO Players (Top 200 Ranked Only)"
        embed = await format_topelo_embed(top_players, title)

        embed.description = (f"Top {len(top_players)} ranked players by highest ELO (ranks 1-200 only)\n"
                           f"**Faction Leaders:** 😈Chaos 🗡️Dark Eldar ✨Eldar 🛡️Guard 🤖Necron 💪Orc 🔥Sisters ⭐Space Marine 🎯Tau")


        footer_text = (f"Minimum games: {min_games} • Top 200 ranked players only • Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        embed.set_footer(text=footer_text)

        await interaction.followup.send(embed=embed)

    except Exception as e:
        print(f"Error in topelo command: {e}")
        await interaction.followup.send(f"An error occurred while fetching top ELO data: {str(e)}", ephemeral=True)




@app_commands.command(name="help", description="Show all available commands and what they do")
async def slash_help(interaction: discord.Interaction):
    embed = discord.Embed(
        title="Dawn of War Bot Commands",
        description="Here are all the available commands for Dawn of War - Definitive Edition statistics:",
        color=0x3498DB
    )


    embed.add_field(
        name="📊 Player Statistics",
        value=(
            "`/factions <player>` - Show live faction stats (winrate, rank, ELO) for a player\n"
            "`/1v1winrate <player>` - Show overall 1v1 winrate for a player\n"
            "`/storedelo <steamid>` - Show stored ELO data for a Steam ID"
        ),
        inline=False
    )


    embed.add_field(
        name="⚔️ Match History",
        value=(
            "`/latestmatch <player>` - Show the latest 1v1 match for a player\n"
            "`/matchhistory <player> [limit]` - Show recent 1v1 match history (1-10 matches)\n"
            "`/playermatchhistory <player> [limit]` - Show stored match history for a player"
        ),
        inline=False
    )


    embed.add_field(
        name="🏆 Leaderboards & Statistics",
        value=(
            "`/raceleaderboard <race> [limit]` - CURRENTLY DOWN check out https://dowstats.com/relic-ladder for an active leaderboard\n"
            "`/topelo [limit] [start]` - (CURRENTLY DOWN)\n"
            "`/scanstats` - Show statistics about the stored player database\n"
            "`/matchstats` - Show comprehensive database statistics and race performance"
        ),
        inline=False
    )


    embed.add_field(
        name="🔍 Match Analysis",
        value=(
            "`/racematchups <race>` - Show win/loss statistics for a race vs all others\n"
            "`/allmatchups` - Show win rates for all race combinations (5+ games)\n"
            "`/mapstats <map>` - Choose a map to see race win rates on that specific map"
        ),
        inline=False
    )


    embed.add_field(
        name="💡 Usage Tips",
        value=(
            "• Use either **Steam ID** (17 digits) or **player alias** for most commands\n"
            "• Steam ID example: `76561198356992755`\n"
            "• Player alias example: `deemo1225`\n"
            "• All faction and ELO data is fetched live from the API\n"
            "• Match history is stored locally for faster access"
        ),
        inline=False
    )


    embed.add_field(
        name="🎮 Available Factions",
        value="Chaos • Dark Eldar • Eldar • Imperial Guard • Necrons • Orks • Sisters of Battle • Space Marines • Tau Empire",
        inline=False
    )


    embed.add_field(
        name="📡 Data Sources",
        value=(
            "• **Live ELO Data:** Fetched directly from Relic's Dawn of War API\n"
            "• **Match History:** Stored locally with automatic updates\n"
            "• **Player Lookup:** Uses stored aliases for faster player resolution\n"
            "• Data includes 1v1 ranked matches only"
        ),
        inline=False
    )

    embed.set_footer(text="Dawn of War - Definitive Edition | Live ELO data from Relic API")

    await interaction.response.send_message(embed=embed)



@bot.event
async def on_ready():


    load_match_data_from_file()
    load_aliases_from_file()

    try:
        synced = await bot.tree.sync()
        print(f"Bot online as {bot.user}! Synced {len(synced)} commands.")
        print(f"Loaded {get_stored_match_count()} stored matches.")
        print(f"Loaded {len(player_aliases)} stored player aliases.")
    except Exception as e:
        print(f"Failed to sync commands: {e}")

async def cleanup():
    await connection_manager.close()
    save_match_data_to_file()


@bot.event
async def on_error(event, *args, **kwargs):
    print(f"Error in {event}: {args}")


@bot.event
async def on_disconnect():
    await cleanup()


if __name__ == "__main__":
    bot.tree.add_command(slash_factions)
    bot.tree.add_command(slash_1v1winrate)
    bot.tree.add_command(slash_latest_match)
    bot.tree.add_command(slash_match_history)
    bot.tree.add_command(slash_scan_matches)
    bot.tree.add_command(slash_match_stats)
    bot.tree.add_command(slash_race_matchups)
    bot.tree.add_command(slash_map_stats)
    bot.tree.add_command(slash_all_matchups)
    bot.tree.add_command(slash_help)
    bot.tree.add_command(slash_race_leaderboard)
    bot.tree.add_command(slash_topelo)
    try:
        bot.run(TOKEN)
    finally:
        asyncio.run(cleanup())