import os
import requests
import logging
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, '.env'))

STEAM_API_KEY = os.getenv('STEAM_API_KEY', '')
GAME_DIR = os.getenv('GAME_DIR', 'tf')  # Default to Team Fortress 2

if not STEAM_API_KEY:
    logging.warning("No STEAM_API_KEY found in .env file. Server scanning will be disabled.")
    logging.info("Get a free Steam API key at: https://steamcommunity.com/dev/apikey")

logging.info(f"Game configured: {GAME_DIR}")

def get_server_list(game=None, timeout=60):
    """Fetches the server list from the Steam Web API."""
    from utils import is_valid_public_ip
    
    # Use env variable if no game specified
    if game is None:
        game = GAME_DIR
    
    all_servers = set()  # Use set to auto-deduplicate
    
    if not STEAM_API_KEY:
        logging.error("STEAM_API_KEY not found in .env file. Server scanning disabled.")
        return []
    
    try:
        # Map game shortname to app ID
        game_appids = {
            "tf": 440,      # Team Fortress 2
            "csgo": 730,    # CS:GO
            "cs2": 730,     # CS2
            "cstrike": 10,  # Counter-Strike 1.6
            "dod": 30,      # Day of Defeat
            "hl2dm": 320,   # Half-Life 2: Deathmatch
            "l4d": 500,     # Left 4 Dead
            "l4d2": 550,    # Left 4 Dead 2
        }
        
        appid = game_appids.get(game, 440)  # Default to TF2
        
        # Steam Web API endpoint for game servers
        url = "https://api.steampowered.com/IGameServersService/GetServerList/v1/"
        
        # Query by different regions to bypass the ~10k limit per request
        regions = [
            ("us", "\\region\\0"),   # US East
            ("usw", "\\region\\1"),  # US West  
            ("sa", "\\region\\2"),   # South America
            ("eu", "\\region\\3"),   # Europe
            ("asia", "\\region\\4"), # Asia
            ("au", "\\region\\5"),   # Australia
            ("me", "\\region\\6"),   # Middle East
            ("af", "\\region\\7"),   # Africa
            ("world", ""),           # Unfiltered (catches any missed)
        ]
        
        for region_name, region_filter in regions:
            try:
                # Use both appid AND gamedir filters for reliable game filtering
                filter_str = f"\\appid\\{appid}\\gamedir\\{game}{region_filter}"
                params = {
                    "key": STEAM_API_KEY,
                    "filter": filter_str,
                    "limit": 20000  # Request max allowed
                }
                
                response = requests.get(url, params=params, timeout=timeout)
                response.raise_for_status()
                
                data = response.json()
                servers = data.get("response", {}).get("servers", [])
                
                region_count = 0
                for server in servers:
                    # Validate that server is for the correct game
                    server_appid = server.get("appid", 0)
                    server_gamedir = server.get("gamedir", "")
                    
                    # Only add if it matches our game (double-check the API filter worked)
                    if server_appid != appid and server_gamedir.lower() != game.lower():
                        continue
                    
                    addr = server.get("addr", "")
                    if ":" in addr:
                        ip, port = addr.rsplit(":", 1)
                        # Skip invalid/link-local IPs at the source
                        if not is_valid_public_ip(ip):
                            continue
                        try:
                            all_servers.add((ip, int(port)))
                            region_count += 1
                        except ValueError:
                            continue
                
                logging.debug(f"Region {region_name}: found {region_count} {game} servers")
                
            except requests.exceptions.Timeout:
                logging.warning(f"Steam API request timed out for region {region_name}")
            except requests.exceptions.RequestException as e:
                logging.warning(f"Failed to fetch servers for region {region_name}: {e}")
        
        logging.info(f"Fetched {len(all_servers)} unique servers from Steam Web API (across all regions).")
        
    except Exception as e:
        logging.error(f"Failed to fetch server list from Steam Web API: {e}")
    
    return list(all_servers)
