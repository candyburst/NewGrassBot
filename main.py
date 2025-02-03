import asyncio
import json
import random
import ssl
import time
import uuid
from typing import Dict, Optional, Set

import aiofiles
import aiohttp
from colorama import Fore, Style, init
from loguru import logger
from websockets_proxy import Proxy, proxy_connect

# Initialize colorama
init(autoreset=True)

# File to save the user config
CONFIG_FILE = "config.json"
# Global constants
PING_INTERVAL = 30  # Default ping interval in seconds
# WebSocket Configuration
URI_LIST = [
    "wss://proxy.wynd.network:4444/",
    "wss://proxy2.wynd.network:4650/"
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
}

ERROR_PATTERNS = [
    "Host unreachable",
    "[SSL: WRONG_VERSION_NUMBER]",
    "invalid length of packed IP address string",
    "Empty connect reply",
    "Device creation limit exceeded",
    "sent 1011 (internal error) keepalive ping timeout"
]

class WebSocketClient:
    def __init__(self, socks5_proxy: str, user_id: str, uri: str):
        self.proxy = socks5_proxy
        self.user_id = user_id
        self.uri = uri
        self.device_id = str(uuid.uuid3(uuid.NAMESPACE_DNS, socks5_proxy))

    async def connect(self) -> Optional[bool]:
        logger.info(f"🖥️ Device ID: {self.device_id}")
        while True:
            try:
                await asyncio.sleep(0.1)
                ssl_context = ssl.create_default_context()
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE

                async with proxy_connect(
                    self.uri,
                    proxy=Proxy.from_url(self.proxy),
                    ssl=ssl_context,
                    extra_headers=HEADERS
                ) as websocket:
                    ping_task = asyncio.create_task(self._send_ping(websocket))
                    try:
                        await self._handle_messages(websocket)
                    finally:
                        ping_task.cancel()
                        try:
                            await ping_task
                        except asyncio.CancelledError:
                            pass

            except Exception as e:
                logger.error(f"🚫 Error with proxy {self.proxy}: {str(e)}")
                if any(pattern in str(e) for pattern in ERROR_PATTERNS):
                    logger.info(f"❌ Removing error proxy from list: {self.proxy}")
                    await self._remove_proxy_from_list()
                    return None
                await asyncio.sleep(5)

    async def _send_ping(self, websocket) -> None:
        while True:
            try:
                message = {
                    "id": str(uuid.uuid4()),
                    "version": "1.0.0",
                    "action": "PING",
                    "data": {"user_name": "FakerPK"}
                }
                await websocket.send(json.dumps(message))
                await asyncio.sleep(PING_INTERVAL)
            except Exception as e:
                logger.error(f"🚫 Error sending ping: {str(e)}")
                break

    async def _handle_messages(self, websocket) -> None:
        handlers: Dict = {
            "AUTH": self._handle_auth,
            "PONG": self._handle_pong
        }

        while True:
            response = await websocket.recv()
            message = json.loads(response)
            logger.info(f"📥 Received message: {message}")

            handler = handlers.get(message.get("action"))
            if handler:
                await handler(websocket, message)

    async def _handle_auth(self, websocket, message: Dict) -> None:
        auth_response = {
            "id": message["id"],
            "origin_action": "AUTH",
            "result": {
                "browser_id": self.device_id,
                "user_id": self.user_id,
                "user_agent": HEADERS["User-Agent"],
                "timestamp": int(time.time()),
                "device_type": "desktop",
                "version": "4.29.0",
            }
        }
        await websocket.send(json.dumps(auth_response))

    async def _handle_pong(self, websocket, message: Dict) -> None:
        pong_response = {
            "id": message["id"],
            "origin_action": "PONG"
        }
        await websocket.send(json.dumps(pong_response))

    async def _remove_proxy_from_list(self) -> None:
        try:
            async with aiofiles.open("proxy.txt", "r") as file:
                lines = await file.readlines()

            async with aiofiles.open("proxy.txt", "w") as file:
                await file.writelines(line for line in lines if line.strip() != self.proxy)
        except Exception as e:
            logger.error(f"🚫 Error removing proxy from file: {str(e)}")

class ProxyManager:
    def __init__(self, user_id: str):
        self.user_id = user_id
        self.active_proxies: Set[str] = set()
        self.all_proxies: Set[str] = set()

    async def load_proxies(self) -> None:
        try:
            url = "https://raw.githubusercontent.com/monosans/proxy-list/refs/heads/main/proxies/all.txt"
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        content = await response.text()
                        self.all_proxies = set(line.strip() for line in content.splitlines() if line.strip())
                    else:
                        logger.error(f"❌ Failed to fetch proxies, status code: {response.status}")
        except Exception as e:
            logger.error(f"❌ Error loading proxies: {str(e)}")

    async def start(self, max_proxies: int) -> None:
        await self.load_proxies()
        if not self.all_proxies:
            logger.error("❌ No proxies found in the source list")
            return

        self.active_proxies = set(random.sample(list(self.all_proxies), min(len(self.all_proxies), max_proxies)))
        tasks = {asyncio.create_task(self._run_client(proxy)): proxy for proxy in self.active_proxies}

        while True:
            done, pending = await asyncio.wait(tasks.keys(), return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                proxy = tasks.pop(task)
                if task.result() is None:  # Proxy failed
                    self.active_proxies.remove(proxy)
                    await self.load_proxies()
                    available_proxies = self.all_proxies - self.active_proxies
                    if available_proxies:
                        new_proxy = random.choice(list(available_proxies))
                        self.active_proxies.add(new_proxy)
                        new_task = asyncio.create_task(self._run_client(new_proxy))
                        tasks[new_task] = new_proxy

    async def _run_client(self, proxy: str) -> Optional[bool]:
        uri = random.choice(URI_LIST)
        client = WebSocketClient(proxy, self.user_id, uri)
        return await client.connect()

def setup_logger() -> None:
    logger.remove()
    logger.add(
        "bot.log",
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | <cyan>{message}</cyan>",
        level="INFO",
        rotation="1 day"
    )
    logger.add(
        lambda msg: print(msg),
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | <cyan>{message}</cyan>",
        level="INFO",
        colorize=True
    )

def print_banner() -> None:
    print(f"""{Fore.YELLOW}
 ______    _             _____  _  __
|  ____|  | |           |  __ \| |/ /
| |__ __ _| | _____ _ __| |__) | ' / 
|  __/ _` | |/ / _ \ '__|  ___/|  <  
| | | (_| |   <  __/ |  | |    | . \ 
|_|  \__,_|_|\_\___|_|  |_|    |_|\_\ {Style.RESET_ALL}
    """)
    print(f"{Fore.MAGENTA}Bless Network Bot! AUTOMATE AND DOMINATE {Style.RESET_ALL}")
    print(f"{Fore.RED}============================================={Style.RESET_ALL}")

async def save_user_config(user_id: str, max_proxies: int) -> None:
    config_data = {
        "user_id": user_id,
        "max_proxies": max_proxies
    }
    with open(CONFIG_FILE, "w") as config_file:
        json.dump(config_data, config_file)

async def load_user_config() -> Optional[Dict]:
    try:
        with open(CONFIG_FILE, "r") as config_file:
            config_data = json.load(config_file)

        # Get values from config or set defaults
        user_id = config_data.get("user_id", "")
        max_proxies = config_data.get("max_proxies", 100)  # Default to 100 if not found

        return {"user_id": user_id, "max_proxies": max_proxies}
    except Exception as e:
        logger.error(f"❌ Error loading configuration: {str(e)}")
        return None


async def user_input() -> None:
    print(f"{Fore.GREEN}💻 Welcome to Get Grass Bot Setup! Please provide your details!{Style.RESET_ALL}")
    user_id = input(f"{Fore.YELLOW}🔑 Enter your USER_ID: {Style.RESET_ALL}")
    max_proxies = input(f"{Fore.MAGENTA}📡 Enter the maximum number of proxies to use: {Style.RESET_ALL}")
    max_proxies = int(max_proxies)  # Convert input to an integer

    config_data = {
        "user_id": user_id,
        "max_proxies": max_proxies
    }

    with open(CONFIG_FILE, "w") as config_file:
        json.dump(config_data, config_file, indent=4)

    logger.info(f"✅ Configuration saved! USER_ID: {user_id}, MAX_PROXIES: {max_proxies}")


async def main() -> None:
    setup_logger()
    print_banner()

    # Load user configuration
    config = await load_user_config()

    if not config:
        await user_input()  # If no config exists, prompt the user for input
        return  # Exit the script after saving the config

    # Load the saved config
    user_id = config["user_id"]
    max_proxies = config["max_proxies"]  # This loads the max proxies from the config

    # Log user info with emojis
    logger.info(f"🚀 Starting with USER_ID: {user_id}")
    logger.info(f"📡 Using a maximum of {max_proxies} proxies")
    logger.info(f"⏱️ Ping interval: {PING_INTERVAL} seconds")

    manager = ProxyManager(user_id)
    await manager.start(max_proxies)  # Pass max_proxies to ProxyManager


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Shutting down gracefully...")
    except Exception as e:
        logger.error(f"❌ Fatal error: {str(e)}")
