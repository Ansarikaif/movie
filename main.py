from dotenv import load_dotenv # Moved to the top
load_dotenv() # Moved to the top

import os
import aiohttp
import asyncio
import time
import sys
import io
import re
import logging
from bs4 import BeautifulSoup
from urllib.parse import urljoin, unquote, quote
import database as db
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
    InputMediaPhoto,
    BotCommand,
    BotCommandScopeDefault,
    BotCommandScopeChat
)
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    filters,
    CallbackContext,
    CallbackQueryHandler,
    ContextTypes
)
from telegram.error import BadRequest, Forbidden

# Windows-specific fixes
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO, # Changed to INFO for production, DEBUG is too verbose
    handlers=[
        logging.FileHandler("movie_bot.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- Configuration ---
OMDB_API_KEYS_STR = os.getenv("OMDB_API_KEYS") # Comma-separated keys
OMDB_API_KEYS = [key.strip() for key in OMDB_API_KEYS_STR.split(',')] if OMDB_API_KEYS_STR else []
SHRINKME_API_KEY = os.getenv("SHRINKME_API_KEY")
BOT_TOKEN = os.getenv("BOT_TOKEN")
LOG_CHANNEL_ID = os.getenv("LOG_CHANNEL_ID")

# Admin configuration
ADMIN_IDS_STR = os.getenv("ADMIN_IDS")
ADMIN_IDS = []
if ADMIN_IDS_STR:
    try:
        ADMIN_IDS = [int(uid.strip()) for uid in ADMIN_IDS_STR.split(',') if uid.strip().isdigit()]
    except ValueError:
        logger.error(f"Invalid ADMIN_IDS format: {ADMIN_IDS_STR}")
        ADMIN_IDS = []

# Category configuration
CATEGORY_KEYWORDS = [
    "Bollywood",
    "Chinese",
    "Hollywood",
    "Hindi dubbed",
    "Tamil",
    "animation",
    "indianbangla",
    "tvseries",
    "Korean",
    "S",
]

# FIX: Added missing commas to prevent URL concatenation
BASE_URLS = [
    "http://103.145.232.246/Data/movies/Bollywood/2000/",
    "http://103.145.232.246/Data/movies/Bollywood/2007/",
    "http://103.145.232.246/Data/movies/Bollywood/2008/",
    "http://103.145.232.246/Data/movies/Bollywood/2011/",
    "http://103.145.232.246/Data/movies/Bollywood/2012/",
    "http://103.145.232.246/Data/movies/Bollywood/2013/",
    "http://103.145.232.246/Data/movies/Bollywood/2014/",
    "http://103.145.232.246/Data/movies/Bollywood/2018/",
    "http://103.145.232.246/Data/movies/Bollywood/2019/",
    "http://103.145.232.246/Data/movies/Bollywood/2020/",
    "http://103.145.232.246/Data/movies/Bollywood/2021/",
    "http://103.145.232.246/Data/movies/Bollywood/2022/",
    "http://103.145.232.246/Data/movies/Bollywood/2023/",
    "http://103.145.232.246/Data/movies/Bollywood/2024/",
    "http://103.145.232.246/Data/movies/Bollywood/2025/",
    "http://103.145.232.246/Data/movies/Bollywood/Bollywood%20collection/",
    "http://103.145.232.246/Data/movies/Bollywood/random/",
    "http://103.145.232.246/Data/movies/Chinese/2012/",
    "http://103.145.232.246/Data/movies/Chinese/2020/",
    "http://103.145.232.246/Data/movies/Chinese/2024/",
    "http://103.145.232.246/Data/movies/Hindi%20dubbed/",
    "http://103.145.232.246/Data/movies/Hollywood/2000/",
    "http://103.145.232.246/Data/movies/Hollywood/2008/",
    "http://103.145.232.246/Data/movies/Hollywood/2009/",
    "http://103.145.232.246/Data/movies/Hollywood/2010/",
    "http://103.145.232.246/Data/movies/Hollywood/2011/",
    "http://103.145.232.246/Data/movies/Hollywood/2012/",
    "http://103.145.232.246/Data/movies/Hollywood/2013/",
    "http://103.145.232.246/Data/movies/Hollywood/2014/",
    "http://103.145.232.246/Data/movies/Hollywood/2015/",
    "http://103.145.232.246/Data/movies/Hollywood/2016/",
    "http://103.145.232.246/Data/movies/Hollywood/2017/",
    "http://103.145.232.246/Data/movies/Hollywood/2018/",
    "http://103.145.232.246/Data/movies/Hollywood/2019/",
    "http://103.145.232.246/Data/movies/Hollywood/2020/",
    "http://103.145.232.246/Data/movies/Hollywood/2021/",
    "http://103.145.232.246/Data/movies/Hollywood/2022/",
    "http://103.145.232.246/Data/movies/Hollywood/2023/",
    "http://103.145.232.246/Data/movies/Hollywood/2024/",
    "http://103.145.232.246/Data/movies/Hollywood/2025/",
    "http://103.145.232.246/Data/movies/Hollywood/Hollywood%20collection/",
    "http://103.145.232.246/Data/movies/Hollywood/best/",
    "http://103.145.232.246/Data/movies/Hollywood/best1/",
    "http://103.145.232.246/Data/movies/Hollywood/horror/",
    "http://103.145.232.246/Data/movies/Hollywood/new%202009/",
    "http://103.145.232.246/Data/movies/Hollywood/random/",
    "http://103.145.232.246/Data/movies/Tamil/2020/",
    "http://103.145.232.246/Data/movies/Tamil/2021/",
    "http://103.145.232.246/Data/movies/Tamil/2022/",
    "http://103.145.232.246/Data/movies/Tamil/2023/",
    "http://103.145.232.246/Data/movies/Tamil/2024/",
    "http://103.145.232.246/Data/movies/animation/2009/",
    "http://103.145.232.246/Data/movies/animation/2011-2020/",
    "http://103.145.232.246/Data/movies/animation/2023/",
    "http://103.145.232.246/Data/movies/animation/2024/",
    "http://103.145.232.246/Data/movies/animation/new/",
    "http://103.145.232.246/Data/movies/indianbangla/2000-2021/",
    "http://103.145.232.246/Data/movies/indianbangla/2019/",
    "http://103.145.232.246/Data/movies/indianbangla/2020/",
    "http://103.145.232.246/Data/movies/indianbangla/2022/",
    "http://103.145.232.246/Data/movies/indianbangla/2023/",
    "http://103.145.232.246/Data/movies/indianbangla/2024/",
    "http://103.145.232.246/Data/movies/indianbangla/new/",
    "http://103.145.232.246/Data/movies/indianbangla/random/",
    "http://103.145.232.246/Data/movies/korean/",
    "http://103.145.232.246/Data/movies/s/10-06-2024/",
    "http://103.145.232.246/Data/movies/s/11-7-2024/",
    "http://103.145.232.246/Data/movies/s/17-7-2025/",
    "http://103.145.232.246/Data/movies/s/18-4-2025/",
    "http://103.145.232.246/Data/movies/s/22-6-2024/",
    "http://103.145.232.246/Data/movies/s/28-3-2025/",
    "http://103.145.232.246/Data/movies/s/28-6-2024/",
    "http://103.145.232.246/Data/movies/s/7-7-2024/",
    "http://103.145.232.246/Data/tvseries/Bangla/",
    "http://103.145.232.246/Data/tvseries/English/",
    "http://103.145.232.246/Data/tvseries/Indian/",
    "http://103.145.232.246/Data/tvseries/Squid%20Game/",
    "http://103.145.232.246/Data/tvseries/new/",
]


MAX_RETRIES = 3
REQUEST_TIMEOUT = 20
FILES_PER_PAGE = 10 
METADATA_REQUEST_TIMEOUT = 30 
VIDEO_EXTENSIONS = ('.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm', '.mpeg', '.mpg')

# --- Caching ---
metadata_cache = {}
url_shorten_cache = {}

# --- Tracking ---
search_query_counts = {}
item_selection_counts = {}

# --- Helper Functions ---
def get_category(url: str) -> str:
    """Extracts category from URL based on keywords, handling URL encoding."""
    # First, decode any URL-encoded characters (like %20 for space)
    decoded_url = unquote(url)
    url_lower = decoded_url.lower()
    
    for keyword in CATEGORY_KEYWORDS:
        if keyword.lower() in url_lower:
            return keyword
    return "Uncategorized"

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

async def fetch_url(session: aiohttp.ClientSession, url: str, retries: int = MAX_RETRIES, timeout: int = REQUEST_TIMEOUT):
    for attempt in range(retries):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as response:
                response.raise_for_status()
                return await response.text()
        except Exception as e:
            logger.warning(f"Attempt {attempt+1}/{retries} failed for {url}: {str(e)}")
            if attempt < retries - 1:
                await asyncio.sleep(1 * (2 ** attempt))
    return None

async def get_file_size(session: aiohttp.ClientSession, url: str) -> str:
    """Gets the file size from a URL using a HEAD request."""
    try:
        async with session.head(url, allow_redirects=True, timeout=REQUEST_TIMEOUT) as response:
            response.raise_for_status()
            if 'Content-Length' in response.headers:
                size_bytes = int(response.headers['Content-Length'])
                if size_bytes < 1024:
                    return f"{size_bytes} B"
                elif size_bytes < 1024 * 1024:
                    return f"{size_bytes / 1024:.2f} KB"
                elif size_bytes < 1024 * 1024 * 1024:
                    return f"{size_bytes / (1024 * 1024):.2f} MB"
                else:
                    return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"
            return "Size unknown"
    except Exception as e:
        logger.warning(f"File size error for {url}: {str(e)}")
    return "Size N/A"

def normalize_movie_name(name: str) -> str:
    original_name = name
    name = re.sub(r'\s*\(\d{4}\)\s*|\b\d{3,4}p\b|\b(hdrip|bluray|web-dl|brrip|hdcam|dvdscr|x264|x265|aac|ac3|5\\.1|7\\.1)\b', '', name, flags=re.IGNORECASE)
    name = re.sub(r'[^a-z0-9\s]', '', name.lower())
    normalized = ' '.join(name.split())
    logger.debug(f"Normalized '{original_name}' to '{normalized}'")
    return normalized

async def search_movie(query: str):
    """Searches for movies and web series in the database based on the query."""
    norm_query = normalize_movie_name(query)
    movies = await db.search_movies_by_normalized_name(norm_query)
    webseries = await db.search_webseries_by_normalized_name(norm_query)
    
    # Combine results and remove duplicates while preserving order
    combined = movies + webseries
    seen = set()
    unique_results = [x for x in combined if not (x in seen or seen.add(x))]
    return unique_results

async def scrape_and_update_db():
    """Scrapes all base URLs and updates the database with the findings."""
    logger.info("Starting to scrape and update database...")
    
    # Clear previously scraped movies to ensure the database is fresh.
    # This will not affect manually added movies.
    await db.clear_scraped_movies()
    
    scraped_items = []
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as session:
        tasks = [fetch_and_parse_url(session, base_url, scraped_items) for base_url in BASE_URLS]
        await asyncio.gather(*tasks)

    if scraped_items:
        unique_scraped_items = {item['original_name']: item for item in scraped_items}.values()
        unique_scraped_items_list = list(unique_scraped_items)
        
        await db.add_movie_batch(unique_scraped_items_list)
        logger.info(f"Database update complete. Total items from this scrape: {len(unique_scraped_items_list)}")
    else:
        logger.info("No items were scraped. Database not updated.")

async def fetch_and_parse_url(session: aiohttp.ClientSession, base_url: str, results_list: list):
    content = await fetch_url(session, base_url)
    if not content:
        return

    category = get_category(base_url)
    soup = BeautifulSoup(content, 'html.parser')
    
    for link in soup.find_all('a'):
        href = link.get('href')
        text = unquote(link.text.strip())
        
        if not href or href.startswith(('?', '#', 'javascript:', '../', 'mailto:')):
            continue
        
        full_url = urljoin(base_url, href)
        item_name = text.rstrip('/')

        if not item_name or item_name == "Parent Directory":
            continue

        normalized = normalize_movie_name(item_name)
        if not normalized:
            continue

        results_list.append({
            "url": full_url,
            "type": "directory" if href.endswith('/') else "file",
            "normalized": normalized,
            "original_name": item_name,
            "category": category,
            "source": "scraped"
        })

async def scrape_files_recursive(session: aiohttp.ClientSession, base_url: str, category: str) -> list:
    """Recursively scrapes a directory for video files."""
    files_found = []
    content = await fetch_url(session, base_url)
    if not content:
        return files_found

    soup = BeautifulSoup(content, 'html.parser')
    for link in soup.find_all('a'):
        href = link.get('href')
        text = unquote(link.text.strip())

        if not href or href.startswith(('?', '#', 'javascript:', '../', 'mailto:')):
            continue

        full_url = urljoin(base_url, href)
        
        if href.endswith('/'):
            if text != "Parent Directory":
                files_found.extend(await scrape_files_recursive(session, full_url, category))
        elif any(href.lower().endswith(ext) for ext in VIDEO_EXTENSIONS):
            file_size = await get_file_size(session, full_url)
            display_name = f"[{category}] {text}" if category else text
            files_found.append((full_url, display_name, file_size))
            
    return files_found

async def get_item_files(item_original_name: str) -> list:
    item_info = await db.get_movie_details(item_original_name)
    
    if not item_info:
        return []

    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as session:
            category = item_info.get("category", "")
            if item_info["type"] == "directory":
                return await scrape_files_recursive(session, item_info["url"], category)
            elif item_info["type"] == "file":
                file_size = await get_file_size(session, item_info["url"])
                display_name = f"[{category}] {item_info['original_name']}" if category else item_info['original_name']
                return [(item_info["url"], display_name, file_size)]
            return []
    except Exception as e:
        logger.error(f"File retrieval error: {str(e)}")
        return []

async def shorten_url(url_to_shorten: str) -> str:
    if not SHRINKME_API_KEY:
        return url_to_shorten
        
    if url_to_shorten in url_shorten_cache:
        return url_shorten_cache[url_to_shorten]

    try:
        api_url = "https://shrinkme.io/api"
        params = {"api": SHRINKME_API_KEY, "url": quote(url_to_shorten)}
        
        async with aiohttp.ClientSession() as session:
            async with session.get(api_url, params=params, timeout=REQUEST_TIMEOUT) as response:
                data = await response.json()
                if data.get("status") == "success":
                    shortened = data["shortenedUrl"]
                    url_shorten_cache[url_to_shorten] = shortened
                    return shortened
        return url_to_shorten
    except Exception as e:
        logger.error(f"URL shortening failed: {str(e)}", exc_info=True)
        return url_to_shorten

async def get_movie_metadata(title: str) -> dict:
    """Fetches movie metadata from OMDb, rotating API keys on rate limits."""
    if not OMDB_API_KEYS:
        return {"Response": "False", "Error": "OMDb API keys are not configured."}

    if title in metadata_cache:
        logger.debug(f"Returning cached metadata for title: {title}")
        return metadata_cache[title]

    cleaned_title = re.sub(r'\s*\(\d{4}\).*', '', title).strip()
    year_match = re.search(r'\((\d{4})\)', title)
    year = year_match.group(1) if year_match else None

    search_url = "http://www.omdbapi.com/"
    
    # List of parameter configurations to try in order
    search_configs = []
    # First, try with the specific year if available
    if year:
        search_configs.append({"t": cleaned_title, "y": year})
    # Always have the title-only search as a primary or fallback option
    search_configs.append({"t": cleaned_title})

    async with aiohttp.ClientSession() as session:
        # Iterate through search configurations (e.g., with year, then without)
        for params in search_configs:
            # For each configuration, try all available API keys
            for i, api_key in enumerate(OMDB_API_KEYS):
                params["apikey"] = api_key
                try:
                    timeout = aiohttp.ClientTimeout(total=METADATA_REQUEST_TIMEOUT)
                    logger.info(f"Searching OMDb with key #{i} and params: { {k:v for k,v in params.items() if k != 'apikey'} }")
                    
                    async with session.get(search_url, params=params, timeout=timeout) as response:
                        data = await response.json()
                    
                    if data.get("Response") == "True":
                        metadata_cache[title] = data
                        return data
                    
                    # If rate limited, log and try the next key
                    if "limit reached" in data.get("Error", "").lower():
                        logger.warning(f"OMDb API key #{i} is rate-limited. Trying next key.")
                        continue  # Go to the next API key
                    
                    # If any other error (e.g., "Movie not found"),
                    # stop trying keys for this config and move to the next one.
                    logger.warning(f"OMDb search failed for key #{i} with error: {data.get('Error')}. Trying next search config.")
                    break # Breaks from the inner 'for api_key' loop

                except asyncio.TimeoutError:
                    logger.error(f"OMDb API request timed out for key #{i}.")
                    # Don't rotate on timeout, could be a network issue. Treat as a failure for this config.
                    break # Breaks from the inner 'for api_key' loop
                except Exception as e:
                    logger.error(f"OMDb API error for key #{i}: {str(e)}", exc_info=True)
                    # Treat as a failure for this config
                    break # Breaks from the inner 'for api_key' loop
        
        # If all search configurations and keys fail
        logger.error(f"All OMDb search attempts failed for title '{title}'.")
        return {"Response": "False", "Error": "Movie not found after all attempts."}


# --- Telegram Handlers ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.effective_user
        await db.add_user(user.id)
        user_name = user.first_name
        welcome_message = f"🎬 Welcome {user_name} to Movie Finder Bot!\n\n"
        
        user_commands_message = """✨ <b>User Commands:</b> ✨
/search &lt;movie name&gt; - Search for movies.
  <i>Example:</i> <code>/search Inception</code>
/get &lt;movie name&gt; - Get direct links for the best match.
  <i>Example:</i> <code>/get The Matrix</code>
/request &lt;movie name&gt; - Request a movie that's not available.
  <i>Example:</i> <code>/request Dune Part Two</code>
/browse - Browse movies by category.
/help - Show this help message.
"""
        full_message = welcome_message + user_commands_message

        if is_admin(user.id):
            admin_commands_message = """
👑 <b>Admin Commands:</b> 👑
/stats - View bot statistics.
/popular - See top 10 popular items.
/refreshdb - Refresh the movie database.
/url &lt;category&gt; | &lt;movie_name&gt; | &lt;link1&gt; | &lt;link2&gt;... - Add a movie manually.
  <i>Example:</i> <code>/url Bollywood | My Movie | https://link1.com</code>
/addwebseries &lt;name&gt; | &lt;category&gt; | &lt;poster_url&gt; | &lt;plot&gt; | &lt;S1E1:url1;S1E2:url2&gt; - Add a web series.
  <i>Example:</i> <code>/addwebseries My Series | Webseries | http://poster.url/img.jpg | A great series | S1E1:http://link1.com</code>
/viewrequests - View movie requests.
/broadcast &lt;message&gt; - Send a message to all users.
"""
            full_message += admin_commands_message

        await update.message.reply_text(full_message, parse_mode='HTML')

    except Exception as e:
        logger.error(f"Start command error: {str(e)}", exc_info=True)

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        help_text = """✨ <b>User Commands:</b> ✨
/search &lt;movie name&gt; - Search for movies.
  <i>Example:</i> <code>/search Inception</code>
/get &lt;movie name&gt; - Get direct links for the best match.
  <i>Example:</i> <code>/get The Matrix</code>
/request &lt;movie name&gt; - Request a movie that's not available.
  <i>Example:</i> <code>/request Dune Part Two</code>
/browse - Browse movies by category.
/help - Show this help message.
"""

        if is_admin(user_id):
            help_text += """
👑 <b>Admin Commands:</b> 👑
/stats - View bot statistics.
/popular - See top 10 popular items.
/refreshdb - Refresh the movie database.
/url &lt;category&gt; | &lt;movie_name&gt; | &lt;link1&gt; | &lt;link2&gt;... - Add a movie manually.
  <i>Example:</i> <code>/url Bollywood | My Movie | https://link1.com</code>
/addwebseries &lt;name&gt; | &lt;category&gt; | &lt;poster_url&gt; | &lt;plot&gt; | &lt;S1E1:url1;S1E2:url2&gt; - Add a web series.
  <i>Example:</i> <code>/addwebseries My Series | Webseries | http://poster.url/img.jpg | A great series | S1E1:http://link1.com</code>
/viewrequests - View movie requests.
/broadcast &lt;message&gt; - Send a message to all users.
"""
        
        await update.message.reply_text(help_text, parse_mode='HTML')

    except Exception as e:
        logger.error(f"Help command error: {str(e)}", exc_info=True)

async def handle_message_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message and update.message.text and not update.message.text.startswith('/'):
        context.args = update.message.text.split()
        await handle_search(update, context)

def get_relevancy_score(name, query):
    """Calculates a relevancy score for a search result."""
    norm_name = normalize_movie_name(name)
    # Exact match = highest score
    if norm_name == query:
        return 100
    # Query as whole word = high score
    if f" {query} " in f" {norm_name} ":
        return 90
    # Query at the start = medium score
    if norm_name.startswith(query):
        return 80
    # Lower score for general substring, penalize by length difference
    return 50 - len(norm_name)

async def handle_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await db.add_user(update.effective_user.id)
        if not context.args:
            await update.message.reply_text("❌ Please specify a movie or web series name.", parse_mode='HTML')
            return

        query = ' '.join(context.args)
        norm_query = normalize_movie_name(query)
        logger.info(f"User {update.effective_user.id} searching for: '{query}'")
        search_query_counts[query.lower()] = search_query_counts.get(query.lower(), 0) + 1
        
        processing_msg = await update.message.reply_text(f"⏳ Searching for '<b>{query}</b>'...", parse_mode='HTML')
        
        matched_names = await search_movie(query)
        if not matched_names:
            await processing_msg.edit_text(f"😞 No results for '<b>{query}</b>'. You can request it using /request.", parse_mode='HTML')
            return
            
        # Sort results by relevancy
        sorted_matched_names = sorted(matched_names, key=lambda name: get_relevancy_score(name, norm_query), reverse=True)

        keyboard = []
        for name in sorted_matched_names[:FILES_PER_PAGE]:
            item_info = await db.get_movie_details(name)
            item_type = "movie"
            if not item_info:
                item_info = await db.get_webseries_details(name)
                item_type = "webseries"

            if item_info:
                category = item_info.get("category", "N/A")
                display_text = f"[{category}] {name}" if category else name
                keyboard.append([InlineKeyboardButton(display_text, callback_data=f"select_{item_type}_{name}")])

        await processing_msg.edit_text(
            f"🎬 Results for '<b>{query}</b>':",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
    except Exception as e:
        logger.error(f"Search error: {str(e)}", exc_info=True)
        await update.message.reply_text("⚠️ An error occurred during the search.")


async def handle_get(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await db.add_user(update.effective_user.id)
        if not context.args:
            await update.message.reply_text("❌ Please specify a movie or web series name.", parse_mode='HTML')
            return

        query = ' '.join(context.args)
        norm_query = normalize_movie_name(query)
        logger.info(f"User {update.effective_user.id} direct get: '{query}'")
        
        processing_msg = await update.message.reply_text(f"⏳ Direct search for '<b>{query}</b>'...", parse_mode='HTML')
        
        matched_names = await search_movie(query)
        if not matched_names:
            await processing_msg.edit_text(f"😞 No matches for '<b>{query}</b>'.", parse_mode='HTML')
            return

        # Sort results by relevancy and pick the best one
        sorted_matched_names = sorted(matched_names, key=lambda name: get_relevancy_score(name, norm_query), reverse=True)
        best_match = sorted_matched_names[0]
        
        item_info = await db.get_movie_details(best_match)
        item_type = "movie"
        if not item_info:
            item_info = await db.get_webseries_details(best_match)
            item_type = "webseries"

        if item_info:
            await processing_msg.delete()
            await send_item_details(context, update.message.chat_id, best_match, item_type=item_type)
        else:
            await processing_msg.edit_text(f"😞 No details found for '<b>{best_match}</b>'.", parse_mode='HTML')
        
    except Exception as e:
        logger.error(f"Direct search error: {str(e)}", exc_info=True)
        await update.message.reply_text("⚠️ An error occurred during the direct search.")

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    try:
        await db.add_user(update.effective_user.id)
        data = query.data
        chat_id = query.message.chat_id
        message_id = query.message.message_id

        if data.startswith("select_"):
            parts = data.split('_', 2)
            item_type = parts[1]
            item_name = parts[2] 

            item_selection_counts[item_name] = item_selection_counts.get(item_name, 0) + 1
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=f"⏳ Loading details for <b>{item_name}</b>...",
                parse_mode='HTML'
            )
            await send_item_details(context, chat_id, item_name, item_type=item_type)
        
        elif data.startswith("page_"):
            # Correctly handle item names with underscores
            try:
                # Find the last underscore for the page number
                last_underscore_index = data.rfind('_')
                if last_underscore_index == -1:
                    raise ValueError("Invalid callback data format for page")

                page = int(data[last_underscore_index + 1:])
                
                # The rest of the data is the item type and name
                middle_part = data[len("page_"):last_underscore_index]
                
                # Find the first underscore to separate item_type from item_name
                first_underscore_index = middle_part.find('_')
                if first_underscore_index == -1:
                    raise ValueError("Invalid callback data format for page item type/name")
                    
                item_type = middle_part[:first_underscore_index]
                item_name = middle_part[first_underscore_index + 1:]

                await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=message_id,
                    text=f"⏳ Loading page {page+1} for <b>{item_name}</b>...",
                    parse_mode='HTML'
                )
                await send_item_details(context, chat_id, item_name, page=page, item_type=item_type)
            except (ValueError, IndexError) as e:
                logger.error(f"Error parsing page callback data '{data}': {e}")
                await context.bot.send_message(chat_id, "⚠️ Error processing your request.")
        
        elif data.startswith("browse_category_"):
            # Correctly handle category names with underscores
            try:
                # Find the last underscore, which separates the page number
                last_underscore_index = data.rfind('_')
                if last_underscore_index == -1:
                    raise ValueError("Invalid callback data format")

                # Extract category and page number
                category = data[len("browse_category_"):last_underscore_index]
                page_part = data[last_underscore_index + 1:]
                
                page = int(page_part)

                await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=message_id,
                    text=f"⏳ Loading {category} movies (page {page+1})...",
                    parse_mode='HTML'
                )
                await send_category_movies(context, chat_id, category, page=page)
            except (ValueError, IndexError) as e:
                logger.error(f"Error parsing browse_category callback data '{data}': {e}")
                await context.bot.send_message(chat_id, "⚠️ Error processing your request.")
            
    except Exception as e:
        logger.error(f"Callback error: {str(e)}", exc_info=True)
        await context.bot.send_message(chat_id, "⚠️ Error processing your request.")


async def send_item_details(context: CallbackContext, chat_id: int, item_name: str, page: int = 0, item_type: str = "movie"):
    if not LOG_CHANNEL_ID:
        logger.error("LOG_CHANNEL_ID is not set. Cannot use post-and-forward method.")
        await context.bot.send_message(chat_id, "⚠️ Bot configuration error. Please contact the admin.")
        return
        
    try:
        # --- Build the message content ---
        caption = ""
        reply_markup = None
        poster_url = ""

        if item_type == "movie":
            item_info = await db.get_movie_details(item_name)
            if not item_info:
                await context.bot.send_message(chat_id, "❌ Movie not found in database.")
                return

            metadata = await get_movie_metadata(item_name)
            files = []

            source = item_info.get('source', 'scraped')
            if source == 'manual':
                urls = item_info.get('url', '').split('\n')
                if not urls or not urls[0]:
                     await context.bot.send_message(chat_id, "🚫 No download links found for this manually added movie.")
                     return
                
                async with aiohttp.ClientSession() as session:
                    file_sizes = await asyncio.gather(*[get_file_size(session, u) for u in urls])
                
                files = [(url, f"{item_name} - Link {i+1}", size) for i, (url, size) in enumerate(zip(urls, file_sizes))]
            else:
                files = await get_item_files(item_name)

            if not files:
                await context.bot.send_message(chat_id, f"🚫 No download links could be found for <b>{item_name}</b>. You can request it using <code>/request {item_name}</code>", parse_mode='HTML')
                return

            caption = f"🎬 <b>{metadata.get('Title', item_name)}</b>\n"
            if metadata.get("Response") == "True":
                caption += f"⭐ Rating: {metadata.get('imdbRating', 'N/A')} | 🗓️ Year: {metadata.get('Year', 'N/A')}\n"
                caption += f"📖 Plot: {metadata.get('Plot', 'No description available.')[:250]}...\n\n"
            
            keyboard = []
            start_idx = page * FILES_PER_PAGE
            paginated_files = files[start_idx:start_idx+FILES_PER_PAGE]
            
            shorten_tasks = [shorten_url(url) for url, _, _ in paginated_files]
            shortened_urls = await asyncio.gather(*shorten_tasks)
            
            for (url, name, size), short_url in zip(paginated_files, shortened_urls):
                keyboard.append([InlineKeyboardButton(f"📥 {name[:35]} ({size})", url=short_url)])

            if len(files) > FILES_PER_PAGE:
                nav_buttons = []
                if page > 0:
                    nav_buttons.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"page_{item_type}_{item_name}_{page-1}"))
                nav_buttons.append(InlineKeyboardButton(f"📄 {page+1}/{(len(files)+FILES_PER_PAGE-1)//FILES_PER_PAGE}", callback_data="ignore"))
                if start_idx + FILES_PER_PAGE < len(files):
                    nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"page_{item_type}_{item_name}_{page+1}"))
                keyboard.append(nav_buttons)

            reply_markup = InlineKeyboardMarkup(keyboard)
            poster_url = metadata.get('Poster', '')

        elif item_type == "webseries":
            series_info = await db.get_webseries_details(item_name)
            if not series_info:
                await context.bot.send_message(chat_id, "❌ Web series not found.")
                return
            
            episodes = await db.get_episodes_for_series(series_info["id"])
            if not episodes:
                await context.bot.send_message(chat_id, f"🚫 No episodes found for this web series. You can request it using <code>/request {item_name}</code>", parse_mode='HTML')
                return

            caption = f"📺 <b>{series_info['name']}</b>\n"
            caption += f"📚 Category: {series_info.get('category', 'N/A')}\n"
            caption += f"📖 Plot: {series_info.get('plot', 'No description available.')[:250]}...\n\n"
            caption += "<b>Episodes:</b>\n"

            keyboard = []
            start_idx = page * FILES_PER_PAGE
            paginated_episodes = episodes[start_idx:start_idx+FILES_PER_PAGE]

            shorten_tasks = [shorten_url(ep["url"]) for ep in paginated_episodes]
            shortened_urls = await asyncio.gather(*shorten_tasks)

            for (ep, short_url) in zip(paginated_episodes, shortened_urls):
                ep_display_name = ep.get("name") or f"{series_info['name']} S{ep['season']}E{ep['episode']}"
                keyboard.append([InlineKeyboardButton(f"▶️ {ep_display_name}", url=short_url)])
            
            if len(episodes) > FILES_PER_PAGE:
                nav_buttons = []
                if page > 0:
                    nav_buttons.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"page_{item_type}_{item_name}_{page-1}"))
                nav_buttons.append(InlineKeyboardButton(f"📄 {page+1}/{(len(episodes)+FILES_PER_PAGE-1)//FILES_PER_PAGE}", callback_data="ignore"))
                if start_idx + FILES_PER_PAGE < len(episodes):
                    nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"page_{item_type}_{item_name}_{page+1}"))
                keyboard.append(nav_buttons)

            reply_markup = InlineKeyboardMarkup(keyboard)
            poster_url = series_info.get('poster_url', '')
        
        else:
            await context.bot.send_message(chat_id, "❌ Unknown item type.")
            return

        # --- Post and Forward Logic ---
        try:
            # 1. Send the message to the private log channel
            if poster_url and poster_url.startswith('http'):
                sent_message = await context.bot.send_photo(
                    chat_id=LOG_CHANNEL_ID,
                    photo=poster_url,
                    caption=caption,
                    reply_markup=reply_markup,
                    parse_mode='HTML'
                )
            else:
                sent_message = await context.bot.send_message(
                    chat_id=LOG_CHANNEL_ID,
                    text=caption,
                    reply_markup=reply_markup,
                    parse_mode='HTML'
                )
            
            # 2. Forward the message from the log channel to the user
            await context.bot.forward_message(
                chat_id=chat_id,
                from_chat_id=LOG_CHANNEL_ID,
                message_id=sent_message.message_id
            )

        except BadRequest as e:
            logger.error(f"Message sending/forwarding error (BadRequest): {str(e)}. Sending text only as fallback.")
            await context.bot.send_message(
                chat_id=chat_id,
                text=caption,
                reply_markup=reply_markup,
                parse_mode='HTML'
            )
        except Exception as e:
            logger.error(f"Message sending/forwarding error: {str(e)}", exc_info=True)
            await context.bot.send_message(chat_id, "⚠️ Error displaying results.")
            
    except Exception as e:
        logger.error(f"Details error: {str(e)}", exc_info=True)
        await context.bot.send_message(chat_id, "⚠️ Error processing request.")

async def send_category_movies(context: CallbackContext, chat_id: int, category: str, page: int = 0):
    try:
        offset = page * FILES_PER_PAGE
        movie_names = await db.get_movies_by_category(category, offset, FILES_PER_PAGE)
        webseries_names = await db.get_webseries_by_category(category, offset, FILES_PER_PAGE)
        
        all_items = []
        for name in movie_names:
            all_items.append((name, "movie"))
        for name in webseries_names:
            all_items.append((name, "webseries"))

        all_items.sort(key=lambda x: x[0])

        total_movies = await db.count_movies_in_category(category)
        total_webseries = await db.count_webseries_in_category(category)
        total_items = total_movies + total_webseries

        if not all_items:
            await context.bot.send_message(chat_id, f"No items found in the <b>{category}</b> category.", parse_mode='HTML')
            return

        keyboard = []
        for name, item_type in all_items:
            keyboard.append([InlineKeyboardButton(name, callback_data=f"select_{item_type}_{name}")])

        if total_items > FILES_PER_PAGE:
            nav_buttons = []
            if page > 0:
                nav_buttons.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"browse_category_{category}_{page-1}"))
            nav_buttons.append(InlineKeyboardButton(f"📄 {page+1}/{(total_items + FILES_PER_PAGE - 1) // FILES_PER_PAGE}", callback_data="ignore"))
            if offset + len(all_items) < total_items:
                nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"browse_category_{category}_{page+1}"))
            keyboard.append(nav_buttons)

        reply_markup = InlineKeyboardMarkup(keyboard)
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"🎬 Items in <b>{category}</b> (Page {page+1}):",
            reply_markup=reply_markup,
            parse_mode='HTML'
        )

    except Exception as e:
        logger.error(f"Error in send_category_movies: {e}", exc_info=True)
        await context.bot.send_message(chat_id, "⚠️ An error occurred while fetching category movies.")

# --- Admin Commands ---
async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return

    total_movies = await db.get_movie_count()
    total_webseries = await db.count_webseries()
    stats_text = (
        f"📊 <b>Bot Statistics</b> 📊\n\n"
        f"🎬 Total indexed movies: <b>{total_movies}</b>\n"
        f"📺 Total indexed web series: <b>{total_webseries}</b>\n"
        f"📈 Total searches performed: <b>{sum(search_query_counts.values())}</b>\n"
        f"🔍 Unique search queries: <b>{len(search_query_counts)}</b>\n"
        f"✅ Total items selected: <b>{sum(item_selection_counts.values())}</b>"
    )
    await update.message.reply_text(stats_text, parse_mode='HTML')

async def popular_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return

    top_items = sorted(item_selection_counts.items(), key=lambda x: x[1], reverse=True)[:10]
    if not top_items:
        await update.message.reply_text("No popular items yet.")
        return
        
    popular_text = "🌟 Top Popular Items:\n" + "\n".join(
        f"{i+1}. {item} ({count} selections)" for i, (item, count) in enumerate(top_items)
    )
    await update.message.reply_text(popular_text)

async def refresh_db_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    
    msg = await update.message.reply_text("🔄 Refreshing database from sources... This may take a while.")
    start_time = time.time()
    await scrape_and_update_db()
    total_movies = await db.get_movie_count()
    await msg.edit_text(f"✅ Database refreshed in {time.time()-start_time:.2f}s. Total movies: {total_movies}")

async def handle_add_url(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return

    try:
        full_text = " ".join(context.args)
        parts = [p.strip() for p in full_text.split('|') if p.strip()]

        if len(parts) < 3:
            await update.message.reply_text("❌ Invalid format. Use: /url <category> | <movie_name> | <link1> | <link2> ...")
            return

        category, name = parts[0], parts[1]
        urls = parts[2:]
        normalized_name = normalize_movie_name(name)

        for url in urls:
            if not url.startswith(('http://', 'https://')):
                await update.message.reply_text(f"❌ Invalid URL: {url}. Please provide valid download links.")
                return

        existing_movie = await db.get_movie_by_normalized_name(normalized_name)
        if existing_movie:
            await update.message.reply_text(f"⚠️ Movie '<b>{name}</b>' (normalized: '<b>{normalized_name}</b>') already exists.", parse_mode='HTML')
            return

        url_string = "\n".join(urls)

        await db.add_single_movie(name, url_string, 'file', normalized_name, category, source='manual')
        await update.message.reply_text(f"✅ Successfully added '<b>{name}</b>' with {len(urls)} link(s) to the <b>{category}</b> category.", parse_mode='HTML')

    except Exception as e:
        logger.error(f"Error in handle_add_url: {e}", exc_info=True)
        await update.message.reply_text("⚠️ An error occurred while adding the URL.")

async def handle_add_webseries(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return

    try:
        full_text = " ".join(context.args)
        parts = full_text.split('|')

        if len(parts) < 5:
            await update.message.reply_text("❌ Invalid format. Use: /addwebseries <name> | <category> | <poster_url> | <plot> | <S1E1:url1;S1E2:url2;...>")
            return

        series_name, category, poster_url, plot, episodes_data_str = [p.strip() for p in parts]

        if not all([series_name, category, poster_url, plot, episodes_data_str]):
            await update.message.reply_text("❌ All fields are required.")
            return

        if not poster_url.startswith(('http://', 'https://')):
            await update.message.reply_text("❌ Invalid Poster URL.")
            return

        normalized_series_name = normalize_movie_name(series_name)

        if await db.get_webseries_details(series_name):
            await update.message.reply_text(f"⚠️ Web series '<b>{series_name}</b>' already exists.", parse_mode='HTML')
            return

        series_id = await db.add_webseries(series_name, category, poster_url, plot, normalized_series_name)
        if series_id is None:
            await update.message.reply_text("⚠️ Could not create web series in the database.")
            return

        episodes_list = episodes_data_str.split(';')
        for episode_entry in episodes_list:
            if ':' not in episode_entry:
                logger.warning(f"Skipping invalid episode format: {episode_entry}")
                continue
            
            ep_info, ep_url = [item.strip() for item in episode_entry.split(':', 1)]
            match = re.match(r'S(\d+)E(\d+)', ep_info, re.IGNORECASE)
            if not match:
                logger.warning(f"Skipping invalid episode identifier: {ep_info}")
                continue
            
            season_num, episode_num = int(match.group(1)), int(match.group(2))
            default_episode_name = f"{series_name} S{season_num}E{episode_num}"
            await db.add_episode(series_id, season_num, episode_num, ep_url, episode_name=default_episode_name)
        
        await update.message.reply_text(f"✅ Successfully added web series '<b>{series_name}</b>' with {len(episodes_list)} episodes.", parse_mode='HTML')

    except Exception as e:
        logger.error(f"Error in handle_add_webseries: {e}", exc_info=True)
        await update.message.reply_text("⚠️ An error occurred. Check logs.")

async def handle_request(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await db.add_user(update.effective_user.id)
        if not context.args:
            await update.message.reply_text("❌ Please specify the movie you want to request. Usage: /request <movie name>")
            return

        movie_title = ' '.join(context.args)
        user_id = update.effective_user.id
        await db.add_request(user_id, movie_title)
        await update.message.reply_text(f"✅ Your request for '<b>{movie_title}</b>' has been logged.", parse_mode='HTML')

    except Exception as e:
        logger.error(f"Error in handle_request: {e}", exc_info=True)
        await update.message.reply_text("⚠️ An error occurred while logging your request.")

async def handle_view_requests(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return

    try:
        requests = await db.get_requests()
        if not requests:
            await update.message.reply_text("No movie requests at the moment.")
            return

        message = "<b>Movie Requests (Top 10):</b>\n\n"
        for i, (title, count) in enumerate(requests[:10]):
            message += f"<b>{i+1}.</b> {title} (<i>{count} requests</i>)\n"
        
        await update.message.reply_text(message, parse_mode='HTML')

    except Exception as e:
        logger.error(f"Error in handle_view_requests: {e}", exc_info=True)
        await update.message.reply_text("⚠️ An error occurred while fetching requests.")

async def handle_browse(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await db.add_user(update.effective_user.id)
        categories = sorted(list(set(CATEGORY_KEYWORDS)))
        keyboard = [[InlineKeyboardButton(category, callback_data=f"browse_category_{category}_0")] for category in categories]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text("Browse movies by category:", reply_markup=reply_markup)

    except Exception as e:
        logger.error(f"Error in handle_browse: {e}", exc_info=True)
        await update.message.reply_text("⚠️ An error occurred while fetching categories.")

async def handle_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the /broadcast command for admins."""
    if not is_admin(update.effective_user.id):
        return

    message_to_broadcast = ' '.join(context.args)
    if not message_to_broadcast:
        await update.message.reply_text("❌ Please provide a message to broadcast. Usage: /broadcast <message>")
        return

    user_ids = await db.get_all_user_ids()
    if not user_ids:
        await update.message.reply_text("No users found in the database to broadcast to.")
        return

    msg = await update.message.reply_text(f"📢 Starting broadcast to {len(user_ids)} users...")

    success_count = 0
    fail_count = 0

    for user_id in user_ids:
        try:
            await context.bot.send_message(chat_id=user_id, text=message_to_broadcast, parse_mode='HTML')
            success_count += 1
        except (BadRequest, Forbidden) as e:
            # BadRequest can happen if chat not found, Forbidden if user blocked the bot
            logger.warning(f"Failed to send broadcast to {user_id}: {e}")
            fail_count += 1
        except Exception as e:
            logger.error(f"An unexpected error occurred when broadcasting to {user_id}: {e}", exc_info=True)
            fail_count += 1
        await asyncio.sleep(0.1) # Small delay to avoid hitting rate limits

    summary_text = (
        f"✅ Broadcast complete!\n\n"
        f"Sent successfully: {success_count}\n"
        f"Failed to send: {fail_count}"
    )
    await msg.edit_text(summary_text)


# --- Error Handling ---
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Exception while handling an update: {context.error}", exc_info=context.error)
    if isinstance(update, Update) and update.effective_message:
        try:
            await update.effective_message.reply_text("⚠️ An unexpected error occurred. The developers have been notified.")
        except Exception as e:
            logger.error(f"Error notification failed: {str(e)}")


async def post_init_tasks(application: Application):
    """Initializes DB and runs tasks after the bot is initialized."""
    logger.info("Running post-initialization tasks...")
    # 1. Initialize Database
    try:
        await db.initialize_db()
    except Exception as e:
        logger.critical(f"Database initialization failed in post_init_tasks: {e}. The application will not start.")
        # Re-raising the exception will prevent the bot from starting.
        raise

    # 2. Set Bot Commands
    user_commands = [
        BotCommand("start", "▶️ Start the bot"),
        BotCommand("search", "🔍 Search for a movie"),
        BotCommand("get", "⚡️ Get a movie directly"),
        BotCommand("request", "🙋‍♀️ Request a movie"),
        BotCommand("browse", "🗂️ Browse categories"),
        BotCommand("help", "❓ Show help message")
    ]
    
    # Set default commands for all users
    await application.bot.set_my_commands(user_commands, scope=BotCommandScopeDefault())

    if ADMIN_IDS:
        admin_commands = user_commands + [
            BotCommand("stats", "📊 View bot statistics (Admin)"),
            BotCommand("popular", "🌟 See popular items (Admin)"),
            BotCommand("refreshdb", "🔄 Refresh the movie database (Admin)"),
            BotCommand("url", "➕ Add a new movie URL (Admin)"),
            BotCommand("addwebseries", "➕ Add a new web series (Admin)"),
            BotCommand("viewrequests", "📥 View movie requests (Admin)"),
            BotCommand("broadcast", "📢 Send a message to all users (Admin)")
        ]
        # Set extended commands for each admin
        for admin_id in ADMIN_IDS:
            await application.bot.set_my_commands(admin_commands, scope=BotCommandScopeChat(chat_id=admin_id))
    
    # 3. Perform initial scrape if DB is empty
    if await db.get_movie_count() == 0 and await db.count_webseries() == 0:
        logger.info("Database is empty. Performing initial scrape in the background.")
        asyncio.create_task(scrape_and_update_db())
    logger.info("Post-initialization tasks complete.")

# --- Main Application ---
def main() -> None:
    """Start the bot."""
    if not BOT_TOKEN:
        logger.critical("FATAL: BOT_TOKEN environment variable not set.")
        sys.exit(1)

    # Use the ApplicationBuilder for setup
    application = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .post_init(post_init_tasks)
        .build()
    )

    # Add handlers
    application.add_handler(CommandHandler('start', start))
    application.add_handler(CommandHandler('help', help_command))
    
    application.add_handler(CommandHandler('search', handle_search))
    application.add_handler(CommandHandler('get', handle_get))
    application.add_handler(CommandHandler('request', handle_request))
    application.add_handler(CommandHandler('browse', handle_browse))
    application.add_handler(CallbackQueryHandler(handle_callback))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message_search))

    # Admin commands
    if ADMIN_IDS:
        admin_filter = filters.User(user_id=ADMIN_IDS)
        application.add_handler(CommandHandler('stats', stats_command, filters=admin_filter))
        application.add_handler(CommandHandler('popular', popular_command, filters=admin_filter))
        application.add_handler(CommandHandler('refreshdb', refresh_db_command, filters=admin_filter))
        application.add_handler(CommandHandler('url', handle_add_url, filters=admin_filter))
        application.add_handler(CommandHandler('addwebseries', handle_add_webseries, filters=admin_filter))
        application.add_handler(CommandHandler('viewrequests', handle_view_requests, filters=admin_filter))
        application.add_handler(CommandHandler('broadcast', handle_broadcast, filters=admin_filter))

    application.add_error_handler(error_handler)

    logger.info("Bot is starting up...")
    
    # run_polling() is a blocking call that runs the bot indefinitely
    application.run_polling()


if __name__ == '__main__':
    main()
