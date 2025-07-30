import os
import logging
from supabase import create_client, Client
import time

logger = logging.getLogger(__name__)

supabase_client: Client | None = None


def get_supabase_client() -> Client:
    """
    Initializes and returns a Supabase client instance using lazy loading.
    Credentials are read from environment variables when the client is first requested.
    """
    global supabase_client
    if supabase_client is None:
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_KEY")
        if not supabase_url or not supabase_key:
            logger.critical(
                "FATAL: SUPABASE_URL or SUPABASE_KEY environment variables not set."
            )
            raise ValueError("Supabase credentials are not set in the environment.")
        supabase_client = create_client(supabase_url, supabase_key)
    return supabase_client


async def initialize_db():
    """Initializes and tests the database connection. Raises an exception on failure."""
    logger.info("Initializing Supabase database connection...")
    try:
        client = get_supabase_client()
        # --- FIX: Removed 'await' as the user's library version uses a synchronous .execute() method ---
        client.table("movies").select("id", count="exact").limit(1).execute()
        logger.info("Successfully connected to Supabase.")
    except Exception as e:
        logger.error(f"Failed to connect to Supabase: {e}", exc_info=True)
        raise e

# --- User Functions ---
async def add_user(user_id: int):
    """Adds or updates a user in the database for broadcast purposes."""
    client = get_supabase_client()
    try:
        client.table("users").upsert({"user_id": user_id}, on_conflict="user_id").execute()
        logger.info(f"Upserted user_id: {user_id}")
    except Exception as e:
        logger.error(f"Error upserting user {user_id}: {e}", exc_info=True)


async def get_all_user_ids() -> list[int]:
    """Retrieves a list of all unique user IDs from the database."""
    client = get_supabase_client()
    try:
        response = client.table("users").select("user_id").execute()
        return [item['user_id'] for item in response.data]
    except Exception as e:
        logger.error(f"Error getting all user IDs: {e}", exc_info=True)
        return []


# --- Movie Functions ---
async def clear_scraped_movies():
    """Deletes all records from the movies table that were added by scraping."""
    client = get_supabase_client()
    try:
        response = client.table("movies").delete().eq("source", "scraped").execute()
        logger.info(f"Cleared {len(response.data)} scraped movies from Supabase.")
    except Exception as e:
        logger.error(f"Error clearing scraped movies from Supabase: {e}", exc_info=True)


async def add_movie_batch(movie_items: list):
    """Adds a batch of movie items to the database."""
    if not movie_items:
        return

    client = get_supabase_client()
    timestamp = int(time.time())
    records_to_insert = [
        {
            "name": item["original_name"],
            "url": item["url"],
            "type": item["type"],
            "normalized_name": item["normalized"],
            "category": item["category"],
            "source": item.get("source", "scraped"),
            "last_updated": timestamp,
        }
        for item in movie_items
    ]
    try:
        response = (
            client.table("movies")
            .upsert(records_to_insert, on_conflict="name")
            .execute()
        )
        logger.info(
            f"Successfully added/updated {len(response.data)} movies in Supabase."
        )
    except Exception as e:
        logger.error(f"Error adding movie batch to Supabase: {e}", exc_info=True)


async def search_movies_by_normalized_name(normalized_query: str, limit: int = 15):
    """
    Searches for movies where the normalized_name contains all words from the query,
    matching them as whole words for better accuracy.
    """
    client = get_supabase_client()
    query_words = normalized_query.split()

    if not query_words:
        return []

    try:
        query = client.table("movies").select("name, normalized_name")

        # For each word in the search query, build a filter that matches it as a whole word.
        # This is more precise than a simple 'contains' check.
        for word in query_words:
            or_filter = (
                f"normalized_name.eq.{word},"  # Exact match
                f"normalized_name.ilike.{word} %,"  # Starts with word
                f"normalized_name.ilike.% {word},"  # Ends with word
                f"normalized_name.ilike.% {word} %"  # Contains word with spaces
            )
            query = query.or_(or_filter)

        response = query.limit(limit).execute()
        return [row["name"] for row in response.data]
    except Exception as e:
        logger.error(f"Error searching movies in Supabase: {e}", exc_info=True)
        return []


async def get_movie_details(name: str):
    """Retrieves all details for a specific movie by its exact name."""
    client = get_supabase_client()
    try:
        response = (
            client.table("movies")
            .select("name, url, type, category, source")
            .eq("name", name)
            .limit(1)
            .execute()
        )
        if response.data:
            row = response.data[0]
            return {
                "original_name": row["name"],
                "url": row["url"],
                "type": row["type"],
                "category": row["category"],
                "source": row.get("source", "scraped"),
            }
    except Exception as e:
        logger.error(f"Error getting movie details from Supabase: {e}", exc_info=True)
    return None


async def get_movie_count():
    """Returns the total number of movies in the database."""
    client = get_supabase_client()
    try:
        response = client.table("movies").select("id", count="exact").execute()
        return response.count
    except Exception as e:
        logger.error(f"Error getting movie count from Supabase: {e}", exc_info=True)
    return 0


async def get_movie_by_normalized_name(normalized_name: str):
    """Retrieves a movie by its normalized name."""
    client = get_supabase_client()
    try:
        response = (
            client.table("movies")
            .select("name, url, type, category, source")
            .eq("normalized_name", normalized_name)
            .limit(1)
            .execute()
        )
        if response.data:
            return response.data[0]
    except Exception as e:
        logger.error(
            f"Error getting movie by normalized name from Supabase: {e}", exc_info=True
        )
    return None


async def add_single_movie(
    name: str,
    url: str,
    item_type: str,
    normalized_name: str,
    category: str,
    source: str = "manual",
):
    """Adds or updates a single movie in the database."""
    client = get_supabase_client()
    timestamp = int(time.time())
    try:
        client.table("movies").upsert(
            {
                "name": name,
                "url": url,
                "type": item_type,
                "normalized_name": normalized_name,
                "category": category,
                "source": source,
                "last_updated": timestamp,
            },
            on_conflict="name",
        ).execute()
        logger.info(f"Successfully added/updated '{name}' in Supabase.")
    except Exception as e:
        logger.error(f"Error adding single movie to Supabase: {e}", exc_info=True)


# --- Request Functions ---
async def add_request(user_id: int, movie_title: str):
    client = get_supabase_client()
    timestamp = int(time.time())
    try:
        client.table("requests").insert(
            {"user_id": user_id, "movie_title": movie_title, "timestamp": timestamp}
        ).execute()
        logger.info(f"User {user_id} requested '{movie_title}' in Supabase.")
    except Exception as e:
        logger.error(f"Error adding request to Supabase: {e}", exc_info=True)


async def get_requests():
    client = get_supabase_client()
    try:
        response = client.table("requests").select("movie_title").execute()

        request_counts = {}
        for row in response.data:
            title = row["movie_title"]
            request_counts[title] = request_counts.get(title, 0) + 1

        return sorted(request_counts.items(), key=lambda item: item[1], reverse=True)
    except Exception as e:
        logger.error(f"Error getting requests from Supabase: {e}", exc_info=True)
    return []


async def get_movies_by_category(category: str, offset: int = 0, limit: int = 10):
    client = get_supabase_client()
    try:
        response = (
            client.table("movies")
            .select("name")
            .eq("category", category)
            .order("name")
            .range(offset, offset + limit - 1)
            .execute()
        )
        return [row["name"] for row in response.data]
    except Exception as e:
        logger.error(
            f"Error getting movies by category from Supabase: {e}", exc_info=True
        )
    return []


async def count_movies_in_category(category: str):
    client = get_supabase_client()
    try:
        response = (
            client.table("movies")
            .select("id", count="exact")
            .eq("category", category)
            .execute()
        )
        return response.count
    except Exception as e:
        logger.error(
            f"Error counting movies in category from Supabase: {e}", exc_info=True
        )
    return 0


# --- Webseries Functions ---
async def add_webseries(
    name: str, category: str, poster_url: str, plot: str, normalized_name: str
) -> int | None:
    client = get_supabase_client()
    timestamp = int(time.time())
    try:
        response = (
            client.table("webseries")
            .insert(
                {
                    "name": name,
                    "category": category,
                    "poster_url": poster_url,
                    "plot": plot,
                    "normalized_name": normalized_name,
                    "last_updated": timestamp,
                }
            )
            .execute()
        )
        logger.info(f"Successfully added webseries '{name}' to Supabase.")
        return response.data[0]["id"]
    except Exception as e:
        logger.error(f"Error adding webseries to Supabase: {e}", exc_info=True)
    return None


async def add_episode(
    series_id: int,
    season_number: int,
    episode_number: int,
    url: str,
    episode_name: str | None = None,
):
    client = get_supabase_client()
    try:
        client.table("episodes").upsert(
            {
                "series_id": series_id,
                "season_number": season_number,
                "episode_number": episode_number,
                "url": url,
                "episode_name": episode_name,
            },
            on_conflict="series_id,season_number,episode_number",
        ).execute()
        logger.info(
            f"Added episode S{season_number}E{episode_number} for series ID {series_id} to Supabase."
        )
    except Exception as e:
        logger.error(f"Error adding episode to Supabase: {e}", exc_info=True)


async def get_webseries_details(name: str):
    client = get_supabase_client()
    try:
        response = (
            client.table("webseries")
            .select("id, name, category, poster_url, plot")
            .eq("name", name)
            .limit(1)
            .execute()
        )
        if response.data:
            return response.data[0]
    except Exception as e:
        logger.error(
            f"Error getting webseries details from Supabase: {e}", exc_info=True
        )
    return None


async def get_episodes_for_series(series_id: int):
    client = get_supabase_client()
    try:
        response = (
            client.table("episodes")
            .select("season_number, episode_number, url, episode_name")
            .eq("series_id", series_id)
            .order("season_number")
            .order("episode_number")
            .execute()
        )
        return [
            {
                "season": row["season_number"],
                "episode": row["episode_number"],
                "url": row["url"],
                "name": row["episode_name"],
            }
            for row in response.data
        ]
    except Exception as e:
        logger.error(
            f"Error getting episodes for series from Supabase: {e}", exc_info=True
        )
    return []


async def search_webseries_by_normalized_name(normalized_query: str, limit: int = 15):
    """
    Searches for webseries where the normalized_name contains all words from the query,
    matching them as whole words for better accuracy.
    """
    client = get_supabase_client()
    query_words = normalized_query.split()

    if not query_words:
        return []

    try:
        query = client.table("webseries").select("name, normalized_name")
        
        # For each word in the search query, build a filter that matches it as a whole word.
        for word in query_words:
            or_filter = (
                f"normalized_name.eq.{word},"  # Exact match
                f"normalized_name.ilike.{word} %,"  # Starts with word
                f"normalized_name.ilike.% {word},"  # Ends with word
                f"normalized_name.ilike.% {word} %"  # Contains word with spaces
            )
            query = query.or_(or_filter)

        response = query.limit(limit).execute()
        return [row["name"] for row in response.data]
    except Exception as e:
        logger.error(f"Error searching webseries in Supabase: {e}", exc_info=True)
        return []


async def get_webseries_by_category(category: str, offset: int = 0, limit: int = 10):
    client = get_supabase_client()
    try:
        response = (
            client.table("webseries")
            .select("name")
            .eq("category", category)
            .order("name")
            .range(offset, offset + limit - 1)
            .execute()
        )
        return [row["name"] for row in response.data]
    except Exception as e:
        logger.error(
            f"Error getting webseries by category from Supabase: {e}", exc_info=True
        )
    return []


async def count_webseries_in_category(category: str):
    client = get_supabase_client()
    try:
        response = (
            client.table("webseries")
            .select("id", count="exact")
            .eq("category", category)
            .execute()
        )
        return response.count
    except Exception as e:
        logger.error(
            f"Error counting webseries in category from Supabase: {e}", exc_info=True
        )
    return 0


async def count_webseries():
    client = get_supabase_client()
    try:
        response = client.table("webseries").select("id", count="exact").execute()
        return response.count
    except Exception as e:
        logger.error(f"Error counting webseries from Supabase: {e}", exc_info=True)
    return 0
