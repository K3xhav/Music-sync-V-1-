import sqlite3
import json
from pathlib import Path

# ========== PATHS ==========
BASE_DIR = Path(__file__).parent.parent
JOBS_DB = BASE_DIR / "data" / "jobs.db"
RAW_DIR = BASE_DIR / "data" / "scrapetube" / "youtube"


def get_db_connection():
    conn = sqlite3.connect(str(JOBS_DB))
    conn.row_factory = sqlite3.Row
    return conn


def create_youtube_tracks_silver_table():
    conn = get_db_connection()
    cursor = conn.cursor()


    create_table_sql = """
    CREATE TABLE youtube_tracks_silver (
        spotify_track_id TEXT,
        youtube_video_id TEXT,
        title TEXT,
        channel_name TEXT,
        duration_seconds INTEGER,
        view_count INTEGER,
        ranking_in_search INTEGER,
        time_of_upload TEXT,
        fetched_at TEXT
    )
    """

    cursor.execute(create_table_sql)
    conn.commit()
    conn.close()
    print("Recreated youtube_tracks_silver table")


def select_best_candidate(candidates):
    """
      selection logic
    - Prefer 'Topic' channels
    - Otherwise fallback to rank 1
    """

    if not candidates:
        return None

    topic_candidates = [
        c
        for c in candidates
        if c.get("channel") and "topic" in c.get("channel").lower()
    ]

    if topic_candidates:
        # pick lowest ranking among Topic channels
        return min(topic_candidates, key=lambda c: c.get("ranking_in_search", 999))

    # fallback: rank 1
    return min(candidates, key=lambda c: c.get("ranking_in_search", 999))


def extract_and_insert_youtube_silver_data():
    conn = get_db_connection()
    cursor = conn.cursor()

    for json_file in RAW_DIR.glob("*.json"):
        with open(json_file, "r", encoding="utf-8") as f:
            raw_data = json.load(f)

        spotify_track_id = raw_data["spotify_track_id"]
        fetched_at = raw_data.get("fetched_at")
        candidates = raw_data.get("candidates", [])


        best = select_best_candidate(candidates)

        if not best:
            continue

        cursor.execute(
            """
            INSERT INTO youtube_tracks_silver (
                spotify_track_id,
                youtube_video_id,
                title,
                channel_name,
                duration_seconds,
                view_count,
                ranking_in_search,
                time_of_upload,
                fetched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                spotify_track_id,
                best.get("video_id"),
                best.get("title"),
                best.get("channel"),
                None,
                None,
                best.get("ranking_in_search"),
                best.get("publish_time"),
                fetched_at,
            ),
        )

    conn.commit()
    conn.close()
    print("Inserted ONE selected YouTube video per Spotify track into Silver")


if __name__ == "__main__":
    create_youtube_tracks_silver_table()
    extract_and_insert_youtube_silver_data()
