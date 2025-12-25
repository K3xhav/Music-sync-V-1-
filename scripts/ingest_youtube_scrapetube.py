import sqlite3
import json
from datetime import datetime
from pathlib import Path
import time
import scrapetube

# ========== PATHS ==========
BASE_DIR = Path(__file__).parent.parent
JOBS_DB = BASE_DIR / "data" / "jobs.db"
RAW_DIR = BASE_DIR / "data" / "scrapetube" / "youtube"
RAW_DIR.mkdir(parents=True, exist_ok=True)

# ========== DB ==========
def get_db_connection():
    conn = sqlite3.connect(str(JOBS_DB))
    conn.row_factory = sqlite3.Row
    return conn

def fetch_spotify_tracks():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT spotify_track_id, track_name, artist
        FROM spotify_tracks_silver
    """)
    rows = cursor.fetchall()
    conn.close()
    return rows

# ========== INGEST ==========
def ingest_youtube_scrapetube():
    tracks = fetch_spotify_tracks()
    print(f"Fetched {len(tracks)} tracks from spotify_tracks_silver")

    for idx, track in enumerate(tracks, start=1):
        spotify_track_id = track["spotify_track_id"]
        track_name = track["track_name"]
        artist = track["artist"]

        query = f'{track_name} {artist} Topic'
        print(f"[{idx}/{len(tracks)}] Searching YouTube: {query}")

        try:
            results = scrapetube.get_search(
                query=query,
                limit=3,          # IMPORTANT: keep low
                sleep=1,
                results_type="video",
                sort_by="relevance"
            )

            candidates = []
            for rank, video in enumerate(results, start=1):
                candidates.append({
                    "video_id": video.get("videoId"),
                    "title": video.get("title", {}).get("runs", [{}])[0].get("text"),
                    "channel": video.get("ownerText", {}).get("runs", [{}])[0].get("text"),
                    "ranking_in_search": rank,
                    "publish_time": video.get("publishedTimeText", {}).get("simpleText"),
                })

            payload = {
                "spotify_track_id": spotify_track_id,
                "query": query,
                "fetched_at": datetime.utcnow().isoformat(),
                "ingestion_method": "scrapetube_search",
                "candidates": candidates,
            }

            with open(RAW_DIR / f"{spotify_track_id}.json", "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)

            time.sleep(1)  # global safety pause

        except Exception as e:
            print(f"Failed for {spotify_track_id}: {e}")

    print("scrapetube Bronze ingestion completed")

# ========== MAIN ==========
if __name__ == "__main__":
    ingest_youtube_scrapetube()
