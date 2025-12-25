import sqlite3
import json
import time
from pathlib import Path
from dotenv import load_dotenv
import os
from googleapiclient.discovery import build
from datetime import datetime


load_dotenv()
BASE_DIR = Path(__file__).resolve().parent.parent
JOBS_DB = BASE_DIR / "data" / "jobs.db"
RAW_DIR = BASE_DIR / "data" / "raw" / "youtube"
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"
RAW_DIR.mkdir(parents=True, exist_ok=True)

def youTube_search(query):
    youtube = build(
        YOUTUBE_API_SERVICE_NAME,
        YOUTUBE_API_VERSION,
        developerKey=YOUTUBE_API_KEY,
    )

    request = youtube.search().list(
        part="snippet",
        q=query,
        maxResults=5,
        type="video"
    )
    response = request.execute()
    return response

def connect_db():
    conn = sqlite3.connect(str(JOBS_DB))
    conn.row_factory = sqlite3.Row
    return conn

def fetch_spotify_tracks():
    conn = connect_db()
    cursor = conn.cursor()
    cursor.execute(
        """
    Select spotify_track_id ,track_name ,artist
    FROM spotify_tracks_silver
    """
    )
    rows = cursor.fetchall()
    conn.close()
    return rows


def ingest_youtube_bronze():
    tracks = fetch_spotify_tracks()
    print(f"Fetched {len(tracks)} tracks from spotify_tracks_silver")

    for idx,track in enumerate(tracks, start=1):
        spotify_track_id = track["spotify_track_id"]
        track_name = track["track_name"]
        artist = track["artist"]

        out_path = RAW_DIR / f"{spotify_track_id}.json"

        if out_path.exists():
            print(f"[SKIP] {spotify_track_id} already exists.")
            continue
        query = f"{track_name} {artist} lyrics"
        print(f"[{idx}/{len(tracks)}] Searching YouTube for: {query}")
        try:
            search_response = youTube_search(query)

            payload = {
                "spotify_track_id": spotify_track_id,
                "query": query,
                "fetched_at": datetime.utcnow().isoformat(),
                "youtube_search_response": search_response,
            }
            with open(out_path,"w",encoding="utf-8") as f:
                json.dump(payload,f,ensure_ascii=False,indent=2)
            time.sleep(0.3)
        except Exception as e:
            print(f"Error fetching YouTube data for {spotify_track_id}: {e}")

if __name__ == "__main__":
    ingest_youtube_bronze()
