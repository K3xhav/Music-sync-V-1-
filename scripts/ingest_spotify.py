import sqlite3
import spotipy
import os
from pathlib import Path
import sys
import json
from datetime import datetime
from dotenv import load_dotenv
from spotipy.oauth2 import SpotifyClientCredentials
from spotipy.exceptions import SpotifyException
import time

BASE_DIR = Path(__file__).resolve().parent.parent
JOBS_DB = BASE_DIR / 'data' / 'jobs.db'
RAW_DIR = BASE_DIR / 'data' / 'raw' / 'spotify'
ENV_CLIENT_ID = "SPOTIPY_CLIENT_ID"
ENV_CLIENT_SECRET = "SPOTIPY_CLIENT_SECRET"
JOBS_DB.parent.mkdir(parents=True, exist_ok=True)


def get_db_conn():
    conn = sqlite3.connect(str(JOBS_DB))
    conn.row_factory = sqlite3.Row
    return conn

def get_spotify_playlist_id(job_id: str) -> str:
    conn = get_db_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT spotify_playlist_id FROM playlist_conversion_job WHERE job_id = ?",
            (job_id,),
        )
        row = cur.fetchone()
        if row and row["spotify_playlist_id"]:
            return row["spotify_playlist_id"]
        raise ValueError(f"No spotify_playlist_id for job_id={job_id}")
    finally:
        conn.close()

def build_spotify_client():
    load_dotenv(BASE_DIR / ".env")
    cid = os.getenv(ENV_CLIENT_ID)
    secret = os.getenv(ENV_CLIENT_SECRET)
    if not cid or not secret:
        raise ValueError(f"Set {ENV_CLIENT_ID} and {ENV_CLIENT_SECRET} in .env")
    auth = SpotifyClientCredentials(client_id=cid, client_secret=secret)
    return spotipy.Spotify(auth_manager=auth, requests_timeout=10, retries=3)


def fetch_spotify_playlist_raw(job_id: str, max_retries: int = 3):
    playlist_id = get_spotify_playlist_id(job_id)
    sp = build_spotify_client()

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    out_path = RAW_DIR / f"{job_id}.json"

    attempt = 0
    while True:
        try:
            meta = sp.playlist(
                playlist_id, fields="name,description,tracks(total),owner"
            )
            total = meta.get("tracks", {}).get("total", 0)

            items = []
            limit = 100
            offset = 0
            while True:
                page = sp.playlist_tracks(playlist_id, limit=limit, offset=offset)
                page_items = page.get("items", [])
                items.extend(page_items)
                print(
                    f"[fetch_spotify] job={job_id} fetched {len(items)}/{total} items (offset={offset})"
                )
                if not page.get("next"):
                    break
                offset += limit
                time.sleep(0.2)  # polite pause between pages

            # attach items and metadata
            meta["tracks"]["items"] = items
            meta["fetched_at"] = datetime.utcnow().isoformat()

            # write to file
            with out_path.open("w", encoding="utf-8") as f:
                json.dump(meta, f, ensure_ascii=False, indent=2)

            return {"path": str(out_path), "total_tracks": total, "fetched": len(items)}

        except SpotifyException as e:
            attempt += 1
            if attempt >= max_retries:
                print(f"[fetch_spotify] failed after {attempt} attempts: {e}")
                raise
            wait = 2**attempt
            print(
                f"[fetch_spotify] SpotifyException, retrying in {wait}s... ({attempt}/{max_retries})"
            )
            time.sleep(wait)
        except Exception as e:
            # unexpected; re-raise so caller can mark job FAILED
            print(f"[fetch_spotify] unexpected error: {e}")
            raise


def update_job_status(job_id: str, status: str, finished_at: datetime = None):
    conn = get_db_conn()
    try:
        cur = conn.cursor()
        if finished_at:
            cur.execute(
                "UPDATE playlist_conversion_job SET status = ?, finished_at = ? WHERE job_id = ?",
                (status, finished_at.isoformat(), job_id),
            )
        else:
            cur.execute(
                "UPDATE playlist_conversion_job SET status = ? WHERE job_id = ?",
                (status, job_id),
            )
        conn.commit()
    finally:
        conn.close()

if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python ingest_spotify.py <job_id>")
        sys.exit(1)
    job = sys.argv[1]
    try:
        update_job_status(job, "RUNNING")
        res = fetch_spotify_playlist_raw(job)
        update_job_status(job, "DONE", finished_at=datetime.utcnow())
        print("Saved:", res)
    except Exception as e:
        update_job_status(job, "FAILED", finished_at=datetime.utcnow())
        print("Job failed:", e)
        sys.exit(2)
