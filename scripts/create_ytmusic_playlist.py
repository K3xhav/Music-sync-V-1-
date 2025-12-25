from ytmusicapi import YTMusic
import sqlite3
from pathlib import Path
from datetime import datetime
import time

# ================= PATHS =================
BASE_DIR = Path(__file__).parent.parent
JOBS_DB = BASE_DIR / "data" / "jobs.db"
BROWSER_AUTH = BASE_DIR / "browser.json"

# ================= CONFIG =================
BATCH_SIZE = 100
SLEEP_BETWEEN_BATCHES = 10 # seconds


# ================= DB =================
def get_db_connection():
    conn = sqlite3.connect(str(JOBS_DB))
    conn.row_factory = sqlite3.Row
    return conn


def fetch_latest_job_metadata():
    """
    Source of truth for which playlist is being synced.
    """
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT spotify_playlist_id, playlist_name
        FROM playlist_conversion_job
        ORDER BY created_at DESC
        LIMIT 1
    """
    )

    row = cur.fetchone()
    conn.close()

    if row is None:
        raise Exception("No playlist_conversion_job found")

    return row["spotify_playlist_id"], row["playlist_name"]


def fetch_youtube_video_ids():
    """
    youtube_tracks_silver already guarantees:
    - 1 row = 1 Spotify track
    - 1 YouTube video per track
    """
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT youtube_video_id
        FROM youtube_tracks_silver
        WHERE youtube_video_id IS NOT NULL
    """
    )

    rows = cur.fetchall()
    conn.close()

    video_ids = [row["youtube_video_id"] for row in rows]

    if not video_ids:
        raise Exception("No YouTube videos found in youtube_tracks_silver")

    return video_ids


# ================= MAIN =================
def create_ytmusic_playlist():
    ytmusic = YTMusic(str(BROWSER_AUTH))

    # 1. Job metadata
    spotify_playlist_id, playlist_name = fetch_latest_job_metadata()

    description = (
        "This playlist was automatically created from a Spotify playlist.\n\n"
        f"Spotify Playlist ID: {spotify_playlist_id}\n"
        f"Synced at: {datetime.utcnow().isoformat()} UTC\n\n"
        "Generated via a custom data engineering pipeline."
    )

    print(f"Creating YouTube Music playlist: {playlist_name}")

    # 2. Create playlist
    yt_playlist_id = ytmusic.create_playlist(
        title=playlist_name, description=description, privacy_status="PRIVATE"
    )

    print(f"Playlist created: {yt_playlist_id}")

    # 3. Fetch mapped videos
    video_ids = fetch_youtube_video_ids()
    total = len(video_ids)
    print(f"Total tracks to add: {total}")

    # 4. Add in batches
    for i in range(0, total, BATCH_SIZE):
        batch = video_ids[i : i + BATCH_SIZE]
        print(f"Adding tracks {i + 1} → {i + len(batch)}")

        ytmusic.add_playlist_items(playlistId=yt_playlist_id, videoIds=batch)

        time.sleep(SLEEP_BETWEEN_BATCHES)

    print("Playlist creation completed successfully")


# ================= ENTRY =================
if __name__ == "__main__":
    create_ytmusic_playlist()
