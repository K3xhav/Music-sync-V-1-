"""
clean_spotify.py - Spotify Silver Layer Transformer
Transforms raw JSON into clean database tables.
"""

import sqlite3
import json
from datetime import datetime
from pathlib import Path

# ========== PATHS ==========
BASE_DIR = Path(__file__).parent.parent  # Goes up from scripts to new-pipline
JOBS_DB = BASE_DIR / "data" / "jobs.db"
RAW_DIR = BASE_DIR / "data" / "raw" / "spotify"
JOBS_DB.parent.mkdir(parents=True, exist_ok=True)


# ========== DATABASE CONNECTION ==========
def get_db_connection():
    """Connect to the jobs database."""

    conn = sqlite3.connect(str(JOBS_DB))
    conn.row_factory = sqlite3.Row
    return conn

# ========== TABLE CREATION ==========
def create_silver_data_table():
    """Create the spotify_tracks_silver table if it doesn't exist."""
    conn = get_db_connection()
    cursor = conn.cursor()

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS spotify_tracks_silver (
        spotify_track_id TEXT NOT NULL,
        track_name TEXT NOT NULL,
        artist TEXT NOT NULL,
        album_name TEXT,
        duration_ms INTEGER,
        is_explicit INTEGER,
        added_at INTEGER,
        popularity INTEGER
    )
    """

    cursor.execute(create_table_sql)
    conn.commit()
    conn.close()
    print("Created Silver_data table (if it didn't exist)")

# ========== DATA EXTRACTION ==========
def extract_and_insert_silver_data(job_id):
    """Extract data from raw JSON and insert into Silver_data table."""
    conn = get_db_connection()
    cursor = conn.cursor()

    # 1. Find the raw JSON file
    raw_file_path = RAW_DIR / f"{job_id}.json"

    if not raw_file_path.exists():
        print(f"Raw file not found: {raw_file_path}")
        return False

    # 2. Read and parse the JSON
    with open(raw_file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # 3. Extract tracks from the structure
    track_items = data.get('tracks', {}).get('items', [])

    print(f"Found {len(track_items)} tracks to process")

    # 4. For each track, extract and insert
    inserted_count = 0

    for item in track_items:
        track = item.get('track', {})

        # Extract fields according to YOUR schema
        spotify_track_id = track.get('id', '')
        track_name = track.get('name', '')

        # ARTIST: Get first artist only
        artists = track.get('artists', [])
        if artists:
            artist = artists[0].get('name', '')  # First artist only
        else:
            artist = ''

        album_name = track.get('album', {}).get('name', '')
        duration_ms = track.get('duration_ms', 0)

        # Convert explicit boolean to 0/1
        is_explicit = 1 if track.get('explicit', False) else 0

        # Convert added_at ISO string to timestamp (INTEGER)
        added_at_str = item.get('added_at', '')
        if added_at_str:
            # Convert ISO string to Unix timestamp
            dt = datetime.fromisoformat(added_at_str.replace('Z', '+00:00'))
            added_at = int(dt.timestamp())
        else:
            added_at = 0

        popularity = track.get('popularity', 0)  # Note: matches YOUR typo "popularity"

        # 5. Insert into YOUR spotify_tracks_silver table
        insert_sql = """
        INSERT INTO spotify_tracks_silver
        (spotify_track_id, track_name, artist, album_name,
         duration_ms, is_explicit, added_at, popularity)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """

        cursor.execute(insert_sql, (
            spotify_track_id, track_name, artist, album_name,
            duration_ms, is_explicit, added_at, popularity
        ))

        inserted_count += 1

    # 6. Commit and close
    conn.commit()
    conn.close()

    print(f"Inserted {inserted_count} tracks into spotify_tracks_silver table")
    return True

# ========== MAIN FUNCTION ==========
def main():
    """Main function to run the cleaning process."""
    import sys

    if len(sys.argv) > 1:
        job_id = sys.argv[1]
        print(f"Starting silver layer processing for job: {job_id}")

        # 1. Create table
        create_silver_data_table()

        # 2. Extract and insert data
        success = extract_and_insert_silver_data(job_id)

        if success:
            print("Silver layer processing completed!")
        else:
            print("Silver layer processing failed")
    else:
        print("Usage: python clean_spotify.py <job_id>")
        print("Example: python clean_spotify.py 96ce763a-ab3f-4358-9c4e-90bc2b7c10cf")

if __name__ == "__main__":
    main()
