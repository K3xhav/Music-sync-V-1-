import sqlite3
import uuid
from datetime import datetime

conn = sqlite3.connect("jobs.db")
cursor = conn.cursor()

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS playlist_conversion_job (
        job_id TEXT PRIMARY KEY,
        spotify_playlist_id TEXT,
        playlist_name TEXT,
        user_identifier TEXT,
        status TEXT,
        created_at TEXT,
        finished_at TEXT
    )
"""
)


def create_job(
    spotify_playlist_id: str, playlist_name: str, user_identifier: str
) -> str:
    job_id = str(uuid.uuid4())
    created_at = datetime.utcnow().isoformat()

    cursor.execute(
        """
        INSERT INTO playlist_conversion_job
        (job_id, spotify_playlist_id, playlist_name, user_identifier, status, created_at, finished_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """,
        (
            job_id,
            spotify_playlist_id,
            playlist_name,
            user_identifier,
            "PENDING",
            created_at,
            None,
        ),
    )

    conn.commit()
    return job_id


def get_job(job_id: str):
    cursor.execute("SELECT * FROM playlist_conversion_job WHERE job_id = ?", (job_id,))
    return cursor.fetchone()

if __name__ == "__main__":
    job_id = create_job("playlist123", "My Playlist", "test_user")
    print(get_job(job_id))
