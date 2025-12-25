import sqlite3
from pathlib import Path
from datetime import datetime

# ================= PATHS =================
BASE_DIR = Path(__file__).parent.parent

JOBS_DB = BASE_DIR / "data" / "jobs.db"
MAPPED_DB = BASE_DIR / "data" / "cleaned" / "mapped.db"

MAPPED_DB.parent.mkdir(parents=True, exist_ok=True)


# ================= CONNECTIONS =================
def connect_jobs_db():
    conn = sqlite3.connect(str(JOBS_DB))
    conn.row_factory = sqlite3.Row
    return conn


def connect_mapped_db():
    conn = sqlite3.connect(str(MAPPED_DB))
    conn.row_factory = sqlite3.Row
    return conn


# ================= GOLD TABLE =================
def recreate_gold_table():
    conn = connect_jobs_db()
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS youtube_tracks_gold")

    cursor.execute(
        """
        CREATE TABLE youtube_tracks_gold (
            spotify_track_id TEXT PRIMARY KEY,
            youtube_video_id TEXT,
            title TEXT,
            channel_name TEXT,
            time_of_upload TEXT,
            fetched_at TEXT
        )
        """
    )

    conn.commit()
    conn.close()
    print("Gold table recreated")


def insert_gold_data():
    conn = connect_jobs_db()
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO youtube_tracks_gold (
            spotify_track_id,
            youtube_video_id,
            title,
            channel_name,
            time_of_upload,
            fetched_at
        )
        SELECT
            spotify_track_id,
            youtube_video_id,
            title,
            channel_name,
            time_of_upload,
            fetched_at
        FROM youtube_tracks_silver
        WHERE ranking_in_search = 1
        """
    )

    conn.commit()
    conn.close()
    print("Gold data inserted")


# ================= MAPPING DB =================
def create_mapping_table():
    conn = connect_mapped_db()
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS spotify_youtube_mapping (
            spotify_track_id TEXT PRIMARY KEY,
            youtube_video_id TEXT,
            created_at TEXT
        )
        """
    )

    conn.commit()
    conn.close()
    print("Mapping table ready")


def insert_mapping_data():
    jobs_conn = connect_jobs_db()
    jobs_cursor = jobs_conn.cursor()

    mapped_conn = connect_mapped_db()
    mapped_cursor = mapped_conn.cursor()

    jobs_cursor.execute(
        """
        SELECT spotify_track_id, youtube_video_id
        FROM youtube_tracks_gold
        """
    )

    rows = jobs_cursor.fetchall()
    now = datetime.utcnow().isoformat()

    for row in rows:
        mapped_cursor.execute(
            """
            INSERT OR IGNORE INTO spotify_youtube_mapping (
                spotify_track_id,
                youtube_video_id,
                created_at
            ) VALUES (?, ?, ?)
            """,
            (
                row["spotify_track_id"],
                row["youtube_video_id"],
                now,
            ),
        )

    mapped_conn.commit()
    jobs_conn.close()
    mapped_conn.close()
    print(" Mapping data inserted (append-only)")


# ================= MAIN =================
if __name__ == "__main__":
    recreate_gold_table()
    insert_gold_data()
    create_mapping_table()
    insert_mapping_data()

    print("\nGOLD + MAPPING PIPELINE COMPLETE")
