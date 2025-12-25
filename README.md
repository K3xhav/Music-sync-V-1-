

# Spotify → YouTube Music Data Engineering Pipeline

An end-to-end **data engineering pipeline** that synchronizes Spotify playlists to YouTube Music by ingesting, transforming, matching, and publishing music data across platforms.

This project is built with a **medallion architecture (Bronze → Silver → Gold)** and is designed to evolve from a script-driven V1 into a fully orchestrated **Airflow-based V2**.

---

## Project Overview

Music platforms do not provide direct interoperability. This pipeline bridges that gap by:

1. Extracting playlist data from Spotify
2. Matching Spotify tracks to YouTube Music videos
3. Persisting clean, structured datasets
4. Automatically creating a YouTube Music playlist with matched tracks

The focus of this project is **data engineering**, not UI or monetization.

---

## Architecture (V1)

```
Spotify API
   ↓
Bronze (raw JSON)
   ↓
Silver (clean tables)
   ↓
YouTube Search (ScrapeTube) / YouTube Searching (youtube V3 api)
   ↓
Silver (YouTube Candidates)
   ↓
Gold Layer (1 : 1 Track Mapping)
   ↓
YouTube Music Playlist Creation
```

### Key Design Choices

* **Medallion architecture** for data quality and traceability
* **Deterministic ID mapping** (spotify_track_id ↔ youtube_video_id)
* **Batch-safe ingestion** to respect external platform limits
* **Stateless scripts**, state stored in database

---

## Tech Stack

### Data Engineering

* Python
* SQLite (V1 metadata store)
* Structured JSON (Bronze)
* Relational tables (Silver / Gold)

### APIs & Libraries

* Spotify Web API (Spotipy)
* YouTube scraping (ScrapeTube / PyTube – V1)
* YouTube Music API (ytmusicapi)

---

## Project Structure

```
music-sync/
│
├── data/
│   ├── raw/
│   │   ├── spotify/
│   │   └── youtube/
│   ├── cleaned/
│   └── scraptetube/
│
├── scripts/
│   ├── ingest_spotify.py
│   ├── clean_spotify.py
│   ├── ingest_youtube.py
|   ├── ingest_youtube_scraptube.py
│   ├── clean_youtube.py
│   ├── create_ytmusic_playlist.py
│   ├── job.py
|   ├── playlist_tracks_gold.py
│
├── browser.json        # YT Music auth (not committed)
├── .env.example
├── README.md
├── test_ytmusic.py
```

---

## Data Model

### Spotify (Silver)

| Field            | Description              |
| ---------------- | ------------------------ |
| spotify_track_id | Spotify unique ID        |
| track_name       | Song title               |
| artist           | Primary artist           |
| album_name       | Album name               |
| duration_ms      | Track duration           |
| is_explicit      | Explicit flag            |
| added_at         | Playlist add time        |
| popularity       | Spotify popularity score |

### YouTube (Silver)

| Field             | Description            |
| ----------------- | ---------------------- |
| spotify_track_id  | Foreign key            |
| youtube_video_id  | Selected YouTube video |
| title             | Video title            |
| channel_name      | Channel                |
| ranking_in_search | Search rank            |
| time_of_upload    | Upload timestamp       |

### Gold Mapping

| spotify_track_id | youtube_video_id |
| ---------------- | ---------------- |

---

## Pipeline Flow (V1)

1. **Create Job**

   * Register playlist metadata in `playlist_conversion_job`

2. **Spotify Ingestion**

   * Fetch playlist → raw JSON
   * Clean → `spotify_tracks_silver`

3. **YouTube Ingestion**

   * Search per track
   * Store raw candidates
   * Filter → `youtube_tracks_silver`

4. **Gold Mapping**

   * Enforce 1 video per Spotify track

5. **Playlist Creation**

   * Create YouTube Music playlist
   * Add tracks in safe batches

---

## Known Limitations (Intentional)

This is **V1**, focused on correctness and data flow.

* YouTube Music enforces **rate limits**
* Large playlists (400+ tracks) are added in batches
* Temporary throttling may slow ingestion
* SQLite used for simplicity (Postgres planned in V2)

These constraints are **handled, documented, and planned for**, not ignored.

---
