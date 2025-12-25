from dataclasses import dataclass
from typing import List, Optional
from datetime import datetime

@dataclass
class spotifyTrack:
    spotify_track_id: str
    track_name: str
    artist: List[str]
    album_name: Optional[str]
    duration_ms: int
    is_explicit: bool
    added_at: Optional[datetime]
    popularity: Optional[int]

@dataclass
class YouTubeCandidate:
    spotify_track_id: str
    video_id: str
    title: str
    channel_name: Optional[str]
    duration_seconds: Optional[int]
    view_count: Optional[int]
    ranking_in_search: Optional[int]
    time_of_upload: Optional[datetime]

@dataclass
class TrackMatch:
    spotify_Track_id: str
    youtube_video_id: str
    match_score: float
    title_similarity: float
    duration_difference: int
    is_version_match: bool
    matched_at: datetime
