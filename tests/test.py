from pathlib import Path

import pandas as pd

from main import database_setup


# Database path
db_dir = Path("output") / "tubescriber_test.db"
db_dir.parent.mkdir(parents=True, exist_ok=True)

# Mock DataFrames
channel_df = pd.DataFrame(
    [
        {
            "channel_id": "123",
            "channel_handle": "@example",
            "channel_title": "Example Channel",
            "channel_subscribers": 1000,
            "channel_description": "A great channel"
         }
    ]
)

video_df = pd.DataFrame(
    [
        {
            "video_id": "v1",
            "channel_id": "123",
            "video_title": "First Video",
            "video_views": 100,
            "video_likes": 10,
            "video_comments": 5,
            "video_engagement": 15.0,
            "video_published_at": "2023-11-01",
            "video_description": "An amazing video"
         }
    ]
)

transcript_df = pd.DataFrame(
    [
        {
            "video_id": "v1",
            "channel_id": "123",
            "video_transcript": "Hello world!"
         }
    ]
)

# Call function from `main.py`
database_setup(
    db_dir=db_dir,
    channel_df=channel_df,
    videos_df=video_df,
    transcripts_df=transcript_df
)
