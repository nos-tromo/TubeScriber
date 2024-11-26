import logging
from pathlib import Path
import sqlite3

import pandas as pd


def initialize_database(db_path: Path) -> sqlite3.Connection:
    """
    Initialize the SQLite database and create necessary tables.

    :param db_path: Path to the database file.
    :return: sqlite3.Connection: Database connection object.
    """
    logger = logging.getLogger(__name__)

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Create Channel table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS channel (
            channel_id TEXT PRIMARY KEY,
            channel_handle TEXT NOT NULL,
            channel_title TEXT NOT NULL,
            channel_subscribers INTEGER,
            channel_description TEXT
        )
        """)

        # Create Video table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS video (
            video_id TEXT PRIMARY KEY,
            channel_id TEXT NOT NULL,
            video_title TEXT NOT NULL,
            video_views INTEGER,
            video_likes INTEGER,
            video_comments INTEGER,
            video_engagement REAL,
            video_published_at TEXT,
            video_description TEXT,
            FOREIGN KEY (channel_id) REFERENCES channel(channel_id) ON DELETE CASCADE
        )
        """)

        # Create Transcript table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS transcript (
            video_id TEXT PRIMARY KEY,
            channel_id TEXT NOT NULL,
            video_transcript TEXT NOT NULL,
            FOREIGN KEY (video_id) REFERENCES video(video_id) ON DELETE CASCADE
        )
        """)

        conn.commit()
        return conn

    except sqlite3.OperationalError as e:
        logger.error(f"Error while connecting to database: {e}")


def upsert_data(
        db_conn: sqlite3.Connection,
        channel_df: pd.DataFrame = None,
        videos_df: pd.DataFrame = None,
        transcripts_df: pd.DataFrame = None
) -> None:
    """
    Inserts or updates data in the SQLite database for channels, videos, and transcripts.

    This function performs an "upsert" operation, which inserts new records or updates
    existing ones in the database if a conflict occurs on the primary key.

    :param db_conn: SQLite database connection object.
    :param channel_df: DataFrame containing channel data with columns:
        - "channel_id": Unique identifier for the channel.
        - "channel_handle": Handle or username of the channel.
        - "channel_title": Title or name of the channel.
        - "channel_subscribers": Number of subscribers the channel has.
        - "channel_description": Description of the channel.
    :param videos_df: DataFrame containing video data with columns:
        - "video_id": Unique identifier for the video.
        - "channel_id": Identifier for the channel associated with the video.
        - "video_title": Title of the video.
        - "video_views": Number of views the video has.
        - "video_likes": Number of likes on the video.
        - "video_comments": Number of comments on the video.
        - "video_engagement": Aggregate engagement metric for the video.
        - "video_published_at": Publication timestamp of the video.
        - "video_description": Description of the video.
    :param transcripts_df: DataFrame containing transcript data with columns:
        - "video_id": Unique identifier for the video.
        - "channel_id": Identifier for the channel associated with the video.
        - "video_transcript": Transcript text of the video.

    :return: None
    """
    logger = logging.getLogger(__name__)
    cursor = None

    try:
        cursor = db_conn.cursor()

        # Upsert for channels
        if channel_df is not None and not channel_df.empty:
            for _, row in channel_df.iterrows():
                cursor.execute("""
                INSERT INTO channel (channel_id, channel_handle, channel_title, channel_subscribers, channel_description)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(channel_id) DO UPDATE SET
                    channel_handle = excluded.channel_handle,
                    channel_title = excluded.channel_title,
                    channel_subscribers = excluded.channel_subscribers,
                    channel_description = excluded.channel_description;
                """, (row["channel_id"], row["channel_handle"], row["channel_title"],
                      row["channel_subscribers"], row["channel_description"]))

        # Upsert for videos
        if videos_df is not None and not videos_df.empty:
            for _, row in videos_df.iterrows():
                cursor.execute("""
                INSERT INTO video (video_id, channel_id, video_title, video_views, video_likes, video_comments, video_engagement, video_published_at, video_description)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(video_id) DO UPDATE SET
                    video_title = excluded.video_title,
                    video_views = excluded.video_views,
                    video_likes = excluded.video_likes,
                    video_comments = excluded.video_comments,
                    video_engagement = excluded.video_engagement,
                    video_published_at = excluded.video_published_at,
                    video_description = excluded.video_description;
                """, (row["video_id"], row["channel_id"], row["video_title"],
                      row["video_views"], row["video_likes"], row["video_comments"],
                      row["video_engagement"], row["video_published_at"], row["video_description"]))

        # Upsert for transcripts
        if transcripts_df is not None and not transcripts_df.empty:
            for _, row in transcripts_df.iterrows():
                cursor.execute("""
                INSERT INTO transcript (video_id, channel_id, video_transcript)
                VALUES (?, ?, ?)
                ON CONFLICT(video_id) DO UPDATE SET
                    video_transcript = excluded.video_transcript;
                """, (row["video_id"], row["channel_id"], row["video_transcript"]))

        db_conn.commit()
        logger.info("Successfully inserted data into database.")

    except sqlite3.OperationalError as e:
        logger.error(f"Error while upserting to database: {e}")

    finally:
        if cursor:
            cursor.close()
        db_conn.close()
