import logging
from pathlib import Path

from googleapiclient.discovery import build
import googleapiclient.discovery
import googleapiclient.errors
import pandas as pd
from youtube_transcript_api import YouTubeTranscriptApi


class YoutubeTranscriber:
    """
    A class for downloading YouTube video transcripts, storing the transcripts,
    and managing related channel and video metadata.

    Attributes:
        api_key (str): YouTube API key for accessing YouTube Data API.
        channel_handle (str): Handle of the YouTube channel to be processed.
        tables_dir (Path): Directory to store tabular data (e.g., CSV files).
        transcripts_dir (Path): Directory to store transcript files.
        logger (logging.Logger): Logger for tracking execution and errors.
        channel_id (str): Unique identifier of the YouTube channel, fetched via the API.

    Methods:
        __init__(api_key, channel_handle, tables_dir, transcripts_dir):
            Initializes the YoutubeTranscriber instance.
        _write_to_df(data, tag):
            Writes data to a Pandas DataFrame and saves it as a CSV file.
        _store_channel_info(title, n_subscribers, description):
            Stores channel metadata in DataFrame and CSV format.
        get_channel_info():
            Fetches channel metadata from the YouTube API.
        _get_video_info(video_id):
            Retrieves metadata for a specific video using the YouTube API.
        _calculate_engagement_rate(views, likes, comments):
            Calculates engagement rate for a video based on views, likes, and comments.
        _store_data():
            Processes and stores video metadata and transcripts in DataFrames.
        get_transcripts():
            Retrieves video transcripts from the YouTube channel's upload playlist.
    """
    def __init__(
            self, api_key: str,
            channel_handle: str,
            tables_dir: Path,
            transcripts_dir: Path,
    ) -> None:
        """
        Initialize the YoutubeTranscriber class.

        :param api_key: YouTube API key for accessing the YouTube Data API.
        :param channel_handle: YouTube channel handle (e.g. '@examplechannel').
        :param tables_dir: Directory to store tabular data (CSV files).
        :param transcripts_dir: Directory to store video transcript files.
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.api_key = api_key
        self.channel_handle = channel_handle
        self.tables_dir = tables_dir
        self.transcripts_dir = transcripts_dir
        self.channel_id = None

    def _write_to_df(self, data: list, tag: str) -> pd.DataFrame:
        """
        Writes a list of dictionaries to a Pandas DataFrame and saves it as a CSV file.

        :param data: List of dictionaries containing data to be stored.
        :param tag: Identifier for the output file name.
        :return: DataFrame created from the input data.
        """
        df = pd.DataFrame(data).replace("\n", " ", regex=True)
        if "published_at" in df.columns:
            df = df.sort_values(["published_at"], ascending=False).reset_index(drop=True)
        else:
            self.logger.warning("Warning: 'published_at' column is missing. DataFrame will not be sorted by this column.")
        df = df.drop_duplicates()
        df.to_csv(f"{self.tables_dir}/{self.channel_handle}_{tag}.csv", index=False, encoding="utf-8")
        return df

    def _store_channel_info(
            self,
            title: str,
            n_subscribers: int,
            description: str,
    ) -> pd.DataFrame:
        """
        Stores channel metadata in a Pandas DataFrame and a CSV file.

        :param title: Channel title.
        :param n_subscribers: Number of subscribers for the channel.
        :param description: Description of the channel.
        :return: DataFrame containing the stored channel metadata.
        """
        try:
            channel_data = {
                "channel_id": self.channel_id,
                "channel_handle": self.channel_handle,
                "channel_title": title,
                "channel_subscribers": n_subscribers,
                "channel_description": description
            }

            df = self._write_to_df([channel_data], "channel")

            self.logger.info(
                f"Finished storing channel info for @{self.channel_handle}."
            )
            return df

        except Exception as e:
            self.logger.error(f"Error storing info from channel {self.channel_handle}: {e}", exc_info=True)
            raise

    def get_channel_info(self) -> pd.DataFrame:
        """
        Fetches metadata for the channel associated with the provided handle.

        :return: DataFrame containing the channel metadata (title, subscribers, description, etc.).
        """
        try:
            youtube = build(
                "youtube",
                "v3",
                developerKey=self.api_key,
                cache_discovery=False
            )
            request = youtube.channels().list(
                part="snippet,statistics",
                forHandle=self.channel_handle
            )
            response = request.execute()

            if response.get("items"):
                # First item
                item = response.get("items", [{}])[0]

                # Snippet details
                snippet = item.get("snippet", {})
                self.channel_id = item.get("id", "Unknown Channel ID")
                channel_title = snippet.get("title", "Unknown Channel Title")
                channel_description = snippet.get("description", "")

                # Statistics details
                statistics = item.get("statistics", {})
                channel_n_subscribers = int(statistics.get("subscriberCount", "Unknown Subscriber Count"))

                self.logger.info(f"Finished retrieving channel info for @{self.channel_handle}.")

                return self._store_channel_info(
                    title=channel_title,
                    description=channel_description,
                    n_subscribers=channel_n_subscribers,
                )

            else:
                self.logger.warning(f"No channel found for the handle {self.channel_handle}.")

        except Exception as e:
            self.logger.error(f"Error retrieving YouTube channel information: {e}", exc_info=True)
            raise

    def _get_video_info(
            self,
            video_id: str
    ) -> tuple[str, str, str, int, int, int] | tuple[None, None, None, None, None, None]:
        """
        Retrieves metadata for a specific video using the YouTube API.

        :param video_id: Video ID for which to retrieve information.
        :return: Tuple containing video title, description, published date, views, likes, and comments.
        """
        try:
            youtube = build("youtube", "v3", developerKey=self.api_key)
            request = youtube.videos().list(
                part="snippet,statistics",
                id=video_id
            )
            response = request.execute()
            if response.get("items"):
                # First item
                item = response.get("items", [{}])[0]

                # Snippet details
                snippet = item.get("snippet", {})
                title = snippet.get("title", "Unknown Title")
                description = snippet.get("description", "No Description")
                published_at = snippet.get("publishedAt", "Unknown Publish Date")

                # Statistics details
                statistics = item.get("statistics", {})
                video_n_views = int(statistics.get("viewCount", 0))
                video_n_likes = int(statistics.get("likeCount", 0))
                video_n_comments = int(statistics.get("commentCount", 0))

                return title, description, published_at, video_n_views, video_n_likes, video_n_comments
            else:
                self.logger.warning(f"No video found for ID {video_id}. Returning None values.")
                return None, None, None, None, None, None
        except Exception as e:
            self.logger.error(f"Error retrieving info for channel {self.channel_handle}: {e}", exc_info=True)
            raise

    def _calculate_engagement_rate(
            self,
            views: int,
            likes: int,
            comments: int
    ) -> float | None:
        """
        Calculates engagement rate for a video based on views, likes, and comments.

        :param views: Number of views for the video.
        :param likes: Number of likes on the video.
        :param comments: Number of comments on the video.
        :return: Engagement rate as a percentage, or None if views are zero.
        """
        try:
            return ((likes + comments) / views) * 100
        except Exception as e:
            self.logger.error(f"Error calculating engagement rate: {e}", exc_info=True)

    def _store_data(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Processes and stores video metadata and transcripts in DataFrames.

        :return: Tuple of DataFrames containing video metadata and transcript data.
        """
        try:
            video_data = []
            transcripts_data = []

            for file in self.transcripts_dir.iterdir():
                if file.suffix == ".txt":
                    try:
                        with open(file, "r", encoding="utf-8") as f:
                            video_transcript = f.read()
                    except UnicodeDecodeError as e:
                        self.logger.error(f"Error reading file {file}: {e}", exc_info=True)
                        continue

                    # Get video info
                    filename = file.stem
                    title, description, published_at, video_n_views, video_n_likes, video_n_comments = self._get_video_info(
                        filename
                    )
                    video_engagement = self._calculate_engagement_rate(
                        views=video_n_views,
                        likes=video_n_likes,
                        comments=video_n_comments
                    )
                    # Append to data with consistent keys
                    video_data.append(
                        {
                            "video_id": filename,
                            "channel_id": self.channel_id,
                            "video_title": title,
                            "video_views": video_n_views,
                            "video_likes": video_n_likes,
                            "video_comments": video_n_comments,
                            "video_engagement": video_engagement,
                            "video_published_at": published_at,
                            "video_description": description,
                        }
                    )

                    transcripts_data.append(
                        {
                            "video_id": filename,
                            "channel_id": self.channel_id,
                            "video_transcript": video_transcript
                        }
                    )

            videos_df = self._write_to_df(video_data, "videos")
            transcripts_df = self._write_to_df(transcripts_data, "transcripts")
            return videos_df, transcripts_df

        except Exception as e:
            self.logger.error(f"Error processing data from channel {self.channel_handle}: {e}", exc_info=True)
            raise

    def get_transcripts(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Retrieves video transcripts from the YouTube channel's upload playlist.

        :return: Tuple of DataFrames containing video metadata and transcripts.
        """
        try:
            youtube = googleapiclient.discovery.build(
                "youtube",
                "v3",
                developerKey=self.api_key
            )

            # Get the ID of the channel's "Uploads" playlist
            request = youtube.channels().list(
                part="contentDetails",
                id=self.channel_id
            )

            response = request.execute()
            self.logger.info(f"Downloading transcripts for channel {self.channel_handle}.")

            uploads_playlist_id = (
                response.get("items", [{}])[0]
                .get("contentDetails", {})
                .get("relatedPlaylists", {})
                .get("uploads", "No Uploads Playlist")
            )

            # Get all video IDs from the "uploads" playlist and fetch transcripts
            video_ids = []
            next_page_token = None
            n_transcripts = 0

            while True:
                request = youtube.playlistItems().list(
                    part="contentDetails",
                    playlistId=uploads_playlist_id,
                    maxResults=50,
                    pageToken=next_page_token
                )

                response = request.execute()

                for item in response.get("items"):
                    video_id = item.get("contentDetails", {}).get("videoId", "No Video ID")
                    video_ids.append(video_id)

                    transcript_file = f"{self.transcripts_dir}/{video_id}.txt"
                    if not Path(transcript_file).is_file():
                        try:
                            transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)
                            for transcript in transcript_list:
                                transcript_data = transcript.fetch()
                                with open(transcript_file, "w", encoding="utf-8") as f:
                                    for line in transcript_data:
                                        f.write(line["text"] + "\n")
                                n_transcripts += 1
                                self.logger.info(f"Downloaded transcript #{n_transcripts} for video ID {video_id} to {transcript_file}.")
                        except Exception as e:
                            self.logger.warning(f"Could not download transcripts for video {video_id}: {e}")
                    else:
                        self.logger.info(f"Transcript file already exists: {transcript_file}")

                next_page_token = response.get("nextPageToken")
                if next_page_token is None:
                    break

            self.logger.info(
                f"Finished retrieving {n_transcripts} transcripts for channel @{self.channel_handle}."
            )
            return self._store_data()

        except Exception as e:
            self.logger.error(f"Error retrieving transcripts for channel {self.channel_handle}: {e}", exc_info=True)
            raise
