from datetime import datetime
import logging
from pathlib import Path

logfile_name = f"tubescriber_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logs_dir = Path(".logs")
logs_dir.mkdir(parents=True, exist_ok=True)
logfile_path = logs_dir / logfile_name

file_handler = logging.FileHandler(logfile_path)
file_handler.setLevel(logging.ERROR)  # Only log errors to the logfile
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)  # Display info-level and above on CLI

logging.basicConfig(
    level=logging.DEBUG,  # Set root logger level to capture all messages
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[file_handler, stream_handler]
)

logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.ERROR)


import os
import sys

from dotenv import find_dotenv, load_dotenv, set_key
import pandas as pd

from modules.database import initialize_database, upsert_data
from modules.scraper import YoutubeTranscriber


def _get_api_key(api_key_name: str = "API_KEY") -> str:
    """
    Retrieve the API key from the environment or prompt the user to enter it.

    :param api_key_name: The API key name to locate the token within .env.
    :return: The API key.
    """
    dotenv_path = find_dotenv()
    load_dotenv(dotenv_path)
    api_key_value = os.getenv(api_key_name)
    if not api_key_value:
        api_key_value = input("Token not found. Please enter your API key: ")
        if dotenv_path:
            set_key(dotenv_path, api_key_name, api_key_value)
        else:
            with open(".env", "w") as env_file:
                env_file.write(f"{api_key_name}={api_key_value}\n")
    return api_key_value


def _create_directories(channel_handle: str) -> tuple[Path, Path, Path]:
    """
    Create directories for the given channel.

    :param channel_handle: YouTube channel handle.
    :return: A tuple containing the tables, transcripts, and database directory.
    """
    output_dir = Path("output") / channel_handle
    tables_dir = output_dir / "tables"
    transcripts_dir = output_dir / "transcripts"
    db_dir = Path("output") / "tubescriber.db"

    output_dir.mkdir(parents=True, exist_ok=True)
    tables_dir.mkdir(parents=True, exist_ok=True)
    transcripts_dir.mkdir(parents=True, exist_ok=True)

    return tables_dir, transcripts_dir, db_dir


def _get_yt_data(
        api_key: str,
        channel_handle: str,
        table_dir: Path,
        transcripts_dir: Path,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Retrieve YouTube data for the given channel handle, and API key.

    :param api_key: API key.
    :param channel_handle: YouTube channel handle.
    :param table_dir: Result tables directory.
    :param transcripts_dir: Data directory.
    :return: A tuple of DataFrames containing information about the channel, the videos, and the transcripts.
    """
    youtube_scraper = YoutubeTranscriber(
        api_key=api_key,
        channel_handle=channel_handle,
        tables_dir=table_dir,
        transcripts_dir=transcripts_dir
    )

    channel_df = youtube_scraper.get_channel_info()
    videos_df, transcripts_df = youtube_scraper.get_transcripts()

    return channel_df, videos_df, transcripts_df


def database_setup(
        db_dir: Path,
        channel_df: pd.DataFrame = None,
        videos_df: pd.DataFrame = None,
        transcripts_df: pd.DataFrame = None
) -> None:
    """
    Populates a SQLite database with channel, video, and transcript data. Existing records are updated on conflicts
    using an upsert strategy. The database is saved to "output/tubescriber.db".

    :param db_dir: Path to the SQLite database directory.
    :param channel_df: DataFrame containing details about the channel.
    :param videos_df: DataFrame containing details about the channel's videos.
    :param transcripts_df: DataFrame containing details about the channel videos' transcripts.
    """
    db_conn = initialize_database(db_dir)
    upsert_data(
        db_conn=db_conn,
        channel_df=channel_df,
        videos_df=videos_df,
        transcripts_df=transcripts_df
    )


def main() -> None:
    """
    Main entry point for the script.
    """
    try:
        channels = sys.argv[1:] if len(sys.argv) > 1 else input("Enter YouTube channel handles (comma-separated): ").split(",")
        channels = [channel.strip() for channel in channels]
        for channel in channels:
            logging.info(f"Processing channel: {channel}")

            api_key = _get_api_key()
            tables_dir, transcripts_dir, db_dir = _create_directories(channel)

            channel_df, videos_df, transcripts_df = _get_yt_data(
                api_key=api_key,
                channel_handle=channel,
                table_dir=tables_dir,
                transcripts_dir=transcripts_dir,
            )

            database_setup(
                db_dir=db_dir,
                channel_df=channel_df,
                videos_df=videos_df,
                transcripts_df=transcripts_df
            )

            logging.info(f"Finished processing channel: {channel}")

    except Exception as e:
        logging.exception(f"An unexpected error occurred: {e}")
        raise


if __name__ == "__main__":
    main()
