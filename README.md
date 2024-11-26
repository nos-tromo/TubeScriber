# TubeScriber
TubeScriber is a tool to download and store information from YouTube channels.

### Prerequisites
- [`uv`](https://github.com/astral-sh/uv) for Python version and dependency management
- YouTube Data API v3 key will be required upon first startup

### Installation
```bash
git clone https://github.com/nos-tromo/TubeScriber.git
cd TubeScriber
uv sync
```

### Usage
Run `main.py` to retrieve data from multiple channels:
```bash
uv run python main.py [CHANNEL_HANDLE_1] [CHANNEL_HANDLE_2] ... [CHANNEL_HANDLE_N]
```
The script will download all transcripts available from the channel via accessing the *Uploads* playlist
as well as other information available via the YouTube Data API v3. A channel's handle can be located in the URL: 
youtube.com/@handle. The API has a limit of 10,000 GET requests per day.

### Output
TubeScriber creates:
- txt files for every transcript
- csv files for channel info, videos and transcripts 
- all results combined are stored in an SQLite database at `output/tubescriber.db`

### Logfiles
If you face errors or unusual behavior, see `.logs` for further debugging.

### Feedback
I hope you find this application to be a valuable tool. If you have any feedback or suggestions on how to improve
TubeScriber, please let me know. I'm always looking for ways to improve this tool to better serve the community.
