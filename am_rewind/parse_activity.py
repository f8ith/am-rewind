import argparse
import asyncio
import datetime
import warnings

import aiohttp
import pandas as pd

from am_rewind.get_album import CACHE_HIT, get_artist_from_album
from am_rewind.utils import load_cache
from am_rewind.throttledclientsession import ThrottledClientSession

CSV_FILE_NAME = "Apple Music Play Activity.csv"
CONTAINER_FILE_NAME = "Apple Music - Container Details.csv"
ALBUM_COLUMN = "Album Name"
PLAY_DURATION_COLUMN = "Play Duration Milliseconds"
SONG_COLUMN = "Song Name"
DATE_COLUMN = "Event End Timestamp"

ALL_COLUMNS = [v for k, v in list(locals().items()) if k.endswith("_COLUMN")]

# Hyper-parameters
INSUFFICIENT_DURATION_MILLIS = 15000
MAX_DURATION_MILLIS = (
    1800000  # clip durations longer than 30 mins (customize to liking)
)
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"
START_DATE = datetime.datetime(2016, 1, 1, 0, 0, 0).astimezone(datetime.timezone.utc)
RATE_LIMIT = 0.3  # equivalent to 18 req/minute

ARTIST_COLUMN = "Artist"

# Cache stats
cache_hits = 0
cache_misses = 0


async def get_artist(session: aiohttp.ClientSession, cache: dict, album: str) -> str:
    """
    Returns the artist for a given album, based on a partial match in CONTAINER_FILE_NAME
    """
    global cache_hits, cache_misses

    df = pd.read_csv(CONTAINER_FILE_NAME)
    df = df.dropna(how="any", subset=["Container Description"])
    df = df[df["Container Description"].str.contains(album, regex=False)]

    if len(df) == 0:
        artist, cache_status = await get_artist_from_album(session, cache, album)

        if cache_status == CACHE_HIT:
            cache_hits += 1
        else:
            cache_misses += 1

        return pprint_artists(artist)

    return pprint_artists(tuple(df["Artists"].values[0].split(", ")))


def pprint_artists(artists: tuple) -> str:
    """
    Pretty prints a tuple of artists
    """
    if len(artists) == 0:
        return "Unknown"

    if len(artists) > 3:
        return ", ".join(artists[:3]) + ", ..."

    return ", ".join(artists)


async def parse_activity(args):
    """
    Parses Apple Music Play Activity.csv
    """
    warnings.filterwarnings("ignore")

    df = pd.read_csv(CSV_FILE_NAME)

    # Filter columns of interest
    df = df[ALL_COLUMNS]

    # Remove columns where there is no data in any of the columns
    df = df.dropna(how="any")

    # Remove rows where the date is before the start date
    df[DATE_COLUMN] = pd.to_datetime(df[DATE_COLUMN], format="ISO8601")
    df = df[df[DATE_COLUMN] > START_DATE]

    # Remove rows where the play duration is less than 10 seconds
    df = df[df[PLAY_DURATION_COLUMN] >= INSUFFICIENT_DURATION_MILLIS]

    df[PLAY_DURATION_COLUMN] = df[PLAY_DURATION_COLUMN].clip(upper=MAX_DURATION_MILLIS)

    cache = load_cache()

    # Add in the artist column
    async with ThrottledClientSession(
        rate_limit=RATE_LIMIT,
        filters=["http://ws.audioscrobbler.com/2.0"],
        limit_filtered=False,
    ) as session:
        df[ARTIST_COLUMN] = await asyncio.gather(
            *(get_artist(session, cache, v) for v in df[ALBUM_COLUMN])
        )

    print("Songs played: ", len(df))
    print("Last date: ", df[DATE_COLUMN].max().strftime("%Y-%m-%d"))

    # Get total play time
    total_play_time = df[PLAY_DURATION_COLUMN].sum()
    print("Total play time (min): ", round(total_play_time / (1000 * 60), 1))
    print()

    # Find the top 5 artists by play time
    artist_play_time = df.groupby(ARTIST_COLUMN)[PLAY_DURATION_COLUMN].sum()
    artist_play_time = artist_play_time.sort_values(ascending=False)
    top_artists_df = pd.DataFrame(
        {
            "Artist": artist_play_time.index,
            "Play Time (min)": (artist_play_time.values / (1000 * 60)).round(1),
            "Percentage": (artist_play_time.values / total_play_time * 100).round(1),
        }
    ).head(5)
    print("Top 5 artists by play time:")
    top_artists_df.index += 1
    print(top_artists_df)
    print()

    # Find the top 5 albums by play time
    album_play_time = df.groupby([ARTIST_COLUMN, ALBUM_COLUMN])[
        PLAY_DURATION_COLUMN
    ].sum()
    album_play_time = album_play_time.sort_values(ascending=False)
    top_albums_df = pd.DataFrame(
        {
            "Artist": album_play_time.index.get_level_values(ARTIST_COLUMN),
            "Album": album_play_time.index.get_level_values(ALBUM_COLUMN),
            "Play Time (h)": (album_play_time.values / (1000 * 60 * 60)).round(1),
        }
    ).head(5)
    print("Top 5 albums by play time:")
    top_albums_df.index += 1
    print(top_albums_df)
    print()

    # Find the top 10 songs by play time, and show album name
    song_play_time = df.groupby([ARTIST_COLUMN, ALBUM_COLUMN, SONG_COLUMN])[
        PLAY_DURATION_COLUMN
    ].sum()
    song_play_time = song_play_time.sort_values(ascending=False)
    top_songs_df = pd.DataFrame(
        {
            "Artist": song_play_time.index.get_level_values(ARTIST_COLUMN),
            "Album": song_play_time.index.get_level_values(ALBUM_COLUMN),
            "Song": song_play_time.index.get_level_values(SONG_COLUMN),
            "Play Time (min)": (song_play_time.values / (1000 * 60)).round(1),
        }
    ).head(10)
    print("Top 10 songs by play time:")
    top_songs_df.index += 1
    print(top_songs_df)

    if args.debug:
        print()
        print("Cache hit%:", round(cache_hits / (cache_hits + cache_misses) * 100, 1))

    df.to_csv(
        f"{datetime.datetime.now().strftime('%Y-%m-%d-%H%M%S')}-activity.csv",
        index=True,
    )
