import argparse
import datetime
from pathlib import Path
import requests
import os
import sys

import dateutil.parser
import pandas as pd

from am_rewind.utils import load_env
from am_rewind.parse_activity import (
    DATE_COLUMN,
    ARTIST_COLUMN,
    SONG_COLUMN,
    ALBUM_COLUMN,
)

load_env()

# Set DEBUG to True to test local dev server.
# API keys for local dev server and the real server are different.
DEBUG = False
ROOT = "http://localhost:8100" if DEBUG else "https://api.listenbrainz.org"


def submit_listen(listen_type, payload, token):
    """Submits listens for the track(s) in payload.

    Args:
        listen_type (str): either of 'single', 'import' or 'playing_now'
        payload: A list of Track dictionaries.
        token: the auth token of the user you're submitting listens for

    Returns:
         The json response if there's an OK status.

    Raises:
         An HTTPError if there's a failure.
         A ValueError is the JSON in the response is invalid.
    """

    response = requests.post(
        url="{0}/1/submit-listens".format(ROOT),
        json={
            "listen_type": listen_type,
            "payload": payload,
        },
        headers={"Authorization": "Token {0}".format(token)},
    )

    response.raise_for_status()

    return response.json()


def submit_payload(args):
    # Parse arguments

    if not args.csv_file:
        print("invalid file path")
        sys.exit(1)

    # Input token from the user and call submit listen
    token = os.environ.get("LISTENBRAINZ_TOKEN", None)
    if not token:
        token = input("Please enter your auth token: ")

    df = pd.read_csv(args.csv_file)
    all_payload = [
        {
            "listened_at": dateutil.parser.parse(date).timestamp(),
            "track_metadata": {
                "artist_name": artist,
                "track_name": song,
                "release_name": album,
            },
        }
        for date, artist, song, album in zip(
            df[DATE_COLUMN], df[ARTIST_COLUMN], df[SONG_COLUMN], df[ALBUM_COLUMN]
        )
    ]

    num_chunks = len(df) // 500 + 1

    for i in range(num_chunks):
        payload = all_payload[i * 500 : (i + 1) * 500]
        if not args.pretend:
            json_response = submit_listen(
                listen_type="import", payload=payload, token=token
            )
            print(f"chunk {i}: response was: {json_response}")
        else:
            print(f"submitted: {payload}")
