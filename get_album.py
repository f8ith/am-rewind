# We need a dedicated module to guessing the album, because Apple can't fucking give it to us. The best
# way to do this would be through an API, but that's not factored in yet; this is pretty brute-force right now.
import io
import json
import os
import pickle
import sys
from typing import List

import aiohttp

from utils import load_env

load_env()

API_KEY = os.environ.get("LASTFM_API_KEY", "")
CACHE_FILE_NAME = ".get_album_cache"
CACHE_HIT = 0
CACHE_MISS = 1
RETRY_COUNT = 3

cache = {}
if os.path.exists(CACHE_FILE_NAME):
    with open(CACHE_FILE_NAME, "rb") as f:
        cache = pickle.load(f)


async def parse_unknown_album(
    s: aiohttp.ClientSession, album: str, retry_count: int = 0
) -> List:
    if retry_count > RETRY_COUNT:
        print("retry count exceeded")
        sys.exit(1)

    if album in cache:
        try:
            return [
                (cache[album],),
                CACHE_HIT,
            ]
        except:
            ...

    async with s.get(
        "http://ws.audioscrobbler.com/2.0",
        params={
            "method": "album.search",
            "album": album,
            "api_key": API_KEY,
            "format": "json",
        },
    ) as response:
        try:
            data = await response.json()

            try:
                cache[album] = data["results"]["albummatches"]["album"][0]["artist"]

                # Write back to cache
                write_cache()

                return [
                    (data["results"]["albummatches"]["album"][0]["artist"],),
                    CACHE_MISS,
                ]
            except IndexError as e:
                print(f"lastfm: no matches found for {album}")
            except KeyError as e:
                # We've failed. Sigh.
                print(e.with_traceback(None), file=sys.stderr)
        except json.JSONDecodeError:
            print(f"lastfm: when requesting {album}, expected json got {response.text}")

    print("falling back to itunes")
    return [await itunes_fetch(s, album), CACHE_MISS]


async def itunes_fetch(s: aiohttp.ClientSession, album: str):
    async with s.get(
        "https://itunes.apple.com/search",
        params={
            "term": album,
            "music": "music",
            "country": "HK",
            "lang": "en_us" if album.isascii() else "ja_jp",
            "entity": "album",
        },
    ) as response:
        buffer = io.BytesIO(await response.read())
        try:
            data = json.load(buffer)
            cache[album] = data["results"][0]["artistName"]
            write_cache()
            return cache[album]
        except IndexError as e:
            print(f"itunes: no matches found for {album}")
        except KeyError as e:
            # We've failed. Sigh.
            print(e.with_traceback(None), file=sys.stderr)
        except json.JSONDecodeError:
            print(f"itunes: when requesting {album}, expected json got {response.text}")
            return ()


def write_cache():
    with open(CACHE_FILE_NAME, "wb") as f:
        pickle.dump(cache, f)
