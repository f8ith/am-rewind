# We need a dedicated module to guessing the album, because Apple can't fucking give it to us. The best
# way to do this would be through an API, but that's not factored in yet; this is pretty brute-force right now.
from enum import Enum
import io
import json
import os
import re
import sys
from typing import List

import aiohttp

from utils import write_cache, load_env, load_cache

load_env()

API_KEY = os.environ.get("LASTFM_API_KEY", "")
CACHE_HIT = 0
CACHE_MISS = 1
RETRY_COUNT = 3
USE_ITUNES = True

banned_words = [
    "(Deluxe)",
    "(Extended)",
    "(Live)",
    "[Karaoke Edition]",
    "(Karaoke Edition)",
    "- Single",
]
rx = re.compile("|".join(banned_words))


# shared cache between lastfm and itunes
async def get_artist_from_album(
    s: aiohttp.ClientSession, cache: dict, album: str, retry_count: int = 0
) -> List:
    ### There is trouble parsing special releases like Extended, Deluxe etc. albums. Strip album name of such modifiers
    rx.sub("", album)
    album.strip()

    if retry_count > RETRY_COUNT:
        print("retry count exceeded")
        sys.exit(1)

    if album in cache:
        try:
            return [
                cache[album],
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
                cache[album] = (data["results"]["albummatches"]["album"][0]["artist"],)

                # Write back to cache
                write_cache(cache)

                return [
                    cache[album],
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

    if USE_ITUNES:
        # Just cache the artist as unknown at this point if () is returned
        cache[album] = await itunes_fetch(s, cache, album, "album")
        return [cache[album], CACHE_MISS]
    return [(), CACHE_MISS]


class LangEnum(str, Enum):
    us = "en_us"
    jp = "ja_jp"


async def itunes_fetch(
    s: aiohttp.ClientSession,
    cache: dict,
    term: str,
    entity: str,
):
    global USE_ITUNES
    lang = "en_us" if term.isascii() else "ja_jp"  # sensible default
    async with s.get(
        "https://itunes.apple.com/search",
        params={
            "term": term,
            "music": "music",
            "country": "HK",
            "lang": lang,
            "entity": entity,
        },
    ) as response:
        if response.status == 403:
            ### Do not write cache if rate limited
            print(f"itunes: rate limited")
            USE_ITUNES = False
            return ()

        cache[term] = ()
        try:
            buffer = io.BytesIO(await response.read())
            data = json.load(buffer)
            cache[term] = (data["results"][0]["artistName"],)
        except IndexError as e:
            print(f"itunes: no matches found for {term}")
        except json.JSONDecodeError:
            print(f"itunes: when requesting {term}, expected json got {response.text}")
        except Exception as e:
            # We've failed. Sigh.
            print(e.with_traceback(None), file=sys.stderr)

        write_cache(cache)

        return cache[term]
