import argparse
from pathlib import Path

import listenbrainz
import parse_history
import parse_activity
from utils import subcommand, argument, load_cache, CACHE_FILE_NAME, write_cache

parser = argparse.ArgumentParser(description="misc functions.")
subparsers = parser.add_subparsers(dest="subcommand")


@subcommand(
    subparsers,
    args=[
        argument(
            "--dest",
            type=Path,
            nargs="?",
            default=CACHE_FILE_NAME,
            help="path to new cache file",
        ),
    ],
)
async def clear_unknowns(args):
    cache = load_cache()
    new_cache = {}

    count = 0
    for k, v in cache.items():
        if cache[k] == ():
            count += 1
        else:
            new_cache[k] = v

    print(f"{count} unknown artists cleared")
    write_cache(new_cache, args.dest)


@subcommand(
    subparsers,
    args=[
        argument(
            "--dest",
            type=Path,
            nargs="?",
            default=CACHE_FILE_NAME,
            help="path to new cache file",
        ),
    ],
)
async def fix_cache(args):
    cache = load_cache()
    new_cache = {}

    for k, v in cache.items():
        if isinstance(cache[k], str):
            new_cache[k] = (v,)
        elif isinstance(cache[k], tuple):
            new_cache[k] = v
        else:
            print("unknown type?")
            return

    write_cache(new_cache, args.dest)


@subcommand(
    subparsers,
    args=[
        argument("find", help="specify an artist to overwrite"),
        argument("replace", help="new name of artist"),
        argument(
            "--dest",
            type=Path,
            default=CACHE_FILE_NAME,
            help="path to new cache file",
        ),
    ],
)
async def replace_cache(args):
    cache = load_cache()

    for k, v in cache.items():
        for i in range(len(v)):
            if v[i].strip() == args.find:
                cache[k] = v[:i] + (args.replace,) + v[i + 1 :]
    write_cache(cache, args.dest)


@subcommand(
    subparsers,
    args=[
        argument(
            "--dest",
            nargs="?",
            default=CACHE_FILE_NAME,
            help="path to new cache file",
        ),
    ],
)
async def print_cache(args):
    cache = load_cache()

    for k, v in cache.items():
        for i in range(len(v)):
            if v[i].strip() == args.find:
                cache[k] = v[:i] + (args.replace,) + v[i + 1 :]

    write_cache(cache, args.dest)


subcommand(subparsers, name="activity")(parse_activity.parse_activity)
subcommand(subparsers, name="history")(parse_history.parse_history)
subcommand(
    subparsers,
    name="listenbrainz",
    args=[
        argument("csv_file", type=Path),
        argument("--pretend", default=False, action="store_true"),
    ],
)(listenbrainz.submit_payload)


def main():
    # Parse arguments
    parser.add_argument("--debug", default=False, action="store_true")
    args = parser.parse_args()
    if args.subcommand is None:
        parser.print_help()
    else:
        args.func(args)


if __name__ == "__main__":
    main()
