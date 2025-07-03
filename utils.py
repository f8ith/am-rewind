import asyncio
import datetime
import inspect
import os
import pickle

from pathlib import Path

CACHE_FILE_NAME = ".get_album_cache"
BACKUP_DIR = "backups"

os.makedirs(BACKUP_DIR, exist_ok=True)


def backup_cache():
    backup_cache = load_cache()
    with open(
        f"{BACKUP_DIR}/{datetime.datetime.now().strftime('%Y-%m-%d-%H%M%S')}-{CACHE_FILE_NAME}",
        "wb",
    ) as f:
        pickle.dump(backup_cache, f)


def load_cache(name=".get_album_cache"):
    if os.path.exists(CACHE_FILE_NAME):
        with open(CACHE_FILE_NAME, "rb") as f:
            cache = pickle.load(f)
        return cache

    return {}


def write_cache(cache: dict, dest: Path = Path(CACHE_FILE_NAME)):
    with open(dest, "wb") as f:
        pickle.dump(cache, f)


def load_env():
    with open(".env", "r") as f:
        for line in f:
            splitted = line.split("=")
            os.environ[splitted[0].strip()] = str(splitted[1].strip())


def subcommand(parent, args=[], name=""):
    def decorator(func):
        parser = parent.add_parser(
            func.__name__ if name == "" else name, description=func.__doc__
        )
        for arg in args:
            parser.add_argument(*arg[0], **arg[1])

        def wrapper(arg_namespace):
            asyncio.run(func(arg_namespace))

        if inspect.iscoroutinefunction(func):
            parser.set_defaults(func=wrapper)
        else:
            parser.set_defaults(func=func)

    return decorator


def argument(*name_or_flags, **kwargs):
    return ([*name_or_flags], kwargs)
