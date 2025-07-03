import os


def load_env():
    with open(".env", "r") as f:
        for line in f:
            splitted = line.split("=")
            os.environ[splitted[0].strip()] = str(splitted[1].strip())
