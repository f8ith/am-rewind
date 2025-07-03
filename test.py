import asyncio

import aiohttp

from get_album import itunes_fetch


async def main():
    async with aiohttp.ClientSession() as s:
        print(await itunes_fetch(s, "言葉のいらない約束 / 暁月夜 -アカツキヅクヨ- (feat. 鎖那) - EP"))


if __name__ == "__main__":
    asyncio.run(main())
