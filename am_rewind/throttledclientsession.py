## -----------------------------------------------------------
#  Class ThrottledClientSession(aiohttp.ClientSession)
#
#  Rate-limited async http client session
#
#  Inherits aiohttp.ClientSession
## -----------------------------------------------------------

from typing import Any, Literal, Optional, Tuple, TypeGuard, Union, get_args
from aiohttp import ClientSession, ClientResponse
from asyncio import (
    Queue,
    Task,
    CancelledError,
    TimeoutError,
    sleep,
    create_task,
    wait_for,
)

import time
import logging
from warnings import warn
import re
from math import ceil, log

logger = logging.getLogger()
error = logger.error
message = logger.warning
verbose = logger.info
debug = logger.debug

HTTPmethod = Literal[
    "GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS", "TRACE", "CONNECT", "PATCH"
]


def is_HTTP_method(method: Any) -> TypeGuard[HTTPmethod]:
    """Check if method is a valid HTTP method"""
    return isinstance(method, str) and method in get_args(HTTPmethod)


UrlFilter = Union[str, re.Pattern]


class ThrottledClientSession(ClientSession):
    """
    Rate-throttled client session class inherited from aiohttp.ClientSession)

    Inherits from aiohttp.ClientSession that may cause a warning.
    """

    _LOG_FILLER: int = 20

    def __init__(
        self,
        rate_limit: float = 0,
        filters: list[UrlFilter | Tuple[Optional[str], UrlFilter]] = list(),
        limit_filtered: bool = False,  # whether 'filters' allow/whitelists URLs
        re_filter: bool = False,  # use regexp filters
        *args,
        **kwargs,
    ) -> None:
        assert isinstance(rate_limit, (int, float)), "rate_limit has to be float"
        assert isinstance(filters, list), "filters has to be list"
        assert isinstance(limit_filtered, bool), "limit_filtered has to be bool"
        # assert isinstance(re_filter, bool), "re_filter has to be bool"

        super().__init__(*args, **kwargs)

        self._rate_limit: float = rate_limit
        self._fillerTask: Optional[Task] = None
        self._qlen: int = 1
        if rate_limit > self._LOG_FILLER:
            self._qlen = ceil(log(rate_limit))
        self._queue: Queue = Queue(maxsize=self._qlen)
        self._start_time: float = time.time()
        self._count: int = 0
        self._errors: int = 0
        self._limit_filtered: bool = limit_filtered
        # self._re_filter: bool = re_filter
        self._filters: list[Tuple[Optional[HTTPmethod], UrlFilter]] = list()
        for filter in filters:
            url: UrlFilter = ""
            method: str | None = None
            if isinstance(filter, tuple) and len(filter) == 2:
                method = filter[0]
                url = filter[1]
            elif isinstance(filter, str) or isinstance(filter, re.Pattern):
                url = filter
            self.add_filter(filter=url, method=method)

        self._set_limit()

    def add_filter(self, filter: UrlFilter, method: str | None = None):
        """Add a filter to filter list"""
        if not (method is None or is_HTTP_method(method=method)):
            raise ValueError(f"'method' is not None or a valid HTTP method: {method}")
        # if self._re_filter:
        #     self._filters.append((method, re.compile(filter)))
        # else:
        self._filters.append((method, filter))

    @classmethod
    def _rate_str(cls, rate: float) -> str:
        """Get rate as a formatted string"""
        if rate >= 1:
            return f"{rate:.1f} requests/sec"
        elif rate > 0:
            return f"{1/rate:.1f} secs/request"
        else:
            return "-"

    @property
    def rate_limit(self) -> float:
        return self._rate_limit

    @property
    def rate_limit_str(self) -> str:
        """Give rate-limit as formatted string"""
        return self._rate_str(self._rate_limit)

    @property
    def rate(self) -> float:
        return self._count / (time.time() - self._start_time)

    @property
    def rate_str(self) -> str:
        return self._rate_str(self.rate)

    @property
    def count(self) -> int:
        return self._count

    @property
    def errors(self) -> int:
        return self._errors

    @property
    def stats(self) -> str:
        """Get session statistics as string"""
        return f"rate limit: {self.rate_limit_str}, rate: {self.rate_str}, requests: {self.count}, errors: {self.errors}"

    @property
    def stats_dict(self) -> dict[str, float | int]:
        """Get session statistics as dict"""
        res = {
            "rate": self.rate,
            "rate_limit": self.rate_limit,
            "count": self.count,
            "errors": self.errors,
        }
        return res

    @classmethod
    def print_stats(cls, stats: dict[str, float | int]) -> str:
        try:
            rate_limit: float = stats["rate_limit"]
            rate: float = stats["rate"]
            count: float = stats["count"]
            errors: float = stats["errors"]

            rate_limit_str: str
            if rate_limit >= 1 or rate_limit == 0:
                rate_limit_str = f"{rate_limit:.1f} requests/sec"
            else:
                rate_limit_str = f"{1/rate_limit:.1f} secs/request"

            return f"rate limit: {rate_limit_str}, rate: {rate:.1f} request/sec, requests: {count:.0f}, errors: {errors:.0f}"
        except KeyError as err:
            return f"Incorrect stats format: {err}"
        except Exception as err:
            return f"Unexpected error: {err}"

    def reset_counters(self) -> dict[str, float | int]:
        """Reset rate counters and return current results"""
        res = self.stats_dict
        self._start_time = time.time()
        self._count = 0
        return res

    def _set_limit(self) -> float:
        if self._fillerTask is not None:
            self._fillerTask.cancel()
            self._fillerTask = None

        if self._rate_limit > self._LOG_FILLER:
            self._fillerTask = create_task(self._filler())
        elif self._rate_limit > 0:
            self._fillerTask = create_task(self._filler_simple())

        return self._rate_limit

    async def close(self) -> None:
        """Close rate-limiter's "bucket filler" task"""
        debug(self.stats)
        try:
            if self._fillerTask is not None:
                self._fillerTask.cancel()
                await wait_for(self._fillerTask, timeout=0.5)
        except TimeoutError as err:
            debug(f"Timeout while cancelling bucket filler: {err}")
        except CancelledError:
            debug("Cancelled")
        await super().close()

    async def _filler_simple(self) -> None:
        """Filler task to fill the leaky bucket algo"""
        assert self.rate_limit > 0, "_filler cannot be started without rate limit"
        try:
            wait: float = self._qlen / self.rate_limit
            # debug(f'SLEEP: {1/self.rate_limit}')
            while True:
                await self._queue.put(None)
                await sleep(wait)
        except CancelledError:
            debug("Cancelled")
        except Exception as err:
            error(f"{err}")
        # finally:
        # 	self._queue = None
        return None

    async def _filler(self) -> None:
        """Filler task to fill the leaky bucket algo.
        Uses longer queue for performance (maybe) :-)"""
        assert self.rate_limit > 0, "_filler cannot be started without rate limit"
        try:
            wait: float = self._qlen / self.rate_limit
            # debug(f'SLEEP: {wait}')
            while True:
                for _ in range(self._qlen):
                    await self._queue.put(None)
                await sleep(wait)
        except CancelledError:
            debug("Cancelled")
        except Exception as err:
            error(f"{err}")
        return None

    async def _request(self, *args, **kwargs) -> ClientResponse:
        """Throttled _request()"""
        if self.is_limited(method=args[0], url=args[1]):
            await self._queue.get()
            self._queue.task_done()
        resp: ClientResponse = await super()._request(*args, **kwargs)
        self._count += 1
        if not resp.ok:
            self._errors += 1
        return resp

    def is_limited(self, method: str, url: str) -> bool:
        """Check whether the rate limit should be applied"""
        try:
            if self._rate_limit == 0:
                return False
            if not is_HTTP_method(method):
                raise ValueError(f"'method' is not a valid HTTP method: {method}")
            for method_filter, url_filter in self._filters:
                if method_filter is None or method == method_filter:
                    if (
                        isinstance(url_filter, re.Pattern)
                        and url_filter.match(url) is not None
                    ):
                        return self._limit_filtered
                    elif isinstance(url_filter, str) and url.startswith(url_filter):
                        return self._limit_filtered

            return not self._limit_filtered
        except Exception as err:
            error(f"{err}")
        return True
