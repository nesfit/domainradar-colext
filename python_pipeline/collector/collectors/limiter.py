"""limiter.py: The local rate limiter for the RDAP collectors."""
__author__ = "Ondřej Ondryáš <xondry02@vut.cz>"

import asyncio

from asynciolimiter import (Limiter as AioLim,
                            StrictLimiter as AioStrict,
                            LeakyBucketLimiter as AioLeakyBucket)

TLim = AioLim | AioStrict | AioLeakyBucket


class Limiter:
    OK = 0
    IMMEDIATE_FAIL = 1
    TIMEOUT = 2

    def __init__(self, limiter: TLim, immediate: bool, max_wait: float):
        self._limiter = limiter
        self._immediate = immediate
        self._max_wait = max_wait
        self._next_fill = False

    @property
    def max_time(self):
        return 0.1 if self._immediate else self._max_wait

    async def _wait_with_possible_fill(self):
        if self._next_fill:
            self._next_fill = False
            await self.fill()
        await self._limiter.wait()

    async def acquire(self) -> int:
        timeout = self.max_time
        if timeout == -1:
            await self._wait_with_possible_fill()
            return self.OK
        else:
            try:
                await asyncio.wait_for(self._wait_with_possible_fill(), timeout)
                return self.OK
            except TimeoutError:
                return self.IMMEDIATE_FAIL if self._immediate else self.TIMEOUT

    def fill_on_next(self):
        self._next_fill = True

    # noinspection PyProtectedMember
    async def fill(self, n: int | None = None):
        n = n if n is not None else max(int(self._limiter.rate), 1)
        lim = self._limiter
        futures = []
        for _ in range(n):
            if not lim._locked:
                lim._maybe_lock()
                continue
            fut = asyncio.get_running_loop().create_future()
            futures.append(fut)

        if len(futures) > 0:
            lim._waiters.extend(futures)
            await asyncio.wait(futures)


class LimiterProvider:
    _type_map = {"limiter": AioLim, "strict": AioStrict, "leaky_bucket_meter": AioLeakyBucket}

    def __init__(self, config: dict):
        self._default = config.get("limiter", {"immediate": False,
                                               "max_wait": -1,
                                               "type": "limiter",
                                               "rate": 5,
                                               "max_burst": 5})

        self._default_immediate = self._default.get("immediate", False)
        self._default_max_wait = self._default.get("max_wait", -1)
        self._default_type = self._default.get("type", "limiter")
        self._overrides = config.get("limiter_overrides", {})

        self._limiters: dict[str, Limiter] = {}

    def get_limiter(self, endpoint: str, tld: str | None = None) -> Limiter:
        if endpoint in self._limiters:
            return self._limiters[endpoint]

        settings = self._overrides.get(endpoint)
        if not settings:
            if not tld:
                settings = self._default
            else:
                settings = self._overrides.get(tld, self._default)

        new_limiter = self._make_limiter(settings)
        self._limiters[endpoint] = new_limiter
        return new_limiter

    def _make_limiter(self, settings: dict) -> Limiter:
        lim_type = settings.get("type", self._default_type)
        immediate = settings.get("immediate", self._default_immediate)
        max_wait = settings.get("max_wait", self._default_max_wait)

        args = {"type": None, "immediate": None, "max_wait": None} | settings
        del args["type"]
        del args["immediate"]
        del args["max_wait"]

        limiter_cls = self._type_map.get(lim_type)
        if not limiter_cls:
            raise ValueError(f"Unknown limiter type: {lim_type}")

        return Limiter(limiter_cls(**args), immediate, max_wait)
