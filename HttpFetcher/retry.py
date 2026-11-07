"""Async throttling and retries shared by all pages of one export."""

import asyncio
import random
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from functools import wraps

import aiohttp


def validate_retries(max_retries):
    if isinstance(max_retries, bool) or not isinstance(max_retries, int) or max_retries < 0:
        raise ValueError("max_retries must be a non-negative integer")


class RateLimiter:
    """Space request starts at least 1/rate seconds apart, without bursts."""

    def __init__(self, max_requests_per_second=10):
        if max_requests_per_second <= 0:
            raise ValueError("max_requests_per_second must be positive")
        self.interval = 1 / max_requests_per_second
        self.next_request = 0.0
        self.lock = asyncio.Lock()

    async def wait_for_token(self):
        async with self.lock:
            delay = self.next_request - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
            self.next_request = time.monotonic() + self.interval


def _retry_after(headers):
    value = (headers or {}).get("Retry-After")
    if value is None:
        return 0.0
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            return max(0.0, (parsedate_to_datetime(value) - datetime.now(timezone.utc)).total_seconds())
        except (TypeError, ValueError, OverflowError):
            return 0.0


def auto_retry(max_retries=3):
    """Retry raised aiohttp 429/5xx errors; max_retries excludes the first try."""
    validate_retries(max_retries)

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except aiohttp.ClientResponseError as error:
                    retryable = error.status == 429 or 500 <= error.status < 600
                    if not retryable or attempt == max_retries:
                        raise
                    delay = max(2 ** attempt + random.random(), _retry_after(error.headers))
                    await asyncio.sleep(delay)

        return wrapper

    return decorator
