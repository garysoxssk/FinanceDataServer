"""Page-by-page order export with disk-backed global ordering."""

import csv
import os
import sqlite3
from contextlib import aclosing, closing
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory

import aiohttp

from .retry import RateLimiter, auto_retry, validate_retries
from .serializers import FIELDS, parse_order, parse_timestamp


DUMMY_ORDERS = (
    {"id": 3, "created_at": "2026-09-02T10:00:00+08:00", "amount_cents": 2500,
     "customer": {"id": 103, "email": "sample_email_3@gmail.com"}},
    {"id": 2, "created_at": "2026-09-01T11:00:00Z", "amount_cents": 100,
     "customer_id": "102", "customer_email": "sample_email_2@gmail.com"},
    {"id": 1, "created_at": "2026-09-01T11:00:00Z", "amount_cents": "100",
     "customer_id": "101", "customer_email": "sample_email_1@gmail.com"},
)


class DummyDataRequest:
    """Return local pages directly. Never opens a session or makes a request."""

    def __init__(self, records=None):
        self.records = DUMMY_ORDERS if records is None else records

    async def get_page(self, page, limit):
        start = (page - 1) * limit
        return {
            "page": page,
            "per_page": limit,
            "total_pages": (len(self.records) + limit - 1) // limit,
            "data": list(self.records[start:start + limit]),
        }


def _validate_page(payload, page):
    if not isinstance(payload, dict):
        raise ValueError("Page response must be an object")
    for name in ("page", "per_page", "total_pages"):
        value = payload.get(name)
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"{name} must be an integer")
    if payload["page"] != page or payload["per_page"] <= 0 or payload["total_pages"] < 0:
        raise ValueError("Invalid pagination metadata")
    if not isinstance(payload.get("data"), list):
        raise ValueError("data must be a list")
    if len(payload["data"]) > payload["per_page"]:
        raise ValueError("data exceeds per_page")
    if payload["total_pages"] == 0:
        if page != 1 or payload["data"]:
            raise ValueError("Zero total_pages requires an empty first page")
    elif page > payload["total_pages"]:
        raise ValueError("Page exceeds total_pages")
    return payload


async def _pages(base_url, limit, max_retries, dummy, dummy_records):
    async def read_pages(get_page):
        page = 1
        total_pages = None
        while True:
            payload = _validate_page(await get_page(page, limit), page)
            if total_pages is None:
                total_pages = payload["total_pages"]
            elif payload["total_pages"] != total_pages:
                raise ValueError("total_pages changed during export; retry with a stable snapshot")
            yield payload["data"]
            if page >= total_pages:
                break
            page += 1

    if dummy:
        async for records in read_pages(DummyDataRequest(dummy_records).get_page):
            yield records
        return

    limiter = RateLimiter(10)
    url = base_url.rstrip("/") + "/orders"
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        @auto_retry(max_retries=max_retries)
        async def get_page(page, page_limit):
            await limiter.wait_for_token()
            async with session.get(url, params={"page": page, "limit": page_limit}) as response:
                response.raise_for_status()
                return await response.json()

        async for records in read_pages(get_page):
            yield records


async def fetch_all_orders(
    base_url, limit, since=None, *, out_stream, max_retries=3,
    dummy=True, dummy_records=None,
):
    """Write sorted CSV to a caller-owned text stream; return rows written.

    Dummy mode is the default and never uses base_url. Live mode is explicit.
    No partial CSV is written on fetch/parse failures. Stream write failures may
    leave partial output; use export_orders_csv for atomic local replacement.
    """
    if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
        raise ValueError("limit must be a positive integer")
    validate_retries(max_retries)
    threshold = parse_timestamp(since) if since is not None else None
    if not dummy and dummy_records is not None:
        raise ValueError("dummy_records requires dummy=True")
    if not dummy and (not isinstance(base_url, str) or not base_url.startswith(("https://", "http://"))):
        raise ValueError("base_url must be an HTTP(S) API root")

    count = 0
    with TemporaryDirectory(prefix="orders-sort-") as directory:
        with closing(sqlite3.connect(Path(directory) / "orders.sqlite3")) as database:
            database.execute("PRAGMA cache_size = -2048")
            database.execute("PRAGMA temp_store = FILE")
            database.execute("""CREATE TABLE orders (
                sequence INTEGER PRIMARY KEY, id TEXT, created_at TEXT,
                amount_cents TEXT, customer_id TEXT, customer_email TEXT
            )""")
            database.execute("CREATE INDEX orders_sorted ON orders (created_at, id, sequence)")
            async with aclosing(_pages(base_url, limit, max_retries, dummy, dummy_records)) as pages:
                async for records in pages:
                    for record in records:
                        order = parse_order(record)
                        if threshold is not None and parse_timestamp(order["created_at"]) < threshold:
                            continue
                        values = [order[field] for field in FIELDS]
                        # Text storage preserves Python integers beyond SQLite's 64-bit range.
                        values[2] = str(values[2])
                        database.execute("""INSERT INTO orders
                            (id, created_at, amount_cents, customer_id, customer_email)
                            VALUES (?, ?, ?, ?, ?)""", values)
                        count += 1
                    database.commit()

            writer = csv.writer(out_stream)
            writer.writerow(FIELDS)
            rows = database.execute("""SELECT id, created_at, amount_cents, customer_id, customer_email
                FROM orders ORDER BY created_at, id, sequence""")
            writer.writerows(rows)
    return count


async def export_orders_csv(output_path, base_url="", limit=100, since=None, **options):
    """Replace a local CSV only after success; preserve the old file on failure."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = None
    try:
        with NamedTemporaryFile(
            mode="w", encoding="utf-8", newline="", dir=output_path.parent,
            prefix=".orders-", suffix=".tmp", delete=False,
        ) as stream:
            temporary_path = Path(stream.name)
            count = await fetch_all_orders(base_url, limit, since, out_stream=stream, **options)
        os.replace(temporary_path, output_path)
        return count
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
