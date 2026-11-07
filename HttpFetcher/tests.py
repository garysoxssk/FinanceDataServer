import asyncio
import csv
import io
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import RequestFactory, override_settings

from .orders import DUMMY_ORDERS, DummyDataRequest, export_orders_csv, fetch_all_orders
from .retry import RateLimiter, auto_retry
from .serializers import FIELDS, parse_order, parse_timestamp
from .views import data_viewer


def order(identifier="1", timestamp="2026-09-01T11:00:00Z", **changes):
    return {
        "id": identifier, "created_at": timestamp, "amount_cents": 100,
        "customer_id": "101", "customer_email": "a@example.test", **changes,
    }


def page(number, total, records, limit=2):
    return {"page": number, "per_page": limit, "total_pages": total, "data": records}


def http_error(status, headers=None):
    return aiohttp.ClientResponseError(
        SimpleNamespace(real_url="https://example.test/orders"), (),
        status=status, message="test failure", headers=headers,
    )


def response(payload=None, status=200, headers=None):
    result = MagicMock()
    result.__aenter__ = AsyncMock(return_value=result)
    result.__aexit__ = AsyncMock(return_value=False)
    result.json = AsyncMock(return_value=payload)
    result.raise_for_status = MagicMock(
        side_effect=http_error(status, headers) if status >= 400 else None
    )
    return result


def session_for(responses):
    session = MagicMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=False)
    session.get = MagicMock(side_effect=responses)
    return session


class SchemaTests(unittest.TestCase):
    def test_flat_record_and_integer_string(self):
        parsed = parse_order(order(7, amount_cents="-123"))
        self.assertEqual(tuple(parsed), FIELDS)
        self.assertEqual(parsed["id"], "7")
        self.assertEqual(parsed["amount_cents"], -123)
        self.assertIsInstance(parsed["amount_cents"], int)
        self.assertEqual(parsed["created_at"], "2026-09-01T11:00:00.000000Z")

    def test_nested_customer(self):
        record = {"id": 9, "created_at": "2026-09-01T19:00:00+08:00",
                  "amount_cents": 15, "customer": {"id": 20, "email": "a@example.test"}}
        parsed = parse_order(record)
        self.assertEqual(parsed["customer_id"], "20")
        self.assertEqual(parsed["customer_email"], "a@example.test")
        self.assertEqual(parsed["created_at"], "2026-09-01T11:00:00.000000Z")

    def test_supported_timestamp_types(self):
        for value in (0, 0.0, "1970-01-01T08:00:00+08:00", datetime(1970, 1, 1, tzinfo=timezone.utc)):
            with self.subTest(value=value):
                self.assertEqual(parse_timestamp(value), datetime(1970, 1, 1, tzinfo=timezone.utc))

    def test_bad_timestamps(self):
        for value in (True, None, "bad", "2026-09-01", datetime(2026, 9, 1), float("nan"), float("inf")):
            with self.subTest(value=value), self.assertRaises(ValueError):
                parse_timestamp(value)

    def test_fractional_and_boolean_cents_are_rejected(self):
        for amount in (1.5, 1.0, True, None, "1.5", "abc"):
            with self.subTest(amount=amount), self.assertRaises(ValueError):
                parse_order(order(amount_cents=amount))

    def test_missing_fields_and_bad_types(self):
        for field in FIELDS:
            record = order()
            del record[field]
            with self.subTest(missing=field), self.assertRaises(ValueError):
                parse_order(record)
        for changes in ({"id": None}, {"id": True}, {"id": ""}, {"customer_id": []}, {"customer_email": None}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                parse_order(order(**changes))
        with self.assertRaises(ValueError):
            parse_order([])


class RetryTests(unittest.IsolatedAsyncioTestCase):
    async def test_success_returns_once_without_sleep(self):
        function = AsyncMock(return_value={"ok": True})
        with patch("HttpFetcher.retry.asyncio.sleep", new_callable=AsyncMock) as sleep:
            self.assertEqual(await auto_retry(3)(function)(), {"ok": True})
        function.assert_awaited_once()
        sleep.assert_not_awaited()

    async def test_429_and_5xx_use_exponential_jitter(self):
        for status in (429, 500, 503, 599):
            with self.subTest(status=status):
                function = AsyncMock(side_effect=[http_error(status), http_error(status), "ok"])
                with patch("HttpFetcher.retry.asyncio.sleep", new_callable=AsyncMock) as sleep, patch(
                    "HttpFetcher.retry.random.random", return_value=0.25
                ):
                    self.assertEqual(await auto_retry(3)(function)(), "ok")
                self.assertEqual(function.await_count, 3)
                self.assertEqual([call.args[0] for call in sleep.await_args_list], [1.25, 2.25])

    async def test_exhaustion_raises_last_error_without_final_sleep(self):
        failures = [http_error(503) for _ in range(4)]
        function = AsyncMock(side_effect=failures)
        with patch("HttpFetcher.retry.asyncio.sleep", new_callable=AsyncMock) as sleep:
            with self.assertRaises(aiohttp.ClientResponseError) as caught:
                await auto_retry(3)(function)()
        self.assertIs(caught.exception, failures[-1])
        self.assertEqual(function.await_count, 4)
        self.assertEqual(sleep.await_count, 3)

    async def test_zero_retries_still_makes_one_attempt(self):
        function = AsyncMock(side_effect=http_error(429))
        with patch("HttpFetcher.retry.asyncio.sleep", new_callable=AsyncMock) as sleep:
            with self.assertRaises(aiohttp.ClientResponseError):
                await auto_retry(0)(function)()
        function.assert_awaited_once()
        sleep.assert_not_awaited()

    async def test_other_http_and_transport_errors_propagate(self):
        for failure in (http_error(400), http_error(401), http_error(404), aiohttp.ClientConnectionError(), TimeoutError()):
            with self.subTest(failure=type(failure)):
                function = AsyncMock(side_effect=failure)
                with patch("HttpFetcher.retry.asyncio.sleep", new_callable=AsyncMock) as sleep:
                    with self.assertRaises(type(failure)):
                        await auto_retry(3)(function)()
                function.assert_awaited_once()
                sleep.assert_not_awaited()

    async def test_retry_after_is_respected(self):
        function = AsyncMock(side_effect=[http_error(429, {"Retry-After": "10"}), "ok"])
        with patch("HttpFetcher.retry.asyncio.sleep", new_callable=AsyncMock) as sleep:
            self.assertEqual(await auto_retry(1)(function)(), "ok")
        sleep.assert_awaited_once_with(10.0)

    async def test_retry_after_http_date_and_bad_value(self):
        now = datetime(2026, 9, 1, tzinfo=timezone.utc)
        for header, expected in (("Tue, 01 Sep 2026 00:00:10 GMT", 10.0), ("invalid", 1.25)):
            with self.subTest(header=header):
                function = AsyncMock(side_effect=[http_error(503, {"Retry-After": header}), "ok"])
                with patch("HttpFetcher.retry.datetime") as clock, patch(
                    "HttpFetcher.retry.asyncio.sleep", new_callable=AsyncMock
                ) as sleep, patch("HttpFetcher.retry.random.random", return_value=0.25):
                    clock.now.return_value = now
                    await auto_retry(1)(function)()
                sleep.assert_awaited_once_with(expected)

    async def test_bad_retry_configuration(self):
        for value in (-1, True, 1.5, "3"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                auto_retry(value)


class RateLimiterTests(unittest.IsolatedAsyncioTestCase):
    async def test_concurrent_requests_share_spacing_and_replenish(self):
        clock = SimpleNamespace(now=0.0)
        real_sleep = asyncio.sleep

        async def sleep(delay):
            clock.now += delay
            await real_sleep(0)

        limiter = RateLimiter(10)
        starts = []

        async def request():
            await limiter.wait_for_token()
            starts.append(clock.now)

        with patch("HttpFetcher.retry.time", SimpleNamespace(monotonic=lambda: clock.now)), patch(
            "HttpFetcher.retry.asyncio.sleep", side_effect=sleep
        ):
            await asyncio.gather(*(request() for _ in range(25)))
            clock.now += 10
            await request()
        self.assertEqual(len(starts), 26)
        self.assertEqual(starts[0], 0)
        for earlier, later in zip(starts, starts[1:]):
            self.assertGreaterEqual(later - earlier, 0.1 - 1e-9)


class ExportTests(unittest.IsolatedAsyncioTestCase):
    async def test_dummy_pagination_boundaries_never_create_http_session(self):
        for size, limit in ((0, 2), (1, 2), (2, 2), (3, 2), (4, 2), (5, 2)):
            records = [order(str(i)) for i in range(size)]
            stream = io.StringIO()
            original = DummyDataRequest.get_page
            calls = []

            async def tracked(instance, page_number, page_limit):
                calls.append((page_number, page_limit))
                return await original(instance, page_number, page_limit)

            with self.subTest(size=size), patch("HttpFetcher.orders.aiohttp.ClientSession", side_effect=AssertionError("Network forbidden")), patch.object(
                DummyDataRequest, "get_page", new=tracked
            ):
                count = await fetch_all_orders("unused", limit, out_stream=stream, dummy_records=records)
            self.assertEqual(count, size)
            self.assertEqual(calls, [(n, limit) for n in range(1, max(1, (size + limit - 1) // limit) + 1)])
            self.assertEqual(len(list(csv.DictReader(io.StringIO(stream.getvalue())))), size)
            self.assertEqual(next(csv.reader(io.StringIO(stream.getvalue()))), list(FIELDS))
            self.assertFalse(stream.closed)

    async def test_global_order_since_inclusive_and_string_id_tie_break(self):
        records = [
            order("z", "2026-09-02T00:00:00Z"),
            order("excluded", "2026-09-01T10:59:59Z"),
            order("2", "2026-09-01T19:00:00+08:00"),
            order("10", "2026-09-01T11:00:00Z"),
            order("fraction", "2026-09-01T11:00:00.000001Z"),
        ]
        stream = io.StringIO()
        count = await fetch_all_orders("", 1, since="2026-09-01T19:00:00+08:00", out_stream=stream, dummy_records=records)
        rows = list(csv.DictReader(io.StringIO(stream.getvalue())))
        self.assertEqual(count, 4)
        self.assertEqual([row["id"] for row in rows], ["10", "2", "fraction", "z"])

    async def test_duplicates_preserve_input_order_and_csv_escaping(self):
        records = [order("same", amount_cents=2 ** 80, customer_email='a,"b\nc'), order("same", amount_cents=2)]
        stream = io.StringIO()
        await fetch_all_orders("", 1, out_stream=stream, dummy_records=records)
        rows = list(csv.DictReader(io.StringIO(stream.getvalue())))
        self.assertEqual([int(row["amount_cents"]) for row in rows], [2 ** 80, 2])
        self.assertEqual(rows[0]["customer_email"], 'a,"b\nc')

    async def test_all_filtered_writes_header_only(self):
        stream = io.StringIO()
        count = await fetch_all_orders("", 2, "2027-01-01T00:00:00Z", out_stream=stream)
        self.assertEqual(count, 0)
        self.assertEqual(list(csv.reader(io.StringIO(stream.getvalue()))), [list(FIELDS)])

    async def test_invalid_inputs_do_not_write_output(self):
        cases = [
            {"limit": 0}, {"limit": True}, {"limit": 1.5}, {"max_retries": -1},
            {"since": "2026-01-01"}, {"dummy_records": [order(created_at="bad")]},
            {"dummy": False, "base_url": "bad"},
            {"dummy": False, "base_url": "https://example.test", "dummy_records": []},
        ]
        for changes in cases:
            stream = io.StringIO()
            options = {"base_url": "", "limit": 2, "out_stream": stream, **changes}
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                await fetch_all_orders(**options)
            self.assertEqual(stream.getvalue(), "")

    async def test_live_pagination_retry_and_response_cleanup_with_mocks(self):
        responses = [response(status=429), response(page(1, 2, [order("2"), order("1")])), response(page(2, 2, [order("3")]))]
        session = session_for(responses)
        limiter = SimpleNamespace(wait_for_token=AsyncMock())
        stream = io.StringIO()
        with patch("HttpFetcher.orders.aiohttp.ClientSession", return_value=session), patch(
            "HttpFetcher.orders.RateLimiter", return_value=limiter
        ) as limiter_factory, patch("HttpFetcher.retry.asyncio.sleep", new_callable=AsyncMock):
            count = await fetch_all_orders("https://example.test/api/", 2, out_stream=stream, dummy=False)
        self.assertEqual(count, 3)
        self.assertEqual([call.kwargs["params"] for call in session.get.call_args_list], [
            {"page": 1, "limit": 2}, {"page": 1, "limit": 2}, {"page": 2, "limit": 2},
        ])
        self.assertTrue(all(call.args[0] == "https://example.test/api/orders" for call in session.get.call_args_list))
        limiter_factory.assert_called_once_with(10)
        self.assertEqual(limiter.wait_for_token.await_count, 3)
        for item in responses:
            item.__aexit__.assert_awaited_once()
        session.__aexit__.assert_awaited_once()

    async def test_empty_first_page_does_not_end_before_total_pages(self):
        session = session_for([response(page(1, 2, [])), response(page(2, 2, [order()]))])
        with patch("HttpFetcher.orders.aiohttp.ClientSession", return_value=session), patch(
            "HttpFetcher.orders.RateLimiter", return_value=SimpleNamespace(wait_for_token=AsyncMock())
        ):
            self.assertEqual(await fetch_all_orders("https://example.test", 2, out_stream=io.StringIO(), dummy=False), 1)
        self.assertEqual(session.get.call_count, 2)

    async def test_parse_failure_closes_live_session_immediately(self):
        item = response(page(1, 2, [order(amount_cents=None)]))
        session = session_for([item])
        with patch("HttpFetcher.orders.aiohttp.ClientSession", return_value=session), patch(
            "HttpFetcher.orders.RateLimiter", return_value=SimpleNamespace(wait_for_token=AsyncMock())
        ):
            with self.assertRaises(ValueError):
                await fetch_all_orders("https://example.test", 2, out_stream=io.StringIO(), dummy=False)
            session.__aexit__.assert_awaited_once()
        item.__aexit__.assert_awaited_once()

    async def test_later_http_failure_preserves_existing_csv(self):
        session = session_for([
            response(page(1, 2, [order()])), response(status=503), response(status=503),
        ])
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "orders.csv"
            target.write_text("previous export", encoding="utf-8")
            with patch("HttpFetcher.orders.aiohttp.ClientSession", return_value=session), patch(
                "HttpFetcher.orders.RateLimiter", return_value=SimpleNamespace(wait_for_token=AsyncMock())
            ), patch("HttpFetcher.retry.asyncio.sleep", new_callable=AsyncMock):
                with self.assertRaises(aiohttp.ClientResponseError):
                    await export_orders_csv(target, "https://example.test", limit=2, dummy=False, max_retries=1)
            self.assertEqual(target.read_text(encoding="utf-8"), "previous export")
            self.assertEqual(list(Path(directory).iterdir()), [target])
        session.__aexit__.assert_awaited_once()

    async def test_bad_pagination_and_changing_total_fail_without_csv(self):
        cases = [
            [None], [page(2, 2, [])], [page(1, -1, [])],
            [page(1, 0, [order()])], [page(1, 1, {}, 2)],
            [page(1, True, [])], [page(1, 1, [], 0)],
            [page(1, 1, [order(), order(), order()])],
            [page(1, 2, []), page(2, 3, [])],
        ]
        for payloads in cases:
            stream = io.StringIO()
            session = session_for([response(payload) for payload in payloads])
            with self.subTest(payloads=payloads), patch("HttpFetcher.orders.aiohttp.ClientSession", return_value=session), patch(
                "HttpFetcher.orders.RateLimiter", return_value=SimpleNamespace(wait_for_token=AsyncMock())
            ):
                with self.assertRaises(ValueError):
                    await fetch_all_orders("https://example.test", 2, out_stream=stream, dummy=False)
            self.assertEqual(stream.getvalue(), "")

    async def test_atomic_file_success_and_failure_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "nested" / "orders.csv"
            self.assertEqual(await export_orders_csv(target, limit=1), len(DUMMY_ORDERS))
            previous = target.read_bytes()
            with self.assertRaises(ValueError):
                await export_orders_csv(target, dummy_records=[order(), order(amount_cents=None)])
            self.assertEqual(target.read_bytes(), previous)
            self.assertEqual(list(target.parent.iterdir()), [target])
            with patch("HttpFetcher.orders.os.replace", side_effect=OSError("disk error")):
                with self.assertRaises(OSError):
                    await export_orders_csv(target)
            self.assertEqual(target.read_bytes(), previous)
            self.assertEqual(list(target.parent.iterdir()), [target])

    async def test_stream_failure_removes_sort_database(self):
        with tempfile.TemporaryDirectory() as directory:
            stream = MagicMock()
            stream.write.side_effect = OSError("disk full")
            with patch("HttpFetcher.orders.TemporaryDirectory", side_effect=lambda **kwargs: tempfile.TemporaryDirectory(dir=directory)):
                with self.assertRaises(OSError):
                    await fetch_all_orders("", 1, out_stream=stream)
            self.assertEqual(list(Path(directory).iterdir()), [])

    async def test_dummy_view_saves_local_csv_without_network(self):
        with tempfile.TemporaryDirectory() as directory, override_settings(BASE_DIR=Path(directory)), patch(
            "HttpFetcher.orders.aiohttp.ClientSession", side_effect=AssertionError("Network forbidden")
        ):
            result = await data_viewer(RequestFactory().get("/dummy/"))
            self.assertEqual(result.status_code, 200)
            self.assertIn(b"Saved 3 orders", result.content)
            self.assertTrue((Path(directory) / "exports" / "orders.csv").exists())


class CommandTests(unittest.TestCase):
    def test_command_saves_csv_with_filter_without_http(self):
        with tempfile.TemporaryDirectory() as directory, patch(
            "HttpFetcher.orders.aiohttp.ClientSession", side_effect=AssertionError("Network forbidden")
        ):
            target = Path(directory) / "orders.csv"
            stdout = io.StringIO()
            call_command("export_orders", output=target, limit=1, since="2026-09-02T00:00:00Z", stdout=stdout)
            with target.open(newline="", encoding="utf-8") as stream:
                rows = list(csv.DictReader(stream))
            self.assertEqual([row["id"] for row in rows], ["3"])
            self.assertIn("Saved 1 dummy orders", stdout.getvalue())

    def test_command_invalid_limit_is_clear_error(self):
        with tempfile.TemporaryDirectory() as directory, self.assertRaises(CommandError):
            call_command("export_orders", output=Path(directory) / "orders.csv", limit=0)
