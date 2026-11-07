# Local order CSV export

Dummy mode is the default. It reads `DUMMY_ORDERS` directly and paginates those
records locally. It never opens an HTTP session or calls the placeholder URL.
The three fixed sample records deliberately arrive out of order and include
flat and nested customer fields. There are no model writes or migrations.

## Export locally

From the project directory:

```sh
.venv/bin/python manage.py export_orders --limit 2
```

This writes `exports/orders.csv`. Opening `/dummy/` in the running Django app
also writes that file and displays its path and row count. Each successful
export replaces the previous file atomically. Failed exports preserve the
previous CSV and remove temporary files. Concurrent exports to the same path
use separate temporary files; the last successful replacement wins.

Choose another path or apply an inclusive date filter:

```sh
.venv/bin/python manage.py export_orders --output exports/recent.csv --limit 1 --since 2026-09-02T00:00:00Z
```

The destination directory is created automatically. CSV files use UTF-8,
standard CSV quoting, and this column order:

```text
id,created_at,amount_cents,customer_id,customer_email
```

## Function interface

```python
from HttpFetcher.orders import fetch_all_orders, export_orders_csv

# Inside an async function; the caller owns and closes the stream.
with open("orders.csv", "w", encoding="utf-8", newline="") as stream:
    count = await fetch_all_orders(
        "", limit=100, since=None, out_stream=stream, max_retries=3,
    )

# Preferred for local files: atomic replacement, including on write failures.
count = await export_orders_csv("exports/orders.csv", limit=100)
```

`out_stream` is required and keyword-only. `limit` must be a positive integer;
`max_retries` is a non-negative integer counting retries after the first attempt.
Both functions return the number of exported rows. Tests can supply a sequence
through `dummy_records`; an empty sequence represents zero orders.

## Parsing and ordering

- IDs accept strings or integers and are emitted as strings. Equal timestamps
  use string ID order, so `"10"` precedes `"2"`.
- `created_at` and `since` accept timezone-aware ISO 8601 strings, aware Python
  datetimes, or finite Unix timestamps in seconds. Naive dates are rejected
  instead of assuming a timezone. UTC output has fixed microsecond precision.
- `amount_cents` accepts integers and integer strings, including negative
  values. Booleans and floats are rejected; amounts are never silently rounded.
- Customer fields can be flat (`customer_id`, `customer_email`) or nested
  (`customer.id`, `customer.email`). Present flat fields take precedence.
- Missing/invalid required values raise `ValueError`. Invalid records are not
  silently skipped. Email must be a non-empty string; no mailbox verification
  or additional email-format policy is imposed by the exercise.
- Filtering is inclusive: `created_at >= since`. Every page is inspected
  because the API does not promise chronological ordering or server filtering.
- A temporary SQLite database stores matching records and maintains an index
  on `(created_at, id, sequence)`. The CSV streams from that index. Identical
  date/ID keys retain arrival order; duplicates are preserved.

No CSV bytes are emitted until fetching and parsing succeed. A caller-provided
stream can still contain partial output if its write fails. The local-file
wrapper avoids replacing the destination in that situation. Atomic replacement
does not promise durability across power loss; no explicit disk sync is used.

## HTTP path for the future endpoint

The HTTP path is implemented and covered with mocked sessions, but has not been
tested against a real order server. It requires explicit `dummy=False` in the
Python function. `base_url` is the API root, for example
`https://example.test/api`, to which `/orders` is appended. The command and
Django view currently always use dummy mode.

HTTP requests use `page` and `limit` query parameters. The first response defines
`total_pages`; zero/one-page responses and an empty intermediate page are
handled. Invalid metadata or a changing page count fails the export instead of
claiming completeness. A stable server snapshot is still necessary to avoid
missing/duplicating records if the underlying dataset changes without changing
the page count.

One limiter is shared across all HTTP page attempts in an export. Request starts
are spaced at least 0.1 seconds apart, including retries. Separate simultaneous
exports each have their own limiter; a shared account-wide quota would require
a shared limiter across those exports/processes.

Only HTTP 429 and 500–599 errors retry. Backoff is `2**attempt + uniform[0, 1)`
seconds, starting with attempt zero. `Retry-After` seconds/HTTP dates are respected
when longer. Exhaustion raises the final error without another sleep. Other
HTTP errors, transport failures, timeouts, and invalid JSON propagate. Each
request has a 30-second timeout; failed response contexts and sessions close
on errors and cancellation.

## Complexity

Let `N` be records fetched, `K` matching records, `P` pages, `M` the largest
decoded page, and `R` the configured retry count. Treat record sizes as bounded.

- Processing/indexing takes `O(N + K log(K + 1))` time; ordered CSV output takes
  `O(K)` time. Page commits add `O(P)` operations and disk-sync overhead.
- There are `P` successful HTTP requests, or at most `P * (R + 1)` attempts.
  Throttling, network latency, and backoff add wall-clock time. Backoff without
  `Retry-After` can total `O(P * 2**R)` seconds in the worst retry case.
- Working memory is `O(M)` plus a fixed SQLite cache (approximately 2 MiB) and
  fixed CSV buffers. Records and tasks do not accumulate across pages.
  Temporary indexed disk storage and final CSV output are `O(K)`.
- Dummy fixtures themselves are an already-resident sequence. The exporter does
  not copy that entire sequence, but the supplied dummy source occupies memory.

## Adapting to cursor pagination

Replace the page-number loop with a sequential loop that sends the previous
response's `next_cursor`, stopping when it is absent/null. Retry the same cursor
on 429/5xx and advance it only after processing a successful response. Validate
the token type and reject a token equal to the current token; detecting longer
cycles without unbounded memory can use a disk-backed set of seen tokens.

The parser, inclusive filter, retry policy, limiter, SQLite index, and CSV writer
remain unchanged. Pages normally cannot be fetched concurrently because the
next token is unknown until the current response arrives. Cursor pagination
does not itself guarantee date/ID ordering, so disk-backed sorting remains
necessary unless the server explicitly guarantees the required total ordering.

## Tests

```sh
.venv/bin/python manage.py test HttpFetcher
```

All tests run offline. Coverage includes pagination boundaries, response
validation, retry counts/backoff, throttling across concurrent requests,
schema parsing, cross-page sorting, inclusive filtering, CSV escaping, cleanup,
atomic replacement, the local export command, and the Django view.
