###
"""
You are given a REST endpoint GET /orders?page=1&limit=100 that returns JSON objects of the form
{ "page": n, "per_page": m, "total_pages": T, "data": [ ... ] }.
Implement a function
    fetch_all_orders(base_url, limit, since=None, out_stream) that:
    1. retrieves every page until total_pages is exhausted;
    2. flattens each record into the schema: id (string), created_at (UTC ISO 8601), amount_cents (int), customer_id (string), customer_email (string);
    3. handles HTTP errors and rate limiting by retrying 429 and 5xx responses with exponential backoff and jitter, up to a configurable max_retries;
    4. respects a rate limit of 10 requests/second;
    5. supports since to fetch only records with created_at >= since; and
    6. writes results to CSV in stable order (ascending created_at, then id) without holding all records in memory. Provide unit tests for pagination boundaries, retry behavior, and schema parsing. Finally, discuss time and space complexity, and how you would adapt the approach for cursor-based pagination where the server returns a next cursor token instead of total_pages.
"""
###
# https://dummyjson.com/products?limit=5&skip=0 #

