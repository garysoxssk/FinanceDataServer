"""Validate API records without depending on Django models or a database."""

import math
import re
from datetime import datetime, timezone


FIELDS = ("id", "created_at", "amount_cents", "customer_id", "customer_email")


def parse_timestamp(value):
    """Accept aware ISO 8601/datetime values or finite Unix seconds; return UTC."""
    try:
        if isinstance(value, bool):
            raise ValueError("boolean timestamps are invalid")
        if isinstance(value, (int, float)):
            if not math.isfinite(value):
                raise ValueError("timestamp must be finite")
            result = datetime.fromtimestamp(value, timezone.utc)
        elif isinstance(value, str):
            result = datetime.fromisoformat(value)
        elif isinstance(value, datetime):
            result = value
        else:
            raise ValueError("unsupported timestamp type")
        if result.tzinfo is None or result.utcoffset() is None:
            raise ValueError("timestamp must include a timezone")
        return result.astimezone(timezone.utc)
    except (TypeError, ValueError, OverflowError, OSError) as error:
        raise ValueError(f"Invalid timestamp: {value!r}") from error


def _string_id(value, field):
    if isinstance(value, bool) or not isinstance(value, (str, int)) or str(value) == "":
        raise ValueError(f"{field} must be a non-empty string or integer")
    return str(value)


def parse_order(record):
    if not isinstance(record, dict):
        raise ValueError("Each order must be an object")
    try:
        customer = record.get("customer", {})
        if not isinstance(customer, dict):
            customer = {}
        amount = record["amount_cents"]
        if isinstance(amount, bool) or not (
            isinstance(amount, int)
            or isinstance(amount, str) and re.fullmatch(r"[+-]?[0-9]+", amount)
        ):
            raise ValueError("amount_cents must be an integer or integer string")
        email = record.get("customer_email", customer.get("email"))
        if not isinstance(email, str) or not email:
            raise ValueError("customer_email must be a non-empty string")
        return {
            "id": _string_id(record["id"], "id"),
            # Fixed precision makes the stored UTC text sort chronologically.
            "created_at": parse_timestamp(record["created_at"]).isoformat(timespec="microseconds").replace("+00:00", "Z"),
            "amount_cents": int(amount),
            "customer_id": _string_id(record.get("customer_id", customer.get("id")), "customer_id"),
            "customer_email": email,
        }
    except KeyError as error:
        raise ValueError(f"Missing required field: {error.args[0]}") from error
