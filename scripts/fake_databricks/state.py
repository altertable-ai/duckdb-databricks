"""Shared rows and server counters for the fake Databricks warehouse."""

from decimal import Decimal

ORDERS = [
    {"id": 1, "region": "east", "amount": Decimal("10.50"), "note": '{"a": 1}'},
    {"id": 2, "region": "west", "amount": Decimal("20.00"), "note": None},
    {"id": 3, "region": None, "amount": None, "note": "null"},
]

STATE = None
