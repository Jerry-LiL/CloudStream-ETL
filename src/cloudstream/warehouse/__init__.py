"""Warehouse loaders. DuckDB locally, Snowflake when credentials are set."""

from cloudstream.warehouse.service import load_star_schema

__all__ = ["load_star_schema"]
