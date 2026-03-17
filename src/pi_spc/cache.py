"""DuckDB-backed cache for PI time-series data.

Wraps PI bulk queries so repeated requests for the same tags and time
ranges are served from a local DuckDB file instead of re-querying PI.
"""

from __future__ import annotations

__all__ = ["PICache"]

from datetime import datetime, timedelta

import duckdb
import polars as pl

from .pi import _to_aftime, _to_datetime, get_recorded_values_bulk

_DEFAULT_DB = None

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS recorded_values (
    timestamp TIMESTAMP NOT NULL,
    tag       VARCHAR   NOT NULL,
    value     DOUBLE,
    state_name VARCHAR,
    is_good   BOOLEAN   NOT NULL,
    PRIMARY KEY (tag, timestamp)
);
"""

_CREATE_META = """
CREATE TABLE IF NOT EXISTS cache_meta (
    tag        VARCHAR   NOT NULL,
    range_start TIMESTAMP NOT NULL,
    range_end   TIMESTAMP NOT NULL,
    fetched_at  TIMESTAMP NOT NULL DEFAULT current_timestamp,
    PRIMARY KEY (tag, range_start, range_end)
);
"""


def _resolve_time(t) -> datetime:
    """Resolve a PI time expression or datetime to a concrete Python datetime."""
    if isinstance(t, datetime):
        return t
    af = _to_aftime(t)
    return _to_datetime(af)


class PICache:
    """Thin DuckDB cache over PI data retrieval."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.con = duckdb.connect(db_path)
        self.con.execute(_CREATE_TABLE)
        self.con.execute(_CREATE_META)

    def close(self):
        self.con.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_recorded_values(
        self,
        pi_server=None,
        tag_names: str | list[str] = "",
        start="*-1d",
        end="*",
        max_count: int = 10000,
        force_refresh: bool = False,
    ) -> pl.DataFrame:
        """Get recorded values, using cache when available.

        Args:
            pi_server: PIServer instance (from pi_module.connect), or None
                to read from cache only (raises if cache miss).
            tag_names: Tag name(s) to query.
            start: Start time (PI string or datetime).
            end: End time (PI string or datetime).
            max_count: Max values per tag for PI queries.
            force_refresh: If True, bypass cache and re-fetch from PI.

        Returns:
            pl.DataFrame with Timestamp, Tag, Value, StateName, IsGood.
        """
        if isinstance(tag_names, str):
            tag_names = [tag_names]

        start_dt = _resolve_time(start)
        end_dt = _resolve_time(end)

        if not force_refresh:
            uncached = self._find_uncached_tags(tag_names, start_dt, end_dt)
        else:
            uncached = tag_names

        if uncached:
            if pi_server is None:
                import warnings
                warnings.warn(
                    f"Cache miss for {len(uncached)}/{len(tag_names)} tags "
                    "and no PI server provided. Returning cached data only.",
                    stacklevel=2,
                )
            else:
                self._fetch_and_store(pi_server, uncached, start_dt, end_dt, max_count)

        return self._read_cache(tag_names, start_dt, end_dt)

    def clear_cache(self, tag_names: str | list[str] | None = None):
        """Delete cached data. If tag_names is None, clears everything."""
        if tag_names is None:
            self.con.execute("DELETE FROM recorded_values")
            self.con.execute("DELETE FROM cache_meta")
        else:
            if isinstance(tag_names, str):
                tag_names = [tag_names]
            self.con.execute(
                "DELETE FROM recorded_values WHERE tag IN (SELECT unnest(?::VARCHAR[]))",
                [tag_names],
            )
            self.con.execute(
                "DELETE FROM cache_meta WHERE tag IN (SELECT unnest(?::VARCHAR[]))",
                [tag_names],
            )

    def cache_info(self) -> pl.DataFrame:
        """Return a summary of what's cached."""
        return self.con.execute(
            """
            SELECT tag,
                   min(range_start) AS earliest,
                   max(range_end)   AS latest,
                   count(*)         AS range_segments
            FROM cache_meta
            GROUP BY tag
            ORDER BY tag
            """
        ).pl()

    def cache_event_frames(self, df: pl.DataFrame) -> int:
        """Replace cached event frames with the provided DataFrame.

        Args:
            df: DataFrame from search_event_frames (Name, StartTime, EndTime, …).

        Returns:
            Number of event frames cached.
        """
        self.con.execute("DROP TABLE IF EXISTS event_frames")
        self.con.execute("CREATE TABLE event_frames AS SELECT * FROM df")
        print(f"Cached {len(df)} event frames")
        return len(df)

    def get_event_frames(self) -> pl.DataFrame | None:
        """Read cached event frames. Returns None if cache is empty/missing."""
        try:
            df = self.con.execute("SELECT * FROM event_frames").pl()
            return df if not df.is_empty() else None
        except duckdb.CatalogException:
            return None

    def prune_old_data(self, days: int = 30) -> int:
        """Delete cached time-series rows older than *days* days.

        Also removes stale cache_meta entries.

        Returns:
            Number of recorded-value rows deleted.
        """
        cutoff = datetime.now() - timedelta(days=days)
        count = self.con.execute(
            "SELECT count(*) FROM recorded_values WHERE timestamp < ?", [cutoff]
        ).fetchone()[0]
        self.con.execute("DELETE FROM recorded_values WHERE timestamp < ?", [cutoff])
        self.con.execute("DELETE FROM cache_meta WHERE range_end < ?", [cutoff])
        print(f"Pruned {count} recorded-value rows older than {days} days")
        return count

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _find_uncached_tags(
        self, tag_names: list[str], start_dt: datetime, end_dt: datetime
    ) -> list[str]:
        """Return tags that don't have a cached range fully covering [start, end].

        Uses a 5-minute tolerance so that sliding relative times (e.g. ``*-7d``)
        still hit the cache on repeat calls.
        """
        cached = {
            r[0]
            for r in self.con.execute(
                """
                SELECT tag FROM cache_meta
                WHERE tag IN (SELECT unnest(?::VARCHAR[]))
                  AND range_start <= ? + INTERVAL '5 minutes'
                  AND range_end   >= ? - INTERVAL '5 minutes'
                """,
                [tag_names, start_dt, end_dt],
            ).fetchall()
        }
        return [t for t in tag_names if t not in cached]

    def _fetch_and_store(
        self,
        pi_server,
        tag_names: list[str],
        start_dt: datetime,
        end_dt: datetime,
        max_count: int,
    ):
        """Bulk-fetch from PI and insert into DuckDB."""
        df = get_recorded_values_bulk(
            pi_server, tag_names, start=start_dt, end=end_dt, max_count=max_count
        )

        if len(df) > 0:
            self.con.execute(
                """
                INSERT OR REPLACE INTO recorded_values
                SELECT * FROM df
                """,
            )

        for tag in tag_names:
            self.con.execute(
                """
                INSERT OR REPLACE INTO cache_meta (tag, range_start, range_end, fetched_at)
                VALUES (?, ?, ?, current_timestamp)
                """,
                [tag, start_dt, end_dt],
            )

        print(f"Cached {len(df)} rows for {len(tag_names)} tags")

    def _read_cache(
        self, tag_names: list[str], start_dt: datetime, end_dt: datetime
    ) -> pl.DataFrame:
        """Read from DuckDB and return as Polars DataFrame."""
        df = self.con.execute(
            """
            SELECT timestamp, tag, value, state_name, is_good
            FROM recorded_values
            WHERE tag IN (SELECT unnest(?::VARCHAR[]))
              AND timestamp >= ?
              AND timestamp <= ?
            ORDER BY tag, timestamp
            """,
            [tag_names, start_dt, end_dt],
        ).pl()

        return df.rename(
            {
                "timestamp": "Timestamp",
                "tag": "Tag",
                "value": "Value",
                "state_name": "StateName",
                "is_good": "IsGood",
            }
        )
