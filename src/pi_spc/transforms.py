"""Data transformation helpers for PI time-series data.

Functions for converting state-change signals into time intervals and
filtering process data by operational mode or equipment state.
"""

from __future__ import annotations

from datetime import datetime
from typing import Sequence

import polars as pl

__all__ = [
    "state_to_intervals",
    "filter_by_mode",
    "filter_by_mode_and_state",
]


def state_to_intervals(
    df: pl.DataFrame,
    target_value=1,
    tag_col: str = "Tag",
    ts_col: str = "Timestamp",
    value_col: str = "Value",
    batch_end: datetime | None = None,
) -> pl.DataFrame:
    """Convert state-change rows into ``(Start, End)`` intervals.

    For each tag, finds contiguous periods where *value_col* equals
    *target_value* and returns one row per interval with ``Start`` and
    ``End`` timestamps.

    Args:
        df: DataFrame with at least tag, timestamp, and value columns.
        target_value: The value that indicates the "active" state.
        tag_col: Name of the tag/group column.
        ts_col: Name of the timestamp column.
        value_col: Name of the value column.
        batch_end: If provided, fills null ``End`` values (i.e. the last
            open interval) with this timestamp.

    Returns:
        A DataFrame with columns ``[tag_col, "Start", "End"]``.

    Example::

        >>> from pi_spc.transforms import state_to_intervals
        >>> intervals = state_to_intervals(drive_df, target_value=0)
    """
    intervals = (
        df.sort(tag_col, ts_col)
        .with_columns(
            prev_val=pl.col(value_col).shift(1).over(tag_col),
        )
        .filter(pl.col(value_col) != pl.col("prev_val"))
        .with_columns(
            next_ts=pl.col(ts_col).shift(-1).over(tag_col),
        )
        .filter(pl.col(value_col) == target_value)
        .select(
            pl.col(tag_col),
            pl.col(ts_col).alias("Start"),
            pl.col("next_ts").alias("End"),
        )
    )
    if batch_end is not None:
        intervals = intervals.with_columns(
            pl.col("End").fill_null(batch_end)
        )
    return intervals


def filter_by_mode(
    pv_df: pl.DataFrame,
    mode_df: pl.DataFrame,
    allowed_modes: Sequence[str] = ("Production",),
    *,
    mode_col: str = "StateName",
) -> pl.DataFrame:
    """Keep only readings where the HMI operating mode is in *allowed_modes*.

    Uses a backward ``join_asof`` to align the most recent mode state with
    each process-value timestamp.  Falls back to unfiltered data if *mode_df*
    is empty or has no non-null mode values.

    .. note::
        String-type PI tags store their text in the ``StateName`` column
        (not ``Value``), which is preserved by the DuckDB cache.

    Args:
        pv_df: Process-value DataFrame (must have a ``Timestamp`` column).
        mode_df: Mode-tag DataFrame with ``Timestamp`` and *mode_col*.
        allowed_modes: Tuple/list of mode names to keep.
        mode_col: Column in *mode_df* containing the mode string.

    Returns:
        Filtered copy of *pv_df* with only rows matching *allowed_modes*.
    """
    if mode_df.is_empty():
        return pv_df

    _mode = (
        mode_df
        .sort("Timestamp")
        .select(
            pl.col("Timestamp").alias("ts_mode"),
            pl.col(mode_col).alias("Mode"),
        )
        .filter(pl.col("Mode").is_not_null())
    )
    if _mode.is_empty():
        return pv_df

    pv_sorted = pv_df.sort("Timestamp")
    pv_sorted = pv_sorted.join_asof(
        _mode, left_on="Timestamp", right_on="ts_mode", strategy="backward"
    )
    return pv_sorted.filter(pl.col("Mode").is_in(list(allowed_modes)))


def filter_by_mode_and_state(
    pv_df: pl.DataFrame,
    mode_df: pl.DataFrame,
    state_df: pl.DataFrame,
    *,
    state_active_value: float = 1.0,
    allowed_modes: Sequence[str] = ("Production",),
    mode_col: str = "StateName",
) -> pl.DataFrame:
    """Keep readings where mode matches AND a secondary state signal is active.

    Applies :func:`filter_by_mode` first, then further restricts to rows where
    *state_df* indicates the process is in the target state (e.g. dosing
    active).  Falls back to mode-only filtering if the state filter empties
    the result.

    Args:
        pv_df: Process-value DataFrame.
        mode_df: Mode-tag DataFrame.
        state_df: Secondary state-tag DataFrame (e.g. dosing active signal)
            with ``Timestamp`` and ``Value`` columns.
        state_active_value: The numeric value indicating the "active" state.
        allowed_modes: Mode names to keep (passed to :func:`filter_by_mode`).
        mode_col: Column in *mode_df* containing the mode string.

    Returns:
        Filtered copy of *pv_df*.
    """
    pv_filtered = filter_by_mode(pv_df, mode_df, allowed_modes, mode_col=mode_col)

    if state_df.is_empty():
        return pv_filtered

    _state = (
        state_df
        .sort("Timestamp")
        .select(
            pl.col("Timestamp").alias("ts_state"),
            (pl.col("Value").cast(pl.Float64, strict=False) == state_active_value)
            .alias("StateActive"),
        )
    )

    pv_sorted = pv_filtered.sort("Timestamp")
    pv_sorted = pv_sorted.join_asof(
        _state, left_on="Timestamp", right_on="ts_state", strategy="backward"
    )
    result = pv_sorted.filter(pl.col("StateActive") == True)  # noqa: E712

    # Fall back to mode-only if state filter removes everything
    if result.is_empty():
        return pv_filtered
    return result
