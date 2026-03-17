"""General-purpose utility functions.

Small helpers that don't fit neatly into the stats, transforms, or viz modules.
"""

from __future__ import annotations

from datetime import timedelta

__all__ = ["format_timedelta"]


def format_timedelta(td: timedelta) -> str:
    """Format a timedelta into a concise human-readable string.

    Handles negative durations and omits zero-value units.  Always includes
    seconds if no larger unit is present.

    Args:
        td: A :class:`datetime.timedelta` object.

    Returns:
        A string like ``"2 days 3 hours 15 mins 30 secs"`` or ``"-45 mins"``.

    Raises:
        TypeError: If *td* is not a :class:`datetime.timedelta`.

    Example::

        >>> from pi_spc.utils import format_timedelta
        >>> from datetime import timedelta
        >>> format_timedelta(timedelta(hours=2, minutes=15))
        '2 hours 15 mins'
        >>> format_timedelta(timedelta(seconds=0))
        '0 secs'
    """
    if not isinstance(td, timedelta):
        raise TypeError("Input must be a datetime.timedelta object")

    total_seconds = int(td.total_seconds())
    sign = "-" if total_seconds < 0 else ""
    total_seconds = abs(total_seconds)

    days, remainder = divmod(total_seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)

    parts = []
    if days:
        parts.append(f"{days} days")
    if hours:
        parts.append(f"{hours} hours")
    if minutes:
        parts.append(f"{minutes} mins")
    if seconds or not parts:
        parts.append(f"{seconds} secs")

    return sign + " ".join(parts)
