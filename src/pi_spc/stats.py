"""Statistical functions for SPC analysis.

Provides skewness measures and other descriptive statistics useful for
detecting distributional shifts in process data.
"""

from __future__ import annotations

from typing import Sequence

__all__ = ["bowley_skewness"]


def bowley_skewness(
    values: Sequence[float],
    *,
    interpolation: str = "linear",
    min_count: int = 4,
) -> float | None:
    """Compute the Bowley (quartile) skewness coefficient.

    Bowley skewness is robust to outliers and sensitive to asymmetry in the
    middle 50% of the distribution.  It ranges from -1 (left-skewed) to +1
    (right-skewed), with 0 indicating symmetry.

    .. math::

        S_B = \\frac{Q_3 - 2 Q_2 + Q_1}{Q_3 - Q_1}

    Args:
        values: Numeric sequence (list, Polars Series, NumPy array, etc.).
            Polars Series are handled natively; other iterables are converted.
        interpolation: Quantile interpolation method (passed through to
            ``polars.Series.quantile``).  One of ``"linear"``, ``"lower"``,
            ``"higher"``, ``"nearest"``, ``"midpoint"``.
        min_count: Minimum number of values required to compute skewness.
            Returns ``None`` if fewer values are available.

    Returns:
        Bowley skewness as a float, or ``None`` if insufficient data or
        the interquartile range is zero.

    Example::

        >>> from pi_spc.stats import bowley_skewness
        >>> bowley_skewness([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        0.0
        >>> bowley_skewness([1, 1, 1, 2, 5, 8, 12, 20])  # right-skewed
        0.5
    """
    import polars as pl

    if isinstance(values, pl.Series):
        s = values.drop_nulls()
    else:
        s = pl.Series(list(values)).drop_nulls()

    if s.len() < min_count:
        return None

    q1 = float(s.quantile(0.25, interpolation=interpolation))
    q2 = float(s.quantile(0.50, interpolation=interpolation))
    q3 = float(s.quantile(0.75, interpolation=interpolation))
    iqr = q3 - q1

    if iqr == 0:
        return 0.0

    return (q3 - 2 * q2 + q1) / iqr
