"""pi-spc: OSIsoft PI AF SDK wrapper with DuckDB caching and SPC control charts.

Modules:
    pi_spc.pi      — PI AF SDK connection, event frames, tag queries, data retrieval
    pi_spc.cache   — DuckDB-backed smart cache for PI time-series data
    pi_spc.viz     — SPC control charts (I-MR, X̄-S, Pareto, timeline) built on Altair
"""

from pi_spc.cache import PICache
from pi_spc.pi import (
    connect,
    get_interpolated_values,
    get_plot_values,
    get_recorded_values,
    get_recorded_values_bulk,
    get_tag_attributes,
    inspect_event_frame,
    search_event_frames,
    search_tags,
    search_tags_by_query,
)
from pi_spc.viz import (
    IMR_SIGMA_MULTIPLIER,
    MR_UCL_MULTIPLIER,
    S_UCL_MULTIPLIER,
    batch_timeline,
    imr_chart,
    pareto_chart,
    xbar_s_chart,
)

__all__ = [
    # pi module
    "connect",
    "search_event_frames",
    "inspect_event_frame",
    "search_tags",
    "search_tags_by_query",
    "get_tag_attributes",
    "get_recorded_values",
    "get_recorded_values_bulk",
    "get_interpolated_values",
    "get_plot_values",
    # cache
    "PICache",
    # viz
    "pareto_chart",
    "batch_timeline",
    "xbar_s_chart",
    "imr_chart",
    "IMR_SIGMA_MULTIPLIER",
    "MR_UCL_MULTIPLIER",
    "S_UCL_MULTIPLIER",
]

__version__ = "0.1.0"
