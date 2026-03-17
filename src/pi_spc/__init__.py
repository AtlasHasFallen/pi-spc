"""pi-spc: OSIsoft PI AF SDK wrapper with DuckDB caching and SPC control charts.

Modules:
    pi_spc.pi         — PI AF SDK connection, event frames, tag queries, data retrieval
    pi_spc.cache      — DuckDB-backed smart cache for PI time-series data
    pi_spc.viz        — SPC control charts (I-MR, X̄-S, Pareto, timeline) built on Altair
    pi_spc.stats      — Statistical functions (Bowley skewness, etc.)
    pi_spc.transforms — Data transformations (state intervals, mode filtering)
    pi_spc.utils      — General-purpose helpers (timedelta formatting, etc.)
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
from pi_spc.stats import bowley_skewness
from pi_spc.transforms import (
    filter_by_mode,
    filter_by_mode_and_state,
    state_to_intervals,
)
from pi_spc.utils import format_timedelta
from pi_spc.viz import (
    IMR_SIGMA_MULTIPLIER,
    MR_UCL_MULTIPLIER,
    S_UCL_MULTIPLIER,
    assign_stem_levels,
    batch_timeline,
    imr_chart,
    pareto_chart,
    stem_timeline,
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
    # stats
    "bowley_skewness",
    # transforms
    "state_to_intervals",
    "filter_by_mode",
    "filter_by_mode_and_state",
    # utils
    "format_timedelta",
    # viz
    "pareto_chart",
    "batch_timeline",
    "stem_timeline",
    "xbar_s_chart",
    "imr_chart",
    "assign_stem_levels",
    "IMR_SIGMA_MULTIPLIER",
    "MR_UCL_MULTIPLIER",
    "S_UCL_MULTIPLIER",
]

__version__ = "0.1.0"
