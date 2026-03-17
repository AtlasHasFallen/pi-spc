# pi-spc

OSIsoft PI AF SDK wrapper with DuckDB caching and SPC control charts.

## What it does

- **`pi_spc.pi`** — Pythonic wrapper around the OSIsoft AF SDK (.NET via pythonnet): connect to PI, search event frames, bulk-fetch recorded/interpolated values
- **`pi_spc.cache`** — Smart DuckDB cache layer for PI time-series data: automatic fetch-on-miss, partial-overlap detection, tag-level cache management
- **`pi_spc.viz`** — Statistical Process Control charts built on Altair: X̄-S, I-MR, Pareto, batch timeline, and stem-level layout
- **`pi_spc.stats`** — Robust descriptive statistics (Bowley skewness) for detecting distributional shifts
- **`pi_spc.transforms`** — Data transformations: state-change → interval conversion, mode/state filtering via asof joins
- **`pi_spc.utils`** — General helpers: human-readable timedelta formatting

## Prerequisites

- **Windows** with the OSIsoft AF SDK installed (`OSIsoft.AFSDK.dll`)
- Python ≥ 3.10

## Installation

```bash
pip install pi-spc
```

Or from source:

```bash
pip install git+https://github.com/joshuagorton/pi-spc.git
```

## Quick Start

```python
from pi_spc import PI, PICache, imr_chart

# Connect to PI and fetch data
with PI.connect() as (af_sys, af_db, pi_srv):
    frames = PI.search_event_frames(
        af_db, template="Your Template",
        start="*-30d", end="*",
        attributes=["Attribute1", "Attribute2"],
    )

# Cache time-series data locally
cache = PICache("my_store.duckdb")
with PI.connect() as (af_sys, af_db, pi_srv):
    data = cache.get_recorded_values(pi_srv, ["Tag1", "Tag2"], start="*-7d", end="*")

# Plot an I-MR control chart
chart = imr_chart(data, x_col="Timestamp", y_col="Value", title="My Process Variable")
chart.display()
```

## Modules

### `pi_spc.pi` — PI AF SDK Wrapper

| Function | Description |
|---|---|
| `connect(database_name=None)` | Context manager → `(PISystem, AFDatabase, PIServer)` |
| `search_event_frames(...)` | Query event frames by template, time range, attributes |
| `search_tags(...)` | Find PI tags by wildcard pattern |
| `get_recorded_values_bulk(...)` | Bulk fetch recorded values for multiple tags |
| `get_interpolated_values(...)` | Fixed-interval interpolation |
| `get_plot_values(...)` | Exception-based thinning for visualization |

### `pi_spc.cache` — DuckDB Cache

| Function | Description |
|---|---|
| `PICache(db_path)` | Initialize cache with DuckDB file |
| `.get_recorded_values(pi_server, tags, start, end)` | Smart fetch: cache hit → return, miss → fetch + store + return |
| `.cache_event_frames(df)` | Store event frame results |
| `.get_event_frames()` | Retrieve cached frames |
| `.cache_info()` | Show cached tag ranges |
| `.clear_cache(tag_names)` | Remove cached data for specific tags |
| `.prune_old_data(days)` | Delete data older than N days |

### `pi_spc.viz` — SPC Charts

| Function | Description |
|---|---|
| `imr_chart(df, ...)` | Individuals & Moving Range chart with optional spec limits and distribution panel |
| `xbar_s_chart(df, ...)` | X̄-S chart for batch-level subgroup statistics |
| `pareto_chart(df, ...)` | Pareto bar chart with cumulative percentage line |
| `batch_timeline(df, ...)` | Interactive Gantt-style batch timeline |
| `assign_stem_levels(midpoints, ...)` | Greedy stem-level layout for lollipop/timeline charts |

### `pi_spc.stats` — Statistics

| Function | Description |
|---|---|
| `bowley_skewness(values, ...)` | Quartile-based skewness — robust to outliers, detects asymmetry in the middle 50% |

### `pi_spc.transforms` — Data Transformations

| Function | Description |
|---|---|
| `state_to_intervals(df, ...)` | Convert state-change signals (0→1→0) into `(Start, End)` time intervals |
| `filter_by_mode(pv_df, mode_df, ...)` | Filter process data to rows matching allowed HMI modes via backward asof join |
| `filter_by_mode_and_state(pv_df, mode_df, state_df, ...)` | Dual-gate filter: mode + secondary state signal, with fallback |

### `pi_spc.utils` — Utilities

| Function | Description |
|---|---|
| `format_timedelta(td)` | Human-readable timedelta string (e.g. `"2 days 3 hours 15 mins"`) |

## Configuration

### AF SDK Path

The AF SDK DLL is auto-discovered from standard install locations. Override with:

```python
import os
os.environ["AFSDK_PATH"] = r"D:\Custom\Path\OSIsoft.AFSDK.dll"
```

## License

MIT
