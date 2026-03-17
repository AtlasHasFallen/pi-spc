"""Pythonic wrapper around the OSIsoft PI AF SDK.

Provides high-level functions for connecting to PI AF, searching event frames,
discovering PI tags, and retrieving recorded / interpolated / plot data as
Polars DataFrames.  Requires the OSIsoft AF SDK (AFSDK) to be installed on the
host machine.  The DLL is discovered automatically from standard install paths
or via the ``AFSDK_PATH`` environment variable.
"""

import os
import warnings
from contextlib import contextmanager
from datetime import datetime

import polars as pl

__all__ = [
    # Public API
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
    # Internal helpers re-exported for pi_spc.cache
    "_to_aftime",
    "_to_datetime",
    "_extract_af_value",
]

# ---------------------------------------------------------------------------
# Lazy SDK loading — allows import on non-Windows machines for docs / typing
# ---------------------------------------------------------------------------
_sdk_loaded = False

_AFSDK_SEARCH_PATHS = [
    r"C:\Program Files (x86)\PIPC\AF\PublicAssemblies\4.0",
    r"C:\Program Files\PIPC\AF\PublicAssemblies\4.0",
]

_DEFAULT_TAG_ATTRS = None  # populated by _ensure_sdk()


def _find_afsdk_dll():
    """Locate ``OSIsoft.AFSDK.dll``, checking *AFSDK_PATH* then known paths."""
    env = os.environ.get("AFSDK_PATH")
    if env:
        p = os.path.join(env, "OSIsoft.AFSDK.dll") if os.path.isdir(env) else env
        if os.path.isfile(p):
            return p
        raise FileNotFoundError(
            f"AFSDK_PATH is set to '{env}' but OSIsoft.AFSDK.dll was not found there."
        )
    for directory in _AFSDK_SEARCH_PATHS:
        p = os.path.join(directory, "OSIsoft.AFSDK.dll")
        if os.path.isfile(p):
            return p
    raise FileNotFoundError(
        "OSIsoft.AFSDK.dll not found. Install the PI AF SDK or set the AFSDK_PATH "
        "environment variable to the directory containing the DLL (or the full path)."
    )


def _ensure_sdk():
    """Load the AF SDK CLR references on first call.  No-op thereafter."""
    global _sdk_loaded, _DEFAULT_TAG_ATTRS
    if _sdk_loaded:
        return

    import clr  # pythonnet

    clr.AddReference(_find_afsdk_dll())

    # Inject .NET types into module globals so downstream code is unchanged.
    global PISystems, AFSearchMode, AFBoundaryType, CPA
    global PIPagingConfiguration, PIPageType, PIPoint, PIPointList, PIServers
    global AFEventFrameSearch, AFSearchTextOption
    global AFTime, AFTimeRange, AFTimeSpan
    global List, String

    from OSIsoft.AF import PISystems as _PISystems
    from OSIsoft.AF.Asset import AFSearchMode as _AFSearchMode
    from OSIsoft.AF.Data import AFBoundaryType as _AFBoundaryType
    from OSIsoft.AF.PI import PICommonPointAttributes as _CPA
    from OSIsoft.AF.PI import PIPagingConfiguration as _PIPagingConfiguration
    from OSIsoft.AF.PI import PIPageType as _PIPageType
    from OSIsoft.AF.PI import PIPoint as _PIPoint
    from OSIsoft.AF.PI import PIPointList as _PIPointList
    from OSIsoft.AF.PI import PIServers as _PIServers
    from OSIsoft.AF.Search import AFEventFrameSearch as _AFEventFrameSearch
    from OSIsoft.AF.Search import AFSearchTextOption as _AFSearchTextOption
    from OSIsoft.AF.Time import AFTime as _AFTime
    from OSIsoft.AF.Time import AFTimeRange as _AFTimeRange
    from OSIsoft.AF.Time import AFTimeSpan as _AFTimeSpan
    from System import String as _String
    from System.Collections.Generic import List as _List

    PISystems = _PISystems
    AFSearchMode = _AFSearchMode
    AFBoundaryType = _AFBoundaryType
    CPA = _CPA
    PIPagingConfiguration = _PIPagingConfiguration
    PIPageType = _PIPageType
    PIPoint = _PIPoint
    PIPointList = _PIPointList
    PIServers = _PIServers
    AFEventFrameSearch = _AFEventFrameSearch
    AFSearchTextOption = _AFSearchTextOption
    AFTime = _AFTime
    AFTimeRange = _AFTimeRange
    AFTimeSpan = _AFTimeSpan
    List = _List
    String = _String

    _DEFAULT_TAG_ATTRS = [
        CPA.Descriptor,
        CPA.EngineeringUnits,
        CPA.PointSource,
        CPA.PointType,
        CPA.Span,
        CPA.Zero,
        CPA.Step,
        CPA.Compressing,
        CPA.ExceptionDeviation,
    ]

    _sdk_loaded = True


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------
def _to_net_string_list(py_list):
    """Convert a Python list of strings to a .NET List<String>."""
    net_list = List[String]()
    for s in py_list:
        net_list.Add(s)
    return net_list


def _to_aftime(t):
    """Convert a string or Python datetime to AFTime."""
    _ensure_sdk()
    if isinstance(t, datetime):
        return AFTime(t.strftime("%Y-%m-%dT%H:%M:%S.%f"))
    return AFTime(t)


def _to_time_range(start, end):
    """Convert start/end (str or datetime) to an AFTimeRange."""
    return AFTimeRange(_to_aftime(start), _to_aftime(end))


def _to_datetime(af_time):
    """Convert an AFTime to a Python datetime."""
    t = af_time.LocalTime
    return datetime(
        t.Year, t.Month, t.Day, t.Hour, t.Minute, t.Second, t.Millisecond * 1000
    )


def _is_enumeration_value(val):
    """Check if a value is an AFEnumerationValue (digital state)."""
    return type(val).__name__ == "AFEnumerationValue"


_NO_DATA_SENTINELS = frozenset({"No Data", "No data", "Shutdown", "Pt Created"})


def _coerce_value(val):
    """Pass through Python-native types; extract .Name from digital states."""
    if val is None:
        return None
    if isinstance(val, str):
        return None if val in _NO_DATA_SENTINELS else val
    if isinstance(val, (int, float, bool)):
        return val
    if _is_enumeration_value(val):
        return val.Name
    return str(val)


def _extract_af_value(af_val):
    """Extract (numeric_value, state_name) from an AFValue's .Value.

    For digital (AFEnumerationValue): returns (int_code, state_name_str).
    For analog: returns (numeric_value, None).
    For bad quality / unrecognised: returns (None, None).
    """
    raw = af_val.Value
    if _is_enumeration_value(raw):
        return (int(raw.Value), str(raw.Name))
    if isinstance(raw, (int, float)):
        return (raw, None)
    if isinstance(raw, bool):
        return (int(raw), None)
    # Fallback — try numeric conversion, then accept string
    try:
        return (float(raw), None)
    except (TypeError, ValueError):
        pass
    # String tag values — store text in StateName so Value stays numeric-compatible
    try:
        s = str(raw)
        if s:
            return (None, s)
    except Exception:
        pass
    return (None, None)


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------
@contextmanager
def connect(database_name=None):
    """Connect to the default PI AF system and yield (PISystem, AFDatabase, PIServer).

    Usage:
        with connect() as (system, db, server):
            df = search_event_frames(db, "My Template")
    """
    _ensure_sdk()
    af_system = PISystems().DefaultPISystem
    if database_name:
        af_db = af_system.Databases[database_name]
    else:
        af_db = af_system.Databases.DefaultDatabase
    pi_server = PIServers().DefaultPIServer
    pi_server.Connect()
    print(f"AF: {af_system.Name} | DB: {af_db.Name} | DA: {pi_server.Name}")
    try:
        yield af_system, af_db, pi_server
    finally:
        pi_server.Disconnect()
        print(f"Disconnected from {pi_server.Name}")


# ---------------------------------------------------------------------------
# (1) Event frame search → Polars DataFrame
# ---------------------------------------------------------------------------
def search_event_frames(
    af_db,
    template,
    start="*-30d",
    end="*",
    query="",
    attributes=None,
    max_count=10000,
):
    """Search for event frames by template and return a Polars DataFrame.

    Args:
        af_db: AFDatabase instance.
        template: Event frame template name (e.g. "Syringe Fill").
        start: Start time string (default "*-30d").
        end: End time string (default "*").
        query: Additional query filter appended after the template filter.
        attributes: List of attribute names to include as columns.
            If None, no attributes are extracted.
        max_count: Maximum number of results.

    Returns:
        pl.DataFrame with Name, StartTime, EndTime, and requested attributes.
    """
    _ensure_sdk()
    full_query = f"Template:'{template}'"
    if query:
        full_query += f" {query}"

    search = AFEventFrameSearch(
        af_db,
        "EFSearch",
        AFSearchMode.Overlapped,
        _to_aftime(start),
        _to_aftime(end),
        full_query,
    )

    rows = []
    for ef in search.FindEventFrames(0, True, max_count):
        row = {
            "Name": ef.Name,
            "StartTime": _to_datetime(ef.StartTime),
            "EndTime": _to_datetime(ef.EndTime),
        }
        if attributes:
            for attr_name in attributes:
                try:
                    row[attr_name] = _coerce_value(
                        ef.Attributes[attr_name].GetValue().Value
                    )
                except Exception:
                    row[attr_name] = None
        rows.append(row)

    if len(rows) >= max_count:
        warnings.warn(
            f"search_event_frames: returned {len(rows)} rows (max_count={max_count}). "
            "Results may be truncated — increase max_count to retrieve all data.",
            stacklevel=2,
        )

    return pl.DataFrame(rows, infer_schema_length=None)


# ---------------------------------------------------------------------------
# (2) Inspect event frame attributes
# ---------------------------------------------------------------------------
def inspect_event_frame(af_db, template, index=0, start="*-365d", end="*"):
    """Print all attributes and their values for a single event frame.

    Args:
        af_db: AFDatabase instance.
        template: Event frame template name.
        index: Which event frame to inspect (0 = first match).
        start: Start time string.
        end: End time string.

    Returns:
        dict of {attribute_name: value} for the inspected event frame.
    """
    _ensure_sdk()
    search = AFEventFrameSearch(
        af_db,
        "Inspect",
        AFSearchMode.Overlapped,
        _to_aftime(start),
        _to_aftime(end),
        f"Template:'{template}'",
    )

    for i, ef in enumerate(search.FindEventFrames(0, True, index + 1)):
        if i < index:
            continue
        print(f"Event Frame: {ef.Name}")
        print(f"  Start: {ef.StartTime}  End: {ef.EndTime}")
        print(f"  Template: {ef.Template.Name if ef.Template else '(none)'}")
        print(f"  Attributes ({ef.Attributes.Count}):")
        attrs = {}
        for attr in ef.Attributes:
            try:
                val = attr.GetValue().Value
            except Exception:
                val = "(error)"
            attrs[attr.Name] = val
            print(f"    {attr.Name} = {val}")
        return attrs

    print("No event frame found.")
    return {}


# ---------------------------------------------------------------------------
# (3) Search for PI tags
# ---------------------------------------------------------------------------
def search_tags(pi_server, name_filter, source_filter=None, max_results=100):
    """Search for PI points (tags) by name pattern.

    Args:
        pi_server: PIServer instance.
        name_filter: Wildcard name filter (e.g. "SFM*", "*Temperature*").
        source_filter: Optional point source filter.
        max_results: Maximum number of results to return.

    Returns:
        pl.DataFrame with Name and PointType columns.
    """
    _ensure_sdk()
    points = PIPoint.FindPIPoints(pi_server, name_filter, source_filter, None)

    rows = []
    for i, pt in enumerate(points):
        if i >= max_results:
            break
        rows.append({"Name": pt.Name, "PointType": str(pt.PointType)})

    return pl.DataFrame(rows)


def search_tags_by_query(pi_server, query, max_results=100):
    """Search for PI points using a descriptor/name query string.

    Args:
        pi_server: PIServer instance.
        query: Search text matched against name and descriptor.
        max_results: Maximum number of results to return.

    Returns:
        pl.DataFrame with Name and PointType columns.
    """
    _ensure_sdk()
    points = PIPoint.FindPIPoints(
        pi_server, query, True, None, AFSearchTextOption.Contains
    )

    rows = []
    for i, pt in enumerate(points):
        if i >= max_results:
            break
        rows.append({"Name": pt.Name, "PointType": str(pt.PointType)})

    return pl.DataFrame(rows)


def get_tag_attributes(pi_server, tag_names, attributes=None):
    """Get metadata attributes for one or more PI tags.

    Uses bulk FindPIPoints + PIPointList.LoadAttributes for speed.

    Args:
        pi_server: PIServer instance.
        tag_names: Single tag name (str) or list of tag names.
        attributes: List of attribute name strings to retrieve.
            Defaults to descriptor, engunits, pointsource, pointtype,
            span, zero, step, compressing, excdev.
            Use PICommonPointAttributes constants or raw strings like
            "descriptor", "engunits", etc.

    Returns:
        pl.DataFrame with Name plus one column per attribute.
    """
    _ensure_sdk()
    if isinstance(tag_names, str):
        tag_names = [tag_names]

    attrs = attributes or _DEFAULT_TAG_ATTRS

    # Bulk resolve all tag names in one RPC
    net_names = _to_net_string_list(tag_names)
    net_attrs = _to_net_string_list(attrs)
    points = PIPoint.FindPIPoints(pi_server, net_names, net_attrs)

    # Bulk load attributes in one RPC
    pt_list = PIPointList()
    pt_list.AddRange(points)
    pt_list.LoadAttributes(attrs)

    rows = []
    for pt in pt_list:
        row = {"Name": pt.Name}
        for a in attrs:
            row[a] = _coerce_value(pt.GetAttribute(a))
        rows.append(row)

    return pl.DataFrame(rows)


# ---------------------------------------------------------------------------
# (4) Query tag data → Polars DataFrame
# ---------------------------------------------------------------------------
def get_recorded_values(pi_server, tag_names, start="*-1d", end="*", max_count=10000):
    """Get recorded (raw) values for one or more tags.

    Args:
        pi_server: PIServer instance.
        tag_names: Single tag name (str) or list of tag names.
        start: Start time string.
        end: End time string.
        max_count: Max values per tag.

    Returns:
        pl.DataFrame with Timestamp, Tag, Value, StateName, and IsGood columns.
        Value is numeric (int for digital states, float for analog).
        StateName is the digital state label (e.g. "OK", "True") or null for analog tags.
    """
    _ensure_sdk()
    if isinstance(tag_names, str):
        tag_names = [tag_names]

    time_range = _to_time_range(start, end)
    rows = []

    for name in tag_names:
        pt = PIPoint.FindPIPoint(pi_server, name)
        values = pt.RecordedValues(
            time_range, AFBoundaryType.Inside, "", False, max_count
        )
        tag_count = 0
        for v in values:
            numeric, state = _extract_af_value(v) if v.IsGood else (None, None)
            rows.append(
                {
                    "Timestamp": _to_datetime(v.Timestamp),
                    "Tag": name,
                    "Value": numeric,
                    "StateName": state,
                    "IsGood": v.IsGood,
                }
            )
            tag_count += 1
        if tag_count >= max_count:
            warnings.warn(
                f"get_recorded_values: tag '{name}' returned {tag_count} rows "
                f"(max_count={max_count}). Results may be truncated.",
                stacklevel=2,
            )

    return pl.DataFrame(rows)


def get_recorded_values_bulk(
    pi_server, tag_names, start="*-1d", end="*", max_count=10000, page_size=100
):
    """Get recorded values for many tags in a single bulk RPC call.

    Significantly faster than get_recorded_values for large tag lists because
    the server handles all tags in one request instead of N sequential calls.

    Args:
        pi_server: PIServer instance.
        tag_names: List of tag name strings.
        start: Start time (str or datetime).
        end: End time (str or datetime).
        max_count: Max values per tag.
        page_size: Number of tags per server-side page (tune for memory vs speed).

    Returns:
        pl.DataFrame with Timestamp, Tag, Value, StateName, and IsGood columns.
    """
    _ensure_sdk()
    if isinstance(tag_names, str):
        tag_names = [tag_names]

    pt_list = PIPointList()
    for name in tag_names:
        pt_list.Add(PIPoint.FindPIPoint(pi_server, name))

    time_range = _to_time_range(start, end)
    paging = PIPagingConfiguration(PIPageType.TagCount, page_size)

    rows = []
    for af_values in pt_list.RecordedValues(
        time_range, AFBoundaryType.Inside, "", False, paging, max_count
    ):
        tag_name = af_values.PIPoint.Name
        tag_count = 0
        for v in af_values:
            numeric, state = _extract_af_value(v) if v.IsGood else (None, None)
            rows.append(
                {
                    "Timestamp": _to_datetime(v.Timestamp),
                    "Tag": tag_name,
                    "Value": numeric,
                    "StateName": state,
                    "IsGood": v.IsGood,
                }
            )
            tag_count += 1
        if tag_count >= max_count:
            warnings.warn(
                f"get_recorded_values_bulk: tag '{tag_name}' returned {tag_count} rows "
                f"(max_count={max_count}). Results may be truncated.",
                stacklevel=2,
            )

    if not rows:
        return pl.DataFrame({"Timestamp": [], "Tag": [], "Value": [], "IsGood": []})
    return pl.DataFrame(rows)


def get_interpolated_values(pi_server, tag_names, start="*-1d", end="*", interval="1h"):
    """Get interpolated values for one or more tags at a fixed interval.

    Args:
        pi_server: PIServer instance.
        tag_names: Single tag name (str) or list of tag names.
        start: Start time string.
        end: End time string.
        interval: Interval string (e.g. "1h", "10m", "1d").

    Returns:
        pl.DataFrame with Timestamp, Tag, Value, and StateName columns.
        Value is numeric (int for digital states, float for analog).
        StateName is the digital state label or null for analog tags.
    """
    _ensure_sdk()
    if isinstance(tag_names, str):
        tag_names = [tag_names]

    time_range = _to_time_range(start, end)
    span = AFTimeSpan.Parse(interval)
    rows = []

    for name in tag_names:
        pt = PIPoint.FindPIPoint(pi_server, name)
        values = pt.InterpolatedValues(time_range, span, "", False)
        for v in values:
            numeric, state = _extract_af_value(v) if v.IsGood else (None, None)
            rows.append(
                {
                    "Timestamp": _to_datetime(v.Timestamp),
                    "Tag": name,
                    "Value": numeric,
                    "StateName": state,
                }
            )

    return pl.DataFrame(rows)


def get_plot_values(pi_server, tag_names, start="*-1d", end="*", intervals=300):
    """Get plot values (exception-based thinning) for one or more tags.

    Args:
        pi_server: PIServer instance.
        tag_names: Single tag name (str) or list of tag names.
        start: Start time string.
        end: End time string.
        intervals: Number of plot intervals.

    Returns:
        pl.DataFrame with Timestamp, Tag, Value, and StateName columns.
        Value is numeric (int for digital states, float for analog).
        StateName is the digital state label or null for analog tags.
    """
    _ensure_sdk()
    if isinstance(tag_names, str):
        tag_names = [tag_names]

    time_range = _to_time_range(start, end)
    rows = []

    for name in tag_names:
        pt = PIPoint.FindPIPoint(pi_server, name)
        values = pt.PlotValues(time_range, intervals)
        for v in values:
            numeric, state = _extract_af_value(v) if v.IsGood else (None, None)
            rows.append(
                {
                    "Timestamp": _to_datetime(v.Timestamp),
                    "Tag": name,
                    "Value": numeric,
                    "StateName": state,
                }
            )

    return pl.DataFrame(rows)
