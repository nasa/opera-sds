"""
Tests / stubs for dswx_hls_duplicate_analysis.py

These are intended as a starting point. Some are simple smoke tests,
some assert specific behavior, and a few are structured so you can
easily extend them with more detailed checks.
"""

import datetime
import json

import pandas as pd
import pytest

import dswx_hls_duplicate_analysis as mod


# ---------------------------------------------------------------------------
# normalize_to_datetime
# ---------------------------------------------------------------------------

def test_normalize_to_datetime_with_datetime():
    dt = datetime.datetime(2025, 1, 2, 3, 4, 5)
    result = mod.normalize_to_datetime(dt)
    assert isinstance(result, datetime.datetime)
    assert result == dt


def test_normalize_to_datetime_with_date():
    """Date should become a datetime at midnight."""
    d = datetime.date(2025, 1, 2)
    result = mod.normalize_to_datetime(d)
    assert isinstance(result, datetime.datetime)
    assert (result.year, result.month, result.day) == (2025, 1, 2)
    # Intended semantics: normalized to midnight
    assert (result.hour, result.minute, result.second) == (0, 0, 0)


def test_normalize_to_datetime_with_iso_string_datetime():
    s = "2025-01-02T03:04:05"
    result = mod.normalize_to_datetime(s)
    assert isinstance(result, datetime.datetime)
    assert (result.year, result.month, result.day) == (2025, 1, 2)
    assert (result.hour, result.minute, result.second) == (3, 4, 5)


def test_normalize_to_datetime_with_iso_string_date():
    s = "2025-01-02"
    result = mod.normalize_to_datetime(s)
    assert isinstance(result, datetime.datetime)
    assert (result.year, result.month, result.day) == (2025, 1, 2)


def test_normalize_to_datetime_invalid_type_raises():
    with pytest.raises(ValueError):
        mod.normalize_to_datetime(12345)  # unsupported type


def test_normalize_to_datetime_invalid_string_raises():
    with pytest.raises(ValueError):
        mod.normalize_to_datetime("not-a-date")


# ---------------------------------------------------------------------------
# remove_landsat9_granules_error (legacy / buggy helper)
# ---------------------------------------------------------------------------

def test_remove_landsat9_granules_error_basic():
    """
    This tests the *intended* behavior of the older helper:
    filter out any entry whose third element contains 'LANDSAT-9'.
    """
    hls_granules = [
        ("g1", "meta1", "LANDSAT-9_some_id"),
        ("g2", "meta2", "LANDSAT-8_some_id"),
        ("g3", "meta3", "foo LANDSAT-9 bar"),
        ("g4", "meta4", "SENTINEL-2A"),
    ]

    filtered = mod.remove_landsat9_granules_error(hls_granules)

    # Only entries without LANDSAT-9 in the third field should remain
    assert ("g2", "meta2") in filtered
    assert ("g4", "meta4") in filtered
    # Ensure LANDSAT-9 strings were filtered out
    assert all("LANDSAT-9" not in g[1] for g in filtered)


# ---------------------------------------------------------------------------
# remove_landsat9_granules (current implementation)
# ---------------------------------------------------------------------------

def _make_hls_granule(native_id, platform_short_name):
    return {
        "meta": {"native-id": native_id},
        "umm": {
            "Platforms": [
                {"ShortName": platform_short_name},
            ]
        },
    }


def test_remove_landsat9_granules_filters_platform():
    granules = [
        _make_hls_granule("HLS_1", "LANDSAT-9"),
        _make_hls_granule("HLS_2", "LANDSAT-8"),
        _make_hls_granule("HLS_3", "SENTINEL-2A"),
    ]

    filtered = mod.remove_landsat9_granules(granules)

    # HLS_1 should be removed
    ids = [g["meta"]["native-id"] for g in filtered]
    assert "HLS_1" not in ids
    assert "HLS_2" in ids
    assert "HLS_3" in ids


def test_remove_landsat9_granules_empty_input():
    assert mod.remove_landsat9_granules([]) == []


# ---------------------------------------------------------------------------
# query_cmr_for_products
#
# This function talks to the external CMR API via GranuleQuery.
# We mock GranuleQuery so tests are deterministic and offline.
# ---------------------------------------------------------------------------

class FakeGranuleQuery:
    """
    Minimal fake GranuleQuery for testing that query_cmr_for_products uses
    the expected methods and returns data in the expected format.
    """

    def __init__(self):
        self.called = {
            "format": None,
            "short_name": None,
            "revision_date": None,
            "temporal": None,
            "polygon": None,
            "get_all": False,
        }

    def format(self, fmt):
        self.called["format"] = fmt

    def short_name(self, collection):
        self.called["short_name"] = collection

    def revision_date(self, date_from=None, date_to=None):
        self.called["revision_date"] = (date_from, date_to)

    def temporal(self, date_from=None, date_to=None):
        self.called["temporal"] = (date_from, date_to)

    def polygon(self, coords):
        self.called["polygon"] = coords

    def get_all(self):
        self.called["get_all"] = True
        # Return a single batch: JSON string with "items" list
        fake_items = {
            "items": [
                {
                    "meta": {"native-id": "G1"},
                    "umm": {"Platforms": [{"ShortName": "LANDSAT-8"}]},
                },
                {
                    "meta": {"native-id": "G2"},
                    "umm": {"Platforms": [{"ShortName": "SENTINEL-2A"}]},
                },
            ]
        }
        return [json.dumps(fake_items)]


@pytest.fixture
def patched_granule_query(monkeypatch):
    fake_instance = FakeGranuleQuery()

    def fake_constructor():
        return fake_instance

    monkeypatch.setattr(mod, "GranuleQuery", fake_constructor)
    return fake_instance


def test_query_cmr_for_products_requires_some_datetime_filters():
    with pytest.raises(ValueError):
        mod.query_cmr_for_products("OPERA_L3_DSWX-HLS_V1")


def test_query_cmr_for_products_sensor_and_revision_ok(patched_granule_query):
    granules = mod.query_cmr_for_products(
        "OPERA_L3_DSWX-HLS_V1",
        sensor_datetime_from="2025-01-01T00:00:00",
        sensor_datetime_to="2025-01-02T00:00:00",
        revision_datetime_from="2025-01-01T00:00:00",
        revision_datetime_to="2025-01-02T00:00:00",
    )

    assert isinstance(granules, list)
    assert len(granules) == 2
    assert all("meta" in g for g in granules)
    assert patched_granule_query.called["format"] == "umm_json"
    assert patched_granule_query.called["short_name"] == "OPERA_L3_DSWX-HLS_V1"
    assert patched_granule_query.called["get_all"] is True
    # We don't assert exact datetime values, but they should not be None
    rev_from, rev_to = patched_granule_query.called["revision_date"]
    sen_from, sen_to = patched_granule_query.called["temporal"]
    assert rev_from is not None and rev_to is not None
    assert sen_from is not None and sen_to is not None


@pytest.mark.parametrize(
    "flag_attr,expected_polygon",
    [
        ("north_america_flag", mod.NORTH_AMERICA_POLYGON),
        ("central_america_flag", mod.CENTRAL_AMERICA_POLYGON),
    ],
)
def test_query_cmr_for_products_polygon_flags(
    patched_granule_query, flag_attr, expected_polygon
):
    kwargs = {
        "collection": "OPERA_L2_RTC-S1_V1",
        "sensor_datetime_from": "2025-01-01T00:00:00",
        "sensor_datetime_to": "2025-01-02T00:00:00",
        "revision_datetime_from": "2025-01-01T00:00:00",
        "revision_datetime_to": "2025-01-02T00:00:00",
        flag_attr: True,
    }
    granules = mod.query_cmr_for_products(**kwargs)
    assert isinstance(granules, list)
    assert patched_granule_query.called["polygon"] == expected_polygon


def test_query_cmr_for_products_remove_landsat9_uses_filter(monkeypatch):
    """
    Verify that when remove_landsat9=True, the helper is invoked.
    We patch both GranuleQuery and remove_landsat9_granules.
    """
    fake_api = FakeGranuleQuery()

    def fake_constructor():
        return fake_api

    monkeypatch.setattr(mod, "GranuleQuery", fake_constructor)

    called = {"args": None}

    def fake_remove(granules):
        called["args"] = granules
        # return unchanged for this test
        return granules

    monkeypatch.setattr(mod, "remove_landsat9_granules", fake_remove)

    granules = mod.query_cmr_for_products(
        "HLSL30",
        sensor_datetime_from="2025-01-01T00:00:00",
        sensor_datetime_to="2025-01-02T00:00:00",
        remove_landsat9=True,
    )

    assert called["args"] is not None
    assert len(granules) == 2


# ---------------------------------------------------------------------------
# map_inputs_to_output
# ---------------------------------------------------------------------------

def _make_fake_hls_granule(native_id, revision_id, revision_date, platform):
    return {
        "meta": {
            "native-id": native_id,
            "revision-id": revision_id,
            "revision-date": revision_date,
        },
        "umm": {
            "Platforms": [
                {"ShortName": platform},
            ]
        },
    }


def _make_fake_dswx_granule(dswx_id, rev_date, input_product_native_id):
    """AdditionalAttributes[2].Values[0] holds the input HLS granule id."""
    return {
        "meta": {
            "native-id": dswx_id,
            "revision-date": rev_date,
        },
        "umm": {
            "AdditionalAttributes": [
                {},  # index 0
                {},  # index 1
                {"Values": [input_product_native_id]},  # index 2 used in code
            ]
        },
    }


def test_map_inputs_to_output_basic():
    # HLS inputs
    hlsl30_results = [
        _make_fake_hls_granule("HLS_1", "10", "2025-01-01T00:00:00Z", "LANDSAT-8"),
        _make_fake_hls_granule("HLS_2", "11", "2025-01-02T00:00:00Z", "LANDSAT-8"),
    ]
    hlss30_results = [
        _make_fake_hls_granule("HLS_3", "12", "2025-01-03T00:00:00Z", "SENTINEL-2A"),
    ]

    # DSWx products referencing HLS inputs
    dswx_hls_results = [
        _make_fake_dswx_granule("DSWX_A", "2025-02-01T00:00:00Z", "HLS_1"),
        _make_fake_dswx_granule("DSWX_B", "2025-02-02T00:00:00Z", "HLS_1"),
        _make_fake_dswx_granule("DSWX_C", "2025-02-03T00:00:00Z", "HLS_2"),
    ]

    df = mod.map_inputs_to_output(dswx_hls_results, hlsl30_results, hlss30_results)

    assert isinstance(df, pd.DataFrame)
    expected_cols = {
        "DSWx_ID",
        "DSWx_RevDate",
        "InputProduct",
        "InputRevId",
        "InputRevDate",
        "InputPlatform",
        "DSWx_Granule_Count",
    }
    assert expected_cols.issubset(df.columns)

    # HLS_1 appears twice, HLS_2 once
    counts_by_input = {}
    for input_prod, count in zip(df["InputProduct"], df["DSWx_Granule_Count"]):
        counts_by_input.setdefault(input_prod, count)

    assert counts_by_input["HLS_1"] == 2
    assert counts_by_input["HLS_2"] == 1


def test_map_inputs_to_output_handles_empty_inputs():
    df = mod.map_inputs_to_output([], [], [])
    assert isinstance(df, pd.DataFrame)
    assert df.empty


# ---------------------------------------------------------------------------
# process_dswx_by_sensing_date
# ---------------------------------------------------------------------------

def test_process_dswx_by_sensing_date_calls_query_and_map(monkeypatch):
    """
    Smoke test: ensure process_dswx_by_sensing_date calls query_cmr_for_products
    for the expected collections and passes through its outputs to
    map_inputs_to_output.
    """
    called_queries = []

    def fake_query(collection, **kwargs):
        called_queries.append((collection, kwargs))
        # just return a marker list so we can assert pass-through
        return [f"{collection}_granule"]

    monkeypatch.setattr(mod, "query_cmr_for_products", fake_query)

    def fake_map(dswx_results, hlsl30_results, hlss30_results):
        # Make sure the right lists are passed in
        assert dswx_results == ["OPERA_L3_DSWX-HLS_V1_granule"]
        assert hlsl30_results == ["HLSL30_granule"]
        assert hlss30_results == ["HLSS30_granule"]
        # Return a trivial dataframe
        return pd.DataFrame({"dummy": [1, 2, 3]})

    monkeypatch.setattr(mod, "map_inputs_to_output", fake_map)

    date_from = "2025-01-01T00:00:00"
    date_to = "2025-01-02T00:00:00"

    df = mod.process_dswx_by_sensing_date(date_from, date_to)

    assert isinstance(df, pd.DataFrame)
    # We expect three query calls: DSWX-HLS, HLSL30, HLSS30
    collections = [c for c, _ in called_queries]
    assert "OPERA_L3_DSWX-HLS_V1" in collections
    assert "HLSL30" in collections
    assert "HLSS30" in collections


def test_process_dswx_by_sensing_date_single_day(monkeypatch):
    """
    If date_to is None, the code should interpret date_from as a single day
    and extend by one day internally.
    This test mainly exercises that the function doesn't crash in that path.
    """
    def fake_query(collection, **kwargs):
        return []

    monkeypatch.setattr(mod, "query_cmr_for_products", fake_query)
    monkeypatch.setattr(
        mod, "map_inputs_to_output", lambda d, l30, s30: pd.DataFrame()
    )

    df = mod.process_dswx_by_sensing_date("2025-01-01")
    assert isinstance(df, pd.DataFrame)


# ---------------------------------------------------------------------------
# get_dates_between
# ---------------------------------------------------------------------------

def test_get_dates_between_inclusive():
    dates = mod.get_dates_between("2025-01-01", "2025-01-03")
    assert len(dates) == 3
    # Values should be datetime-like and inclusive
    assert dates[0].date() == datetime.date(2025, 1, 1)
    assert dates[-1].date() == datetime.date(2025, 1, 3)


def test_get_dates_between_single_day():
    dates = mod.get_dates_between("2025-01-01", "2025-01-01")
    assert len(dates) == 1
    assert dates[0].date() == datetime.date(2025, 1, 1)


# ---------------------------------------------------------------------------
# process_dswx_by_sensing_date_range
# ---------------------------------------------------------------------------

def test_process_dswx_by_sensing_date_range_smoke(monkeypatch):
    """
    process_dswx_by_sensing_date_range should:
    - Iterate over each date from get_dates_between
    - Call process_dswx_by_sensing_date once per date
    - Return the last DataFrame (or value) returned by process_dswx_by_sensing_date
    """
    # Controlled list of dates
    dates = [
        datetime.datetime(2025, 1, 1),
        datetime.datetime(2025, 1, 2),
    ]

    # Monkeypatch get_dates_between to return our controlled dates
    monkeypatch.setattr(mod, "get_dates_between", lambda start, end: dates)

    called_dates = []

    def fake_process(date, date_to=None):
        # Record which dates were passed in
        called_dates.append(date)
        # Return a distinct sentinel so we can see what comes back
        return f"df_for_{date.strftime('%Y%m%d')}"

    monkeypatch.setattr(mod, "process_dswx_by_sensing_date", fake_process)

    result = mod.process_dswx_by_sensing_date_range("2025-01-01", "2025-01-02")

    # It should have walked over each date in order
    assert called_dates == dates

    # It should return the last df returned by process_dswx_by_sensing_date
    assert result == "df_for_20250102"

    
