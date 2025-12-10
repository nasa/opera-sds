
import asyncio
from collections import defaultdict
import datetime
from dateutil.relativedelta import relativedelta
import functools
import json
import logging
import logging.handlers
import math
import os
import re
import statistics
import sys
import time
from time import gmtime, strftime
from typing import Union, Iterable
import urllib.parse

from cmr import GranuleQuery
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import matplotlib.lines as mlines
import numpy as np
import pandas as pd
from tqdm import tqdm


COLLECTIONS = ["HLSL30", "HLSS30", "OPERA_L3_DSWX-HLS_V1", "empty", "SENTINEL-1A_SLC", "OPERA_L2_RTC-S1_V1", "OPERA_L2_CSLC-S1_V1", "OPERA_L3_DSWX-S1_V1"]
# override to just do one for now
COLLECTIONS =  ["OPERA_L3_DSWX-S1_V1", "OPERA_L3_DSWX-HLS_V1", "OPERA_L2_RTC-S1_V1", "OPERA_L2_CSLC-S1_V1"]
# COLLECTIONS = ["OPERA_L2_RTC-S1_V1"]

NORTH_AMERICA_POLYGON = [[-81.84375, 5.01167], [-76.5, 12.32118], [-63.84375, 19.63068],
                         [-72.84375, 28.90813], [-67.78125, 37.6233], [-48.09375, 47.74415],
                         [-52.875, 57.58387], [-34.3125, 60.39521], [-25.59375, 67.42358],
                         [-13.5, 71.35947], [-8.4375, 82.60486], [-36.84375, 87.10301],
                         [-119.25, 82.60486], [-130.21875, 74.45195], [-168.75, 72.20287],
                         [-169.3125, 64.61224], [-167.44922, 61.97351], [-168.50391, 59.47844],
                         [-178.875, 52.85668], [-178.875, 49.90476], [-144.98438, 57.35483],
                         [-128.39063, 44.70377], [-119.25, 25.58661], [-97.3125, 8.7115],
                         [-81.84375, 5.01167]]

CENTRAL_AMERICA_POLYGON = [[-114.64453, 32.79634], [-117.35156, 32.62063], [-118.54688, 28.94831],
                           [-110.10938, 21.7965], [-108, 23.69416], [-106.59375, 18.91487],
                           [-87.96094, 11.11338], [-80.50781, 5.91239], [-78.25781, 6.26381],
                           [-76.79883, 8.43563], [-78.43359, 10.45628], [-82.21289, 10.59685],
                           [-82.01953, 14.30153], [-82.47656, 17.63308], [-86.625, 17.35194],
                           [-85.85156, 22.2718], [-95.76563, 25.99684], [-98.89453, 26.81794],
                           [-99.73828, 28.36418], [-101.32031, 30.12127], [-103.07813, 30.19155],
                           [-103.28906, 29.59414], [-104.37891, 29.94556], [-105.22266, 31.63237],
                           [-108.94922, 32.01893], [-110.91797, 31.59868], [-114.64453, 32.79634]]



def normalize_to_datetime(value):
    """
    Normalize a variety of date/time input types into a ``datetime.datetime``.

    Accepted input types
    --------------------
    * ``datetime.datetime`` – returned as-is.
    * ``datetime.date`` – promoted to a ``datetime.datetime`` at midnight.
    * ISO-8601 string – e.g. ``"2025-01-02"`` or ``"2025-01-02T03:04:05"``.

    Parameters
    ----------
    value : datetime.datetime | datetime.date | str
        The value to normalize.

    Returns
    -------
    datetime.datetime
        A naive ``datetime`` object representing the same moment or date.

    Raises
    ------
    ValueError
        If the value cannot be interpreted as a supported date/time type
        or if a string is not valid ISO-8601.
    """
    # Case 1: Already a datetime
    if isinstance(value, datetime.datetime):
        ret_value = value

    # Case 2: A date object (but not datetime)
    elif isinstance(value, datetime.date):
        ret_value = datetime.datetime.combine(value, datetime.time.min)

    # Case 3: A string
    elif isinstance(value, str):
        try:
            # From Python 3.11+; for older versions you may need dateutil.
            ret_value = datetime.datetime.fromisoformat(value)
        except ValueError:
            raise ValueError(f"String '{value}' is not a valid ISO-8601 datetime/date")

    else:
        raise ValueError(f"Unsupported type: {type(value)}")

    return ret_value



def remove_landsat9_granules_error(hls_granules):
    """
    Legacy / experimental helper for removing LANDSAT-9 HLS granules.

    This function operates on an older representation of HLS granules where
    each entry is a tuple and the platform information is stored in the
    third element of the tuple. Any granule whose third element contains the
    substring ``'LANDSAT-9'`` is removed.

    This function is retained only for debugging and comparison purposes;
    new code should use :func:`remove_landsat9_granules` instead.

    Parameters
    ----------
    hls_granules : list[tuple]
        Iterable of tuples where ``g[2]`` contains a platform description.

    Returns
    -------
    list[tuple]
        Filtered list of tuples ``(g[0], g[1])`` for non-LANDSAT-9 granules.
    """
    hls_granules = [
        (g[0], g[1]) for g in hls_granules if 'LANDSAT-9' not in g[2]
    ]

    return hls_granules


def remove_landsat9_granules(hls_granules):
    """
    Filter out HLS granules acquired by LANDSAT-9.

    The input is expected to contain CMR UMM JSON records, with platform
    metadata stored under ``['umm']['Platforms'][0]['ShortName']``.
    Any entry whose ``ShortName`` equals ``"LANDSAT-9"`` is removed.

    Parameters
    ----------
    hls_granules : list[dict]
        List of HLS granule metadata dictionaries, in CMR UMM JSON structure.

    Returns
    -------
    list[dict]
        New list containing only non-LANDSAT-9 granules (LANDSAT-8,
        Sentinel-2, etc.).
    """

    filtered_hls_granules = [x for x in hls_granules if x['umm']['Platforms'][0]['ShortName'] != "LANDSAT-9"]

    return filtered_hls_granules


def query_cmr_for_products(collection,
                           sensor_datetime_from=None,
                           sensor_datetime_to=None,
                           revision_datetime_from=None,
                           revision_datetime_to=None,
                           north_america_flag=False,
                           central_america_flag=False,
                           remove_landsat9=False,
                           ):
    """
    Query CMR for granules in a given collection and time window.

    This is a thin wrapper around :class:`cmr.GranuleQuery` that:
    - Applies temporal and/or revision-time constraints.
    - Optionally filters to North America or Central America spatially.
    - Optionally removes LANDSAT-9 HLS inputs.

    At least one of the `sensor_datetime_from` or `revision_datetime_from` must
    be provided.

    Parameters
    ----------
    collection : str
        CMR short name of the collection to query (e.g. ``"HLSL30"`` or
        ``"OPERA_L3_DSWX-HLS_V1"``).
    sensor_datetime_from : str | datetime.date | datetime.datetime, optional
        Start of sensing time window (inclusive). Interpreted via
        :func:`normalize_to_datetime`.
    sensor_datetime_to : str | datetime.date | datetime.datetime, optional
        End of sensing time window (inclusive if provided, otherwise
        inferred as the end of the day corresponding to
        ``sensor_datetime_from``).
    revision_datetime_from : str | datetime.date | datetime.datetime, optional
        Start of revision time window for the granule metadata.
    revision_datetime_to : str | datetime.date | datetime.datetime, optional
        End of revision time window. If omitted but a start is given,
        the end of the day is inferred from ``revision_datetime_from``.
    north_america_flag : bool, optional
        If ``True``, constrain the query to the NORTH_AMERICA_POLYGON.
    central_america_flag : bool, optional
        If ``True`` (and ``north_america_flag`` is ``False``), constrain
        the query to the CENTRAL_AMERICA_POLYGON.
    remove_landsat9 : bool, optional
        If ``True``, run :func:`remove_landsat9_granules` on the result set
        before returning.

    Returns
    -------
    list[dict]
        List of granule metadata dictionaries returned by CMR, in UMM JSON
        structure. If ``remove_landsat9`` is enabled, LANDSAT-9 HLS granules
        are filtered out before returning.

    Raises
    ------
    ValueError
        If no temporal or revision datetime constraints are supplied.
    """

    if (sensor_datetime_from is None) and (sensor_datetime_to is None) and (revision_datetime_from is None) and (revision_datetime_to is None):
        raise ValueError("Must provide datetimes to filter CMR query results")

    #if (sensor_datetime_from is None and sensor_datetime_to is None) or (revision_datetime_from is None and revision_datetime_to is None):
    #    raise ValueError("Must provide either sensor or revision datetimes")


    api = GranuleQuery()

    api.format("umm_json")

    api.short_name(collection)
    #datetime_start = datetime.datetime(date.year, date.month, date.day, 0, 0, 0)
    #datetime_end = datetime.datetime(date.year, date.month, date.day, 23, 59, 59)

    if sensor_datetime_from is not None and sensor_datetime_to is None:
        sensor_datetime_to = datetime.datetime(sensor_datetime_from.year,
                                               sensor_datetime_from.month,
                                               sensor_datetime_from.day,
                                               23,
                                               59,
                                               59,
                                               )
    if revision_datetime_from is not None and revision_datetime_to is None:
        revision_datetime_to = datetime.datetime(revision_datetime_from.year,
                                                 revision_datetime_from.month,
                                                 revision_datetime_from.day,
                                                 23,
                                                 59,
                                                 59,
                                                 )

    if (revision_datetime_from is not None) and (revision_datetime_to is not None):
        api.revision_date(date_from=normalize_to_datetime(revision_datetime_from),
                          date_to=normalize_to_datetime(revision_datetime_to),
                          )
    if (sensor_datetime_from is not None) and (sensor_datetime_to is not None):
        api.temporal(date_from=normalize_to_datetime(sensor_datetime_from),
                     date_to=normalize_to_datetime(sensor_datetime_to),
                     )



    if north_america_flag:
        api.polygon(NORTH_AMERICA_POLYGON)
    elif central_america_flag:
        api.polygon(CENTRAL_AMERICA_POLYGON)

    #if counts_only:
    #    results = api.hits()
    #else:
    results = api.get_all()

    granules = []
    # output_results is split in batches of 2000 results
    for batch in results:
        # batch is a string using umm_json format so str to dict
        granules.extend(json.loads(batch)["items"]) # this is a list of granules

        pass
    # now we have a list of all the granules with their associated metadata

    # This isn't working currently... Landsat-9 is not getting filtered out, I believe due to the
    # reassignment of `granules` here.  Skipping the reassignment and just putting `return` before
    # the function call makes it work as intended.
    if remove_landsat9:
        granules = remove_landsat9_granules(granules)

    return granules




def map_inputs_to_output(dswx_hls_results, hlsl30_results, hlss30_results):
    """
    Build a mapping between DSWx-HLS output granules and their input HLS granules.

    This function takes:
    - A set of DSWx-HLS granules (OPERA_L3_DSWX-HLS_V1).
    - The corresponding HLSL30 and HLSS30 granules.

    It extracts, for each DSWx granule, the input HLS product ID and looks
    up its revision metadata and platform information. It also computes how
    many DSWx granules were produced from each HLS input granule.

    Parameters
    ----------
    dswx_hls_results : list[dict]
        CMR UMM JSON records for DSWx-HLS granules.
    hlsl30_results : list[dict]
        CMR UMM JSON records for HLSL30 granules.
    hlss30_results : list[dict]
        CMR UMM JSON records for HLSS30 granules.

    Returns
    -------
    pandas.DataFrame
        A DataFrame with one row per DSWx-HLS granule and columns:

        * ``DSWx_ID`` – DSWx-HLS granule native ID.
        * ``DSWx_RevDate`` – DSWx-HLS revision date.
        * ``InputProduct`` – input HLS granule native ID.
        * ``InputRevId`` – input HLS revision ID.
        * ``InputRevDate`` – input HLS revision date.
        * ``InputPlatform`` – platform short name of the input (e.g. LANDSAT-8).
        * ``DSWx_Granule_Count`` – number of DSWx granules generated from
          this particular HLS input granule.
    """
    # Make a single dictionary, with HLS granule IDs as the keys, and the associated metadata from CMR as the values.
    # This will be useful in the list comprehensions below.
    hls_results_dict = {x['meta']['native-id'] : x for x in hlsl30_results+hlss30_results}

    dswx_mappings = {}

    dswx_mappings['DSWx_ID'] = [x['meta']['native-id'] for x in dswx_hls_results]
    dswx_mappings['DSWx_RevDate'] = [x['meta']['revision-date'] for x in dswx_hls_results]

    # This line is brittle - the index `2` is a magic number here, for the attribute with the input granule.
    dswx_mappings['InputProduct'] = [x['umm']['AdditionalAttributes'][2]['Values'][0] for x in dswx_hls_results]
    dswx_mappings['InputRevId'] = [hls_results_dict[x]['meta']['revision-id'] for x in dswx_mappings['InputProduct']]
    dswx_mappings['InputRevDate'] = [hls_results_dict[x]['meta']['revision-date'] for x in dswx_mappings['InputProduct']]
    dswx_mappings['InputPlatform'] = [hls_results_dict[x]['umm']['Platforms'][0]['ShortName'] for x in dswx_mappings['InputProduct']]

    prods, inds, counts = np.unique(dswx_mappings['InputProduct'], return_index=True, return_counts=True)
    hls_counts_dict = { p : {'index': i, 'count': c} for p,i,c in zip(prods, inds, counts)}

    dswx_mappings['DSWx_Granule_Count'] = [ hls_counts_dict[x]['count'] for x in dswx_mappings['InputProduct']]

    # calculate some columns that will be useful for later analysis
    dswx_mappings['DSWx_ProductionDateTime'] = [extract_production_datetime_from_dswx_hls_id(x) for x in dswx_mappings['DSWx_ID']]
    dswx_mappings['DSWx_ID_no_pdt'] = [remove_production_datetime_from_granule_id(x) for x in dswx_mappings['DSWx_ID']]


    dswx_mappings_df = pd.DataFrame(dswx_mappings)

    # sort by granule ID to regularize the output
    dswx_mappings_df.sort_values(by="DSWx_ID")

    return dswx_mappings_df


def process_dswx_by_sensing_date(date_from, date_to=None):
    """
    Query DSWx-HLS and HLS input collections for a single sensing-date window.

    This function is the core unit of work: it fetches all DSWx-HLS granules
    sensed within the specified time interval and the corresponding HLSL30 and
    HLSS30 granules, then builds a mapping between outputs and inputs.

    Parameters
    ----------
    date_from : str | datetime.date | datetime.datetime
        Start of the sensing window. If ``date_to`` is ``None``, this is
        interpreted as a day and the query covers the interval
        ``[date_from, date_from + 1 day)``.
    date_to : str | datetime.date | datetime.datetime, optional
        End of sensing window. If provided, the window is
        ``[date_from, date_to)``; if ``None``, a single day window is used.

    Returns
    -------
    pandas.DataFrame
        DataFrame returned by :func:`map_inputs_to_output` for the given
        sensing window.
    """

    if date_to is None:
        date_to = normalize_to_datetime(date_from) + datetime.timedelta(days=1)

    dswx_results = query_cmr_for_products("OPERA_L3_DSWX-HLS_V1",
                                          sensor_datetime_from=date_from,
                                          sensor_datetime_to=date_to,
                                          )
    hlsl30_results = query_cmr_for_products("HLSL30",
                                            sensor_datetime_from=date_from,
                                            sensor_datetime_to=date_to,
                                            remove_landsat9=False,
                                            )
    hlss30_results = query_cmr_for_products("HLSS30",
                                            sensor_datetime_from=date_from,
                                            sensor_datetime_to=date_to,
                                            )

    date_df = map_inputs_to_output(dswx_results, hlsl30_results, hlss30_results)

    return date_df



def get_dates_between(start, end):
    """
    Generate a list of daily timestamps between two dates, inclusive.

    Inputs are normalized via :func:`normalize_to_datetime` and then
    iterated in whole-day steps, preserving the time-of-day of the
    starting value for each element.

    Parameters
    ----------
    start : str | datetime.date | datetime.datetime
        Start of the date range (inclusive).
    end : str | datetime.date | datetime.datetime
        End of the date range (inclusive).

    Returns
    -------
    list[datetime.datetime]
        List of ``datetime`` objects, one per day in the range, including
        both ``start`` and ``end``.
    """
    start_date = normalize_to_datetime(start)
    end_date = normalize_to_datetime(end)
    date_list = []
    current_date = start_date
    while current_date <= end_date:
        date_list.append(current_date)
        current_date += datetime.timedelta(days=1)
    return date_list



def process_dswx_by_sensing_date_range(date_from, date_to, verbose=True):
    """
    Process DSWx-HLS mappings for a multi-day sensing-date range.

    This is a convenience wrapper that iterates over each day in the range
    ``[date_from, date_to]`` (inclusive), calling
    :func:`process_dswx_by_sensing_date` once per day. The current
    implementation returns the DataFrame for the **last** day processed.

    Parameters
    ----------
    date_from : str | datetime.date | datetime.datetime
        Start of the multi-day sensing-date range (inclusive).
    date_to : str | datetime.date | datetime.datetime
        End of the multi-day sensing-date range (inclusive).

    Returns
    -------
    pandas.DataFrame
        The DataFrame returned by :func:`process_dswx_by_sensing_date` for
        the final date in the range.

    Notes
    -----
    This function currently discards per-day results for earlier dates in
    the range. If you need to aggregate across days (e.g., concatenate
    per-day DataFrames), you may want to refactor this function to
    accumulate results instead of returning only the last one.
    """

    dates_list = get_dates_between(date_from, date_to)
    if verbose:
        print(dates_list)

    for date in dates_list:
        if verbose:
            print('starting on', date)
        df = process_dswx_by_sensing_date(date)

        # TODO:  make these data frames add onto each other.  Currently this will just return the last day in the range
        # TODO:  alternately, I could have the code write out the dataframe for each day

        fname = 'dswx_dupes_'+f"{date.year:04d}"f"{date.month:02d}"+f"{date.day:02d}"+'.csv'
        if verbose:
            print("printing:", fname)
        df.to_csv(fname)
        if verbose:
            print("finished with:", fname)
        pass

    return


def extract_production_datetime(granule_id):
    """
    Takes an OPERA granule ID granule ID and returns the production datetime.
    The function will parse the granule ID to figure out the product type, then extract the
    production datetime from the granule ID.

    DSWx-HLS Granule ID example:
    OPERA_L3_DSWx-HLS_T56MQV_20251107T000729Z_20251113T173736Z_S2B_30_v1.1

    DSWx-HLS Granule ID format:
    OPERA_L3_DSWx-HLS_T{MGRSTileID}_{SensorDateTime}_{ProductionDateTime}_{satellite}_{pixelSpacing}_{productVersion}
    """

    # split on underscores, then pick the 3rd string
    prod_type = granule_id.split("_")[2]

    prod_dt = None
    if prod_type == "DSWx-HLS":
        prod_dt = granule_id.split("_")[5]

    return normalize_to_datetime(prod_dt)


def remove_production_datetime_from_granule_id(granule_id):
    """
    strips out the production datetime and returns the resulting string.
    Currently this only works on a DSWx-HLS granule ID.
    """
    # split on underscores, then pick the 3rd string
    prod_type = granule_id.split("_")[2]

    granule_id_no_pdt = None
    if prod_type == "DSWx-HLS":
        granule_id_no_pdt = granule_id[:42] + granule_id[58:]

    return granule_id_no_pdt



def calculate_prod_time_deltas(df):
    """
    Will calculate and return an array of the production time deltas between OPERA product revisions.
    """
    dswx_ids_no_pdt = [remove_production_datetime_from_granule_id(x) for x in df['DSWx_ID']]
    dswx_ids_no_pdt_unique = np.unique(dswx_ids_no_pdt)

    # get the size for the return array
    #ret_len = np.unique(df[ df['DSWx_Granule_Count'] > 1 ]['DSWx_ID_no_pdt']).shape[0]

    delta_times = []

    for id in dswx_ids_no_pdt_unique:
        rows = df[np.array(dswx_ids_no_pdt) == id]

        # we can skip when there's just one row, since there's no delta to calculate
        if len(rows) > 1:
            diffs = np.diff(rows['DSWx_ProductionDateTime'])
            delta_times.extend(list(diffs))
            pass
        pass

    return delta_times
