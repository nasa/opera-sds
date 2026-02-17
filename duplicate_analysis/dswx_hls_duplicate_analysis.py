
import asyncio
from collections import defaultdict
import copy
import datetime
from dateutil.relativedelta import relativedelta
import functools
import inspect
import json
import logging
import logging.handlers
import math
import os
import os.path as osp
import re
import statistics
import sys
import time
from time import gmtime, strftime
import traceback
from typing import Union, Iterable
import urllib.parse

from cmr import GranuleQuery
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import matplotlib.lines as mlines
import numpy as np
import pandas as pd
import requests
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

HLS_PATTERN = re.compile(r'(?P<id>(?P<product_shortname>HLS[.](?P<source>[SL])30)[.](?P<tile_id>T[^\W_]{5})[.]'
                         r'(?P<acquisition_ts>\d{7}T\d{6})[.](?P<collection_version>v\d+[.]\d+))')
HLS_SUFFIX = re.compile(r'[.](B[A-Za-z0-9]{2}|Fmask)[.]tif$')


def process_arg(arg_name, process_func):
    """
    A decorator to apply a function to a specific argument by name,
    whether it is passed as a positional or keyword argument.
    """
    def decorator(func):
        # Get the argument specification of the decorated function
        sig = inspect.signature(func)
        # Get parameter names
        param_names = list(sig.parameters.keys())

        @functools.wraps(func)
        def wrapper(*args, **kwargs):

            # Convert args to a mutable list
            mutable_args = list(args)

            # --- 1. Check in kwargs ---
            if arg_name in kwargs:
                kwargs[arg_name] = process_func(kwargs[arg_name])

            # --- 2. Check in args (positional) ---
            # Determine the index of the argument name
            elif arg_name in param_names:
                arg_index = param_names.index(arg_name)
                # Ensure the index is within the bounds of args
                if arg_index < len(mutable_args):
                    mutable_args[arg_index] = process_func(mutable_args[arg_index])
                # If it's a required argument not in args or kwargs, let Python handle the TypeError later
                # when calling the function.

            # Call the original function with potentially modified args and kwargs
            return func(*mutable_args, **kwargs)

        return wrapper
    return decorator


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

    # Case 4: None is passed in
    elif value is None:
        ret_value = value
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


@process_arg('sensor_datetime_from', normalize_to_datetime)
@process_arg('sensor_datetime_to', normalize_to_datetime)
@process_arg('revision_datetime_from', normalize_to_datetime)
@process_arg('revision_datetime_to', normalize_to_datetime)
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


    #print(sensor_datetime_from, sensor_datetime_to)

    api = GranuleQuery()
    api.format("umm_json")
    api.short_name(collection)

    #api.params['page_size'] = 2000

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
    #results = api.get_all()

    # Manual Search-After Loop
    query_params = dict(api.params)
    query_params['page_size'] = 2000
    query_params["sort_key[]"] = ["provider", "start_date", "producer_granule_id"]
    search_url = "https://cmr.earthdata.nasa.gov/search/granules.umm_json"

    results = []
    search_after = None

    while True:
        headers = {'Accept': 'application/vnd.nasa.cmr.umm_json+json'}
        if search_after:
            headers['CMR-Search-After'] = search_after

        response = requests.get(search_url, params=query_params, headers=headers)
        response.raise_for_status()

        data = response.json().get('items', [])
        if not data:
            break

        results.extend(data)
        search_after = response.headers.get('CMR-Search-After')

        if not search_after:
            break

    #breakpoint()
    #granules = []
    # output_results is split in batches of 2000 results
    #for batch in results:
    #    # batch is a string using umm_json format so str to dict
    #    granules.extend(json.loads(batch)["items"]) # this is a list of granules
    #     pass

    # now we have a list of all the granules with their associated metadata
    granules = copy.deepcopy(results)

    # This isn't working currently... Landsat-9 is not getting filtered out, I believe due to the
    # reassignment of `granules` here.  Skipping the reassignment and just putting `return` before
    # the function call makes it work as intended.
    if remove_landsat9:
        print('here')
        granules = remove_landsat9_granules(granules)

    print(f"{sensor_datetime_from:%Y-%m-%d}: {len(granules)} granules for Collection {collection}")

    return granules


def get_hls_granule_from_dswx_input_list(dswx_input_list):

    for i in dswx_input_list:
        stripped = re.sub(HLS_SUFFIX, '', i)
        tmp = HLS_PATTERN.match(stripped.split('/')[-1])
        if tmp is not None:
            break

    return tmp[0]



def map_inputs_to_output(dswx_hls_results, hlsl30_results, hlss30_results, verbose=False):
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
    #dswx_mappings['InputProduct'] = [x['umm']['AdditionalAttributes'][2]['Values'][0] for x in dswx_hls_results]
    dswx_mappings['InputProduct'] = [get_hls_granule_from_dswx_input_list(x['umm']['InputGranules']) for x in dswx_hls_results]

    # if-statement inside the list comprehension will flag when HLS input granules are missing
    #dswx_mappings['InputRevId'] = [hls_results_dict[x]['meta']['revision-id'] for x in dswx_mappings['InputProduct']]
    #dswx_mappings['InputRevDate'] = [hls_results_dict[x]['meta']['revision-date'] for x in dswx_mappings['InputProduct']]
    #dswx_mappings['InputPlatform'] = [hls_results_dict[x]['umm']['Platforms'][0]['ShortName'] for x in dswx_mappings['InputProduct']]
    dswx_mappings['InputRevId'] = [hls_results_dict[x]['meta']['revision-id'] if x in hls_results_dict else None for x in dswx_mappings['InputProduct']]
    dswx_mappings['InputRevDate'] = [hls_results_dict[x]['meta']['revision-date'] if x in hls_results_dict else None for x in dswx_mappings['InputProduct']]
    dswx_mappings['InputPlatform'] = [hls_results_dict[x]['umm']['Platforms'][0]['ShortName'] if x in hls_results_dict else None for x in dswx_mappings['InputProduct']]

    prods, inds, counts = np.unique(dswx_mappings['InputProduct'], return_index=True, return_counts=True)
    hls_counts_dict = { p : {'index': i, 'count': c} for p,i,c in zip(prods, inds, counts)}

    dswx_mappings['DSWx_Granule_Count'] = [ hls_counts_dict[x]['count'] for x in dswx_mappings['InputProduct']]

    # calculate some columns that will be useful for later analysis
    dswx_mappings['DSWx_ProductionDateTime'] = [extract_production_datetime(x) for x in dswx_mappings['DSWx_ID']]
    dswx_mappings['DSWx_ID_no_pdt'] = [remove_production_datetime_from_granule_id(x) for x in dswx_mappings['DSWx_ID']]


    dswx_mappings_df = pd.DataFrame(dswx_mappings)

    # sort by granule ID to regularize the output
    dswx_mappings_df.sort_values(by="DSWx_ID")

    # Now compile the list of HLS Orphans - HLS products with no DSWx-HLS
    hls_orphans = list(set(hls_results_dict.keys()) - set(dswx_mappings_df['InputProduct']))

    if len(hls_orphans) > 0:
        hls_orphans_df = pd.DataFrame( { 'HLS_GranuleId': hls_orphans,
                                         'HLS_RevId': [hls_results_dict[x]['meta']['revision-id'] for x in hls_orphans],
                                         'HLS_RevDate': [hls_results_dict[x]['meta']['revision-date'] for x in hls_orphans],
                                         'HLS_Platform': [hls_results_dict[x]['umm']['Platforms'][0]['ShortName'] for x in hls_orphans],
                                        }
                                      )
    else:
        # This is kludgey - hard-coding these column names twice, to avoid an error when there are no hls orphans to return.
        hls_orphans_df = pd.DataFrame(columns=['HLS_GranuleId', 'HLS_RevId', 'HLS_RevDate', 'HLS_Platform'])

    # Now compile the list of HLS granule IDs that are missing.
    # These are HLS Granule IDs present in the DSWx-HLS product metadata, but do not show up in the query results.
    missing_hls_ids = [ x for x in dswx_mappings['InputProduct'] if x not in hls_results_dict ]

    print_str = f"Missing {len(missing_hls_ids)} HLS input granules from DAAC query results"
    #if missing_hls_ids and verbose:
    if verbose:
        print_str = print_str + f":  {missing_hls_ids}"
    print(print_str)

    return dswx_mappings_df, hls_orphans_df, missing_hls_ids


def find_orphaned_inputs(dswx_hls_results, hlsl30_results, hlss30_results):

    # Make a single dictionary, with HLS granule IDs as the keys, and the associated metadata from CMR as the values.
    # This will be useful in the list comprehensions below.
    hls_results_dict = {x['meta']['native-id'] : x for x in hlsl30_results+hlss30_results}

    dswx_results_dict =  {x['meta']['native-id'] : x for x in dswx_hls_results}

    return



@process_arg('date_from', normalize_to_datetime)
@process_arg('date_to', normalize_to_datetime)
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

    date_df, hls_orphans_df, missing_hls_ids = map_inputs_to_output(dswx_results, hlsl30_results, hlss30_results)

    return date_df, hls_orphans_df, missing_hls_ids



@process_arg('start', normalize_to_datetime)
@process_arg('end', normalize_to_datetime)
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



@process_arg('date_from', normalize_to_datetime)
@process_arg('date_to', normalize_to_datetime)
def process_dswx_by_sensing_date_range(date_from, date_to, output_dir='.', verbose=True, resume=False):
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
        print( "Dates to process:", [f"{date_obj:%Y-%m-%d}" for date_obj in dates_list] )

    for date in dates_list:
        dupes_fname = f"dswx_dupes_{date.year:04d}-{date.month:02d}-{date.day:02d}.csv"
        orphan_fname = f"hls_orphans_{date.year:04d}-{date.month:02d}-{date.day:02d}.csv"
        missing_fname = f"missing_hls_ids_{date.year:04d}-{date.month:02d}-{date.day:02d}.txt"

        if resume:
            if os.path.isfile( osp.join(output_dir, dupes_fname) ):
                print(f"skipping {date:%Y-%m-%d} since the output already exists ({osp.join(output_dir, dupes_fname)})")
                continue
        try:
            if verbose:
                print(f"starting on {date:%Y-%m-%d}")
            df, orphans, missing_hls_ids = process_dswx_by_sensing_date(date)

            # TODO:  make these data frames add onto each other.  Currently this will just return the last day in the range
            # TODO:  alternately, I could have the code write out the dataframe for each day

            if verbose:
                print("writing:", dupes_fname)
            df.to_csv(osp.join(output_dir, dupes_fname))

            if verbose:
                print("writing:", orphan_fname)
            orphans.to_csv(osp.join(output_dir, orphan_fname))

            if verbose:
                print("writing:", missing_fname)
            # missing_hls_ids is just a list of strings
            with open(osp.join(output_dir, missing_fname), "w") as file:
                # Join all elements with a newline as a separator
                file.write("\n".join(missing_hls_ids))

        except Exception as e:
            print(f"error processing date: {date} with the following Exception: {e}")
            traceback.print_exc()
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
        # do a copy here, because later we may need to convert the datetimes, and I don't want this
        #to propagate back to the original dataframe.
        rows = df[np.array(dswx_ids_no_pdt) == id].copy()

        # we can skip when there's just one row, since there's no delta to calculate
        if len(rows) > 1:
            rows['DSWx_ProductionDateTime'] = rows['DSWx_ProductionDateTime'].map(normalize_to_datetime)
            diffs = np.diff(rows['DSWx_ProductionDateTime'])
            delta_times.extend(list(diffs))
            pass
        pass

    return delta_times


def load_riley_json(input_dict):
    '''
    pre-requisite:
    with open ('./jan-oct 1.json') as f: dupes_jan_oct = json.load(f)

    resulting dict has three fields:  summary, counts_by_date, hls_to_dswx_mappings_by_date

    '''

    dupes_counts = []
    for key, metrics in input_dict.items():
        date_str, doy_str = key.split(" / ")
        dupes_counts.append({"date": pd.to_datetime(date_str),
                             "doy": int(doy_str.split("-")[1]),
                             **metrics
                             })
    dupes_counts_df = pd.DataFrame(dupes_counts).set_index("doy").sort_index()

    return


@process_arg('compare_date', normalize_to_datetime)
def compare_against_riley_results(compare_date, riley_results, my_results_dir):
    '''
    Run this in iPython with the following command:
    for i in np.arange(1, 32, 1):
        print(f'2025-01-{i:02d}: ', dswx.compare_against_riley_results(f'2025-01-{i:02d}', {filepath to Riley's Results}, {filepath to my data dir}))
    '''
    ret_value = True

    #with open ('./jan-oct 1.json') as f:
    #    riley_dupes = json.load(f)
    with open (riley_results) as f:
        riley_dupes = json.load(f)

    date = normalize_to_datetime(compare_date)
    dupes_df = pd.read_csv(osp.join(my_results_dir, f'dswx_dupes_{date.year:04d}-{date.month:02d}-{date.day:02d}.csv'))

    compare_date_key = f"{date.year:04d}-{date.month:02d}-{date.day:02d} / {date.year:04d}-{date.strftime('%j')}"
    riley_dupes_for_date = riley_dupes['hls_to_dswx_mappings_by_date'][compare_date_key]

    # Just pull out the dict entries with non-empty values.  This avoids trying to include orphaned HLS granules.
    riley_dupes_for_date_nonempty = {k:v for k, v in riley_dupes_for_date.items() if v}

    # first compare Riley's list of HLS IDs with multiple DSWx-HLS products against my list of the same
    # This says if the set of unique HLS granules are the same.  This should evaluate to true
    #assert set(riley_dupes_for_date_nonempty.keys()) == set(dupes_df[ dupes_df['DSWx_Granule_Count'] > 1 ]['InputProduct']), f'failed assert for {date}'
    ret_value = ret_value and ( set(riley_dupes_for_date_nonempty.keys()) == set(dupes_df[ dupes_df['DSWx_Granule_Count'] > 1 ]['InputProduct']) )
    # if this is true, then we can just loop over one list and we know we will get all HLS IDs in either set.
    if ret_value:
        # Now confirm that the exact granule DSWx-HLS names are consistent
        for k in riley_dupes_for_date_nonempty.keys():
            dswx_ids_riley = riley_dupes_for_date_nonempty[k]
            dswx_ids_mine = list(dupes_df[ dupes_df['InputProduct'] == k]['DSWx_ID'])

            #assert sorted(dswx_ids_riley) == sorted(dswx_ids_mine), f'failed assert for {k}'
            ret_value = ret_value and (sorted(dswx_ids_riley) == sorted(dswx_ids_mine))

    #return True
    return ret_value


@process_arg('date_from', normalize_to_datetime)
@process_arg('date_to', normalize_to_datetime)
def compare_against_riley_results_by_sensing_date_range(date_from, date_to, riley_results, my_results_dir='.'):
    print(date_from, date_to)
    dates_list = get_dates_between(date_from, date_to)

    ans_for_all_dates = True
    for date in dates_list:
        ans = compare_against_riley_results(date, riley_results, my_results_dir)
        print(f"{date.year:04d}-{date.month:02d}-{date.day:02d}: {ans}")
        ans_for_all_dates = ans_for_all_dates and ans


    print(f"Answer for all dates: {ans_for_all_dates}")

    return


if __name__ == '__main__':
    # this is just a holding bin for code I'm using in iPython.  Delete it before committing to the repo.

    df = pd.read_csv(all_files[0])

    np.histogram(df['DSWx_Granule_Count'])


    df.iloc[ list(np.unique(df['InputProduct'], return_index=True)[1]) ]
    np.histogram(merged_df_hls_unique['InputRevId'])

    np.histogram(merged_df.iloc[ list(np.unique(merged_df['InputProduct'], return_index=True)[1]) ]['InputRevId'])


    prod_time_deltas = []
    prod_time_deltas_sec = []
    for d, f in zip(dfs, all_files):
        print(f)
        delta_times = dswx.calculate_prod_time_deltas(d)
        delta_seconds = [x.total_seconds() for x in delta_times]
        prod_time_deltas.append(delta_times.copy())
        prod_time_deltas_sec.extend(delta_seconds.copy())
        delta_days = [x/60/60/24 for x in delta_seconds]
        #bins = np.arange(0.5,20.5,1)
        bins = np.arange(-6,20,1/24)
        plt.figure()
        plt.hist(delta_days, bins=bins, log=True);
        plt.title('Log-Hist of DSWx-HLS Production Datetime Deltas')
        plt.xlabel('delta days')
        plt.savefig('Prod_Time_Hist_'+f[18:-4]+'.png')
        print('fig saved:', 'Prod_Time_Hist_'+f[18:-4]+'.png')
