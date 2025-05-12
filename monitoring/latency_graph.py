import argparse
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import matplotlib.lines as mlines
import asyncio
import datetime
from datetime import timedelta
import time
from time import gmtime, strftime
from dateutil.relativedelta import relativedelta
import functools
import logging
import logging.handlers
import os
import re
import sys
import urllib.parse
from collections import defaultdict
from typing import Union, Iterable
from cmr import GranuleQuery
import json
import statistics

import time
START_TIME = time.time()
# time range to be for now past month or maybe week
# today = datetime.date.today()

# print(today)


COLLECTIONS = ["HLSL30", "HLSS30", "OPERA_L3_DSWX-HLS_V1", "empty", "SENTINEL-1A_SLC", "OPERA_L2_RTC-S1_V1", "OPERA_L2_CSLC-S1_V1", "OPERA_L3_DSWX-S1_V1"]
# override to just do one for now
COLLECTIONS =  ["OPERA_L3_DSWX-S1_V1"]

OUT_TO_INP_DICT = {
    "DSWx-HLS": "HLSL30",
    "DSWx-S1": "OPERA_L2_RTC-S1_V1",
    "RTC-S1": "SENTINEL-1A_SLC",
    "CSLC-S1": "SENTINEL-1A_SLC"
}


def get_output_products(collection, temporal_begin, updated_since, north_america_flag=False, central_america_flag=False, return_amounts=False):
    '''
    Granule query api call
    '''
    today = string_to_datetime("2025-05-09T23:59:59Z")

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

    api = GranuleQuery()
    # ummjson format is needed because it includes the input granules
    # it says that granule query doesnt support ummjson but it kinda does but leads to some funkiness later
    api.format("umm_json")
    api.short_name(collection)
    api.revision_date(updated_since, today)
    api.temporal(temporal_begin, today)
    if north_america_flag:
        api.polygon(NORTH_AMERICA_POLYGON)
    elif central_america_flag:
        api.polygon(CENTRAL_AMERICA_POLYGON)
    if return_amounts:
        # only return the amount of products in the result
        return api.hits()
    # returns as json of all results
    return api.get_all()


def get_latest_input_granule(input_gran_list, prod_type):
    '''
    hls inputs:
    "HLS.L30.T41WPU.2025121T071630.v2.0.B02.tif",
    "HLS.L30.T41WPU.2025121T071630.v2.0.B03.tif",
    "HLS.L30.T41WPU.2025121T071630.v2.0.B04.tif",

    cslc inputs:
    S1A_IW_SLC__1SDV_20250408T191358_20250408T191425_058669_0743B7_0ADC

    rtc inputs:
    S1A_IW_SLC__1SDV_20250506T143226_20250506T143253_059075_075429_E267

    dswx-s1 inputs:
    "OPERA_L2_RTC-S1_T018-038556-IW3_20250505T233312Z_20250506T114007Z_S1A_30_v1.0.h5",
    "OPERA_L2_RTC-S1_T018-038556-IW3_20250505T233312Z_20250506T114007Z_S1A_30_v1.0_VH.tif",
    "OPERA_L2_RTC-S1_T018-038556-IW3_20250505T233312Z_20250506T114007Z_S1A_30_v1.0_VV.tif",
    "OPERA_L2_RTC-S1_T018-038556-IW3_20250505T233312Z_20250506T114007Z_S1A_30_v1.0_mask.tif"
    "OPERA_L2_RTC-S1_T018-038572-IW1_20250505T233354Z_20250506T204752Z_S1A_30_v1.0.h5",
    "OPERA_L2_RTC-S1_T018-038572-IW1_20250505T233354Z_20250506T204752Z_S1A_30_v1.0_VH.tif",
    "OPERA_L2_RTC-S1_T018-038572-IW1_20250505T233354Z_20250506T204752Z_S1A_30_v1.0_VV.tif",
    "OPERA_L2_RTC-S1_T018-038572-IW1_20250505T233354Z_20250506T204752Z_S1A_30_v1.0_mask.tif

    '''

    latest_end_time = ""
    latest_gran_id = ""
    for gran in input_gran_list:
        if prod_type == "CSLC-S1" or prod_type == "RTC-S1":
            if len(gran.split("_")) < 6:
                continue
            end_time = gran.split("_")[6]
        elif prod_type == "DSWx-HLS":
            if len(gran.split(".")) < 6:
                continue
            if "HLS" not in gran:
                continue
            end_time = gran.split(".")[6]
        elif prod_type == "DSWx-S1":
            if len(gran.split("_")) < 5:
                continue
            end_time = gran.split("_")[5]

        if end_time > latest_end_time:
            latest_end_time = end_time
            latest_gran_id = gran
    if "." in latest_gran_id:
        latest_gran_id = os.path.splitext(latest_gran_id)[0]
    return latest_gran_id


def get_granule_metadata(granule_id, short_name):
    api = GranuleQuery()
    api.format("umm_json")
    api.short_name(short_name)
    api.granule_ur(granule_id)
    print()
    gran_result = api.get_all()
    umm_json_result = json.loads(gran_result[0])
    #should only be one batcn and one result
    gran_dict = umm_json_result["items"][0]
    return gran_dict


def string_to_datetime(date_str):
    date_format = "%Y-%m-%dT%H:%M:%SZ"
    if "." in date_str:
        date_str = date_str.split(".")[0] + "Z"
    date_str = datetime.datetime.strptime(date_str, date_format)
    return date_str


def compute_granule_latency(out_revision, inp_temporal, inp_revision):
    '''
    three computations:
        output reviosion - inp_revision
        output revision - inp_temporal
        inp_revision - inp_temporal
    '''
    date_format = "%Y-%m-%dT%H:%M:%SZ"
    # "2025-05-07T08:46:21.621Z" to string
    # "2025-05-06T14:32:26Z"
    out_revision = string_to_datetime(out_revision)
    inp_temporal = string_to_datetime(inp_temporal)
    inp_revision = string_to_datetime(inp_revision)

    out_inp_rev = (out_revision - inp_revision).total_seconds()/ (60 * 60)
    out_inp_temp = (out_revision - inp_temporal).total_seconds()/ (60 * 60)
    inp_rev_inp_temp = (inp_revision - inp_temporal).total_seconds()/ (60 * 60)

    return out_inp_rev, out_inp_temp, inp_rev_inp_temp


def parse_output_granules(output_results, time_taken_dict={}):
    '''
    iterate through one pass and retrieve only the data we need from latest input.
    sometimes different products have the same latest input.
    '''

    shared_latest_input_dict = {}
    prod_type = None

    # the result is split in batches of 2000 results
    for batch in output_results:
            # it is a string for some reason when using umm_json format so str to dict
        umm_json_result = json.loads(batch)
        granules = umm_json_result["items"]
        for granule in granules:
            gran_id = granule["meta"]["native-id"]
            input_gran_list = granule["umm"]["InputGranules"]
            revision_date = granule["meta"]["revision-date"]
            # temporal_end = granule["umm"]["TemporalExtent"]["RangeDateTime"]["EndingDateTime"]
            prod_type = gran_id.split("_")[2]

            # we only need the latests input granule
            input_granule = get_latest_input_granule(input_gran_list, prod_type)

            if input_granule in shared_latest_input_dict:
                input_metadata = shared_latest_input_dict[input_granule]
            else:
                # retrieve input collection name
                input_short_name = OUT_TO_INP_DICT[prod_type]
                # we then need to cmr search the granule and get its metadata.
                input_metadata = get_granule_metadata(input_granule, input_short_name)
                shared_latest_input_dict[input_granule] = input_metadata

            inp_revision_date = input_metadata["meta"]["revision-date"]
            inp_temporal_end = input_metadata["umm"]["TemporalExtent"]["RangeDateTime"]["EndingDateTime"]

            # compute latency
            out_inp_rev, out_inp_temp, inp_rev_inp_temp = compute_granule_latency(revision_date, inp_temporal_end, inp_revision_date)

            if not time_taken_dict:
                time_taken_dict = {
                    "output_inp_revision_diff": [],
                    "output_inp_temporal_diff": [],
                    "inp_revision_inp_temporal_diff": []
                }

            time_taken_dict["output_inp_revision_diff"].append(out_inp_rev)
            time_taken_dict["output_inp_temporal_diff"].append(out_inp_temp)
            time_taken_dict["inp_revision_inp_temporal_diff"].append(inp_rev_inp_temp)
    return prod_type, time_taken_dict


def histogram_plot_latency(time_taken_dict, temp_time, rev_time, fake_today=None):
    '''
    histogram seems like best way to represent the data
    rev_time = revision begin time
    temp_time = temporal begin time
    '''
    today = string_to_datetime("2025-05-09T23:59:59Z")

    if fake_today:
        today=fake_today
    # Use complementary colors to differential input and output
    COLORS = [
        (0.0, 0.78, 0.55),   # Light Green
        (0.0, 0.78, 0.55),
        (1.0, 0.22, 0.45),   # Compliment
        (1.0, 0.22, 0.45),
        (1.0, 0.22, 0.45),
        (0.0, 0.62, 0.95),   # Light Blue
        (1.0, 0.38, 0.05),   # ComplimentI
        (1.0, 0.38, 0.05),
        (1.0, 0.33, 0.05),
        (1.0, 0.42, 0.15)
    ]

    x_scale = 1.0  # Adjust to scale the width of the plot area
    y_scale = 0.9  # Adjust to scale the height of the plot area
    bar_width = 0.5  # fixed bar width
    plot_size = (16 * x_scale, 9 * y_scale)
    fig = plt.figure(figsize=plot_size, dpi=100)
    fig.suptitle(f'OPERA_L3_DSWX-S1_V1 Latency Temporal time frame: {temp_time} to: {today} Revision time frame : {rev_time} to {today}')

    # project index which will be the row
    p_ind = 1
    plot_count = 0
    for prod_type, prod_dict in time_taken_dict.items():
        # comparison type index which will be column
        prod_time_list = []
        max_time_list = 0
        mean_list = []
        title_list = []
        c_ind = 1
        plot_count += 1
        for comparison_title, times_list in prod_dict.items():
            # plot_count += 1
            mean = statistics.fmean(times_list)
            mean_list.append(mean)
            print(max(times_list))
            if max(times_list) > max_time_list:
                max_time_list = max(times_list)
            prod_time_list.append(times_list)
            title_list.append(comparison_title)

        colors = ['r', 'b', 'g']
        ax = fig.add_subplot(p_ind, c_ind, plot_count)
        ax.hist(prod_time_list[0], bins=range(int(max_time_list) + 2), alpha=0.5,edgecolor='black', color=colors[0], label=title_list[0])
        ax.hist(prod_time_list[1], bins=range(int(max_time_list) + 2), alpha=0.5,edgecolor='black', color=colors[1], label=title_list[1])
        ax.hist(prod_time_list[2], bins=range(int(max_time_list) + 2), alpha=0.5,edgecolor='black', color=colors[2], label=title_list[2])
        ax.axvline(mean_list[0], color='r', linestyle='--', linewidth=1.75, label='mean output inp revision')
        ax.axvline(mean_list[1], color='g', linestyle='--', linewidth=1.75, label='mean output inp temporal')
        ax.axvline(mean_list[2], color='b', linestyle='--', linewidth=1.75, label='mean inp revision inp temporal')
        ax.set_xlabel(f"Time taken in days")
        ax.set_ylabel("Number of Products")
        label_list = []
        for title in title_list:
            label_list.append(title)
        label_list.append('mean output inp revision')
        label_list.append('mean output inp temporal')
        label_list.append('mean inp revision inp temporal')
        label = label_list
        ax.legend(label, loc="upper right", framealpha=0.2)
        if c_ind == 2:
            c_ind = 0
        c_ind += 1
        if p_ind == 2:
            p_ind += 1

    png_basename = 'opera_latency_query'
    png_filename = png_basename + ".png"
    plt.savefig(png_filename, bbox_inches='tight', dpi=400)
    return png_filename


def trigger_latency_graphs(temporal_delta_months=3, revision_delta=1):
    '''
    create latency graphs
    TEMPORARILY TESTING 3 DAYS TEMPORAL CHILL
    '''
    today = string_to_datetime("2025-05-09T23:59:59Z")


    updated_since = today - timedelta(days=revision_delta)
    # temporal_begin = today - relativedelta(months=temporal_delta_months)
    temporal_begin = today - timedelta(days=temporal_delta_months)

    time_taken_dict = {}
    for collection in COLLECTIONS:
        output_results = get_output_products(collection, temporal_begin, updated_since)
        prod_type, prod_time_taken_dict = parse_output_granules(output_results)
        time_taken_dict[prod_type] = prod_time_taken_dict

    with open('time_taken_dict.json', 'w') as fp:
        json.dump(time_taken_dict, fp, indent=2)

    print("temporal_begin:", temporal_begin)
    print("updated_since:", updated_since)
    png_filename = histogram_plot_latency(time_taken_dict, temporal_begin, updated_since)
    print("My program took", time.time() - START_TIME, "to run")

def main():
    """Produce OPERA daily products plots"""
    trigger_latency_graphs()
    '''
    output_results = get_output_products("OPERA_L2_RTC-S1_V1", temporal_begin, updated_since)
    umm_json_result = json.loads(output_results[0])
    print(len(output_results))
    print(len(umm_json_result["items"]))
    print()
    # print(umm_json_result["items"][0]["meta"]["native-id"])
    # print(umm_json_result["items"][0]["umm"]["InputGranules"])

    print(json.dumps(umm_json_result["items"][0], indent=2))
    '''

if __name__ == '__main__':
    main()
