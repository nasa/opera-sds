"""
Script that queries CMR for number of granules generated each of the last N days,
for each product
"""

from datetime import date, timedelta
from time import gmtime, strftime
import logging

from cmr import GranuleQuery
import matplotlib.pyplot as plt
logging.basicConfig(level=logging.INFO)

NUM_DAYS = 10
COLLECTIONS = ["HLSL30", "HLSS30", "OPERA_L3_DSWX-HLS_V1",
               "SENTINEL-1A_SLC", "OPERA_L2_RTC-S1_V1", "OPERA_L2_CSLC-S1_V1", "OPERA_L3_DSWX-S1_V1"]
LABELS = ["HLSL30 (input)", "HLSS30 (input)", "DSWX-HLS (output)",
          "S1A (input)", "RTC-S1 (output)", "CSLC-S1 (output)", "DSWX-S1 (output)"]
COLORS = [
    (0.0, 0.85, 0.60),   # Light Green (HLSL30)
    (0.0, 0.85, 0.60),   # Light Green (HLSS30)
    (0.0, 0.39, 0.25),   # Dark Green  (DSWX-HLS)
    (0.0, 0.62, 0.95),   # Light Blue  ()
    (0.0, 0.35, 0.5),    # Dark Blue   ()
    (0.0, 0.35, 0.5),    # Dark Blue   ()
    (0.0, 0.35, 0.5)     # Dark Blue   ()
]

today = date.today()
now = strftime("%Y-%m-%d %H:%M:%S", gmtime())
dates_list: list[date] = [today - timedelta(days=day) for day in range(NUM_DAYS)]
dates_list.reverse()

logging.info(dates_list)

# configure plotting
plt.rcParams["font.size"] = 4
plt.rcParams["figure.titlesize"] = 10
fig = plt.figure()
fig.suptitle(f'# of Products / Day from: {dates_list[0]} to: {dates_list[NUM_DAYS-1]}',
             fontsize=12)
fig.text(0.5, 0.9, f"Updated: {now} (GMT)",
         horizontalalignment="center", fontsize=10)

# loop over collections
irow = 0
icol = 0
for ic, collection in enumerate(COLLECTIONS):

    # loop over days
    products = [0] * NUM_DAYS
    for i in range(NUM_DAYS):
        start_datetime = dates_list[i]
        stop_datetime = start_datetime + timedelta(days=1)
        logging.info(f"\tDay: {start_datetime}")

        # query CMR for 1 day of standard products
        api = GranuleQuery()
        api.short_name(collection)
        api.temporal(start_datetime, stop_datetime)
        num_products = api.hits()
        logging.info(f"Collection: {collection} Day: {start_datetime} # of products in CMR: "
                     f"{num_products}")
        products[i] = num_products

    # plot this product
    ax = plt.subplot2grid((2, 4), (irow, icol))
    # plt.subplot(2, 4, ic+1)
    plt.bar(dates_list, products, width=0.9,
            tick_label=[x.strftime("%d") for x in dates_list],
            color=COLORS[ic], label=f"{LABELS[ic]}")
    plt.legend(loc="upper right")
    icol += 1
    if ic == 2:
        irow += 1
        icol = 0

# Adjust space between plots to avoid overlap
fig.subplots_adjust(left=None, right=None, top=None, bottom=None, wspace=0.5, hspace=0.5)

plt.savefig('opera_daily_products_query.png', bbox_inches='tight', dpi=400)
plt.close(fig)
# plt.show()
