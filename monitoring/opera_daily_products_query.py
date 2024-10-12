import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from datetime import date, timedelta
from time import gmtime, strftime
import logging
from cmr import GranuleQuery
import numpy as np

logging.basicConfig(level=logging.INFO)


def adjust_saturation(bar_color, adjustment_factor, lighten=True):
    """ Adjust the saturation of a color, lightening or darkening it. """
    base = np.array(mcolors.to_rgb(bar_color))
    if lighten:
        white = np.array([1, 1, 1])
        return tuple(base + (white - base) * (adjustment_factor / 100))
    else:
        return tuple(base * (1 - adjustment_factor / 100))


NUM_DAYS = 30
x_scale = 1.0  # Adjust this to scale the width of the plot area
y_scale = 0.9  # Adjust this to scale the height of the plot area
bar_width = 0.5  # Set a fixed bar width for clarity
plot_size = (16 * x_scale, 9 * y_scale)

COLLECTIONS = ["HLSL30", "HLSS30", "OPERA_L3_DSWX-HLS_V1",
               "SENTINEL-1A_SLC", "OPERA_L2_RTC-S1_V1", "OPERA_L2_CSLC-S1_V1", "OPERA_L3_DSWX-S1_V1"]
LABELS = ["HLSL30 (input)", "HLSS30 (input)", "DSWX-HLS (output)",
          "S1A (input)", "RTC-S1 (output)", "CSLC-S1 (output)", "DSWX-S1 (output)"]
# Use complementary colors to differential input and output
COLORS = [
    (0.0, 0.78, 0.55),   # Light Green
    (0.0, 0.78, 0.55),   # Compliment
    (1.0, 0.22, 0.45),   #
    (0.0, 0.62, 0.95),   # Light Blue
    (1.0, 0.38, 0.05),   #
    (1.0, 0.38, 0.05),   #
    (1.0, 0.33, 0.05)    #
]

today = date.today()
now = strftime("%Y-%m-%d %H:%M:%S", gmtime())
dates_list = [today - timedelta(days=x) for x in range(NUM_DAYS)]
dates_list.reverse()

x_values = list(range(NUM_DAYS))

fig = plt.figure(figsize=plot_size)
fig.suptitle(f'# of Products / Day from: {dates_list[0]} to: {dates_list[NUM_DAYS-1]} (Last Updated: {now} GMT)')

# Create subplots: first 3 in the first row, last 4 in the second row
for ic, collection in enumerate(COLLECTIONS):
    if ic < 3:
        ax = fig.add_subplot(2, 4, ic + 1)  # Positions 1, 2, 3
    else:
        ax = fig.add_subplot(2, 4, ic + 2)  # Skip position 4, start from 5
    products = [0] * NUM_DAYS
    current_month = dates_list[0].month  # Initialize the current month
    original_color = COLORS[ic]  # Store the original color
    # adjust lightening/darkening on month change based on what the graph represents
    if "output" in LABELS[ic]:
        using_lightened = False
    else:
        using_lightened = False

    for i, date in enumerate(dates_list):
        api = GranuleQuery()
        api.short_name(collection)
        api.temporal(date, date + timedelta(days=1))
        products[i] = api.hits()
        logging.info(f"Collection: {collection} Day: {date} # of products in CMR: {products[i]}")

        # Check for month change to toggle the color state
        if date.month != current_month:
            using_lightened = not using_lightened  # Toggle the state for the new month
            current_month = date.month  # Update the current month indicator

        # Set the color based on whether the month should use the lightened version
        if using_lightened:
            color = adjust_saturation(original_color, 15, lighten=False)  # False will darken
        else:
            color = original_color  # Use the original color without any modification)

        ax.bar(x_values[i], products[i], width=bar_width, color=color)

    ax.legend([LABELS[ic]], loc="upper right")

    # Set x-ticks to show only odd days
    odd_days = [d for i, d in enumerate(dates_list) if i % 2 == 0]  # Get odd days
    ax.set_xticks(x_values[::2])  # Set ticks to every other index
    ax.set_xticklabels([d.strftime('%d') for d in odd_days], rotation=45)  # Print odd days without leading zeros
    ax.set_xlabel("Day of Month")
    ax.set_ylabel("Number of Products")

plt.tight_layout()
plt.savefig('opera_daily_products_query.png', bbox_inches='tight', dpi=400)
# plt.show()
