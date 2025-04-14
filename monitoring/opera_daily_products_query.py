import argparse
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import matplotlib.lines as mlines
import datetime
from datetime import timedelta
from time import gmtime, strftime
import logging
import json

from cmr import GranuleQuery
import numpy as np
from tqdm import tqdm


def get_args():
    """
    Parse command-line arguments to configure the script's behavior.

    Returns:
        argparse.Namespace: Contains command-line arguments as attributes.
    """
    parser = argparse.ArgumentParser(description="Generate daily plots.")
    parser.add_argument('-q', '--quiet', action='store_true', help='Minimize output and run progress bar.')
    return parser.parse_args()


def setup_logging(quiet):
    """
        Configures the logging level based on the quiet flag.

        Parameters:
            quiet (bool): If True, set logging to display only warnings and above. Otherwise, set to display info level.
        """
    if quiet:
        logging.basicConfig(level=logging.WARNING)  # Only log warnings and above in quiet mode
    else:
        logging.basicConfig(level=logging.INFO)  # Log info messages in normal mode


def adjust_saturation(bar_color, adjustment_factor, lighten=True):
    """
    Adjusts the saturation of a given color by modifying its luminosity.

    Parameters:
    - bar_color (tuple or str): The base color specified as a name or an RGB tuple.
    - adjustment_factor (float): The percentage factor by which to adjust the color's saturation.

    - lighten (bool): A flag indicating whether to lighten (True) or darken (False) the color.

    Returns:
    - tuple: A new color tuple representing the adjusted color in RGB format.

    """
    base = np.array(mcolors.to_rgb(bar_color))
    if lighten:
        white = np.array([1, 1, 1])
        return tuple(base + (white - base) * (adjustment_factor / 100))
    else:
        return tuple(base * (1 - adjustment_factor / 100))


def remove_trailing_zeros_and_last_entry(lst):
    """
    Removes trailing zeros and the last non-zero entry from a list.

    Parameters:
        lst (list): A list of product summary numbers with trailing zeros.

    Returns:
        list: A new list with the trailing zeros and the last non-zero entry removed.

    """
    # Reverse the list to start checking from the end
    reversed_list = lst[::-1]

    # Find the index of the first non-zero element
    for i, value in enumerate(reversed_list):
        if value != 0:
            # Remove all elements from the first non-zero element onwards
            new_list = reversed_list[i + 1:]  # i+1 to skip the first non-zero element found
            break
    else:
        # no zeros in list
        return []

    # Return the new list in the original order
    return new_list[::-1]


def check_data_values(full_list, stat_list, collection):
    logging.info(f'Collection:  {collection}')
    logging.info(f'Plot list: {full_list}')
    logging.info(f'Stat list: {stat_list}')


def get_statistics(sample_values, collection, sigma_multiplier=2, debug=False):
    """
    Calculates the population standard deviation of the given sample values, excluding the last three entries,
    and computes the boundaries for sigma deviation.

    Parameters:
    - sample_values (list of float): The list of sample values from which to calculate the standard deviation.
    - sigma_multiplier (int): The number of standard deviations to use for the sigma boundary
      calculation (default is 2).

    Returns:
    - tuple: A tuple containing the truncated sample list, the calculated standard deviation,
      and the computed sigma boundaries.

    Excludes the last three entries of the sample values, assuming they are partial or zero

    """
    # throw out trailing zero samples and the last non-zero partial sample
    sample = remove_trailing_zeros_and_last_entry(sample_values)
    if not sample:
        return [sample, 0, [0, 0]]
    if debug:
        check_data_values(sample_values, sample, collection)
    arr = np.array(sample)
    std_dev = arr.std()
    mean = arr.mean()
    std_sigma = [float(mean - (sigma_multiplier * std_dev)), float(mean + (sigma_multiplier * std_dev))]

    return[sample, mean, std_sigma]


def check_data_points(sample, sigma_value):
    """
    Checks each data point in the sample against specified sigma boundaries and logs warnings
    for values outside the range.

    Parameters:
    - sample (list of float): A list of numeric data points to be checked.
    - sigma_value (list of float): A list with two elements specifying the lower and upper boundaries
      of acceptable values based on sigma.

    This function will iterate over each value in the sample list and compare it against the
    lower and upper sigma boundaries.
    It logs a warning if a value falls outside this range.

    """
    for val in sample:
        if val < sigma_value[0]:
            logging.info(f'Value {val} is less than expected low value of {sigma_value[0]}')
            # TODO take some action (email, slack message) - firsts verify this is what Luca wants
        elif val > sigma_value[1]:
            logging.info(f'Value {val} is greater than expected low value of {sigma_value[0]}')
            # TODO - may not need this test (check with Luca)


def get_products(collection, date, north_america_flag=False, central_america_flag=False):
    '''
    Granule query api call
    '''
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
    api.short_name(collection)
    if "DISP" in collection:
        api.revision_date(date, date + timedelta(days=1))
    else:
        api.temporal(date, date + timedelta(days=1))
    if north_america_flag:
        api.polygon(NORTH_AMERICA_POLYGON)
    elif central_america_flag:
        api.polygon(CENTRAL_AMERICA_POLYGON)
    return api.hits()




def plot_products(quiet):
    """
    Generates a series of subplots, each depicting the daily count of products for various collections
    over a period of 30 days.

    The function configures a plot environment with multiple subplots arranged into rows and columns,
    and calculates and overlays statistical markers (mean and 2-sigma boundaries) on each subplot.

    Parameters:
        quiet (bool): If True, reduces console output to warnings and displays a progress bar.

    """
    args = get_args()
    setup_logging(args.quiet)

    NUM_DAYS = 30
    x_scale = 1.0  # Adjust to scale the width of the plot area
    y_scale = 0.9  # Adjust to scale the height of the plot area
    bar_width = 0.5  # fixed bar width
    plot_size = (16 * x_scale, 9 * y_scale)

    COLLECTIONS = ["HLSL30", "HLSS30", "OPERA_L3_DSWX-HLS_V1", "OPERA_L3_DIST-ALERT-HLS_V1", "empty",
                   "SENTINEL-1A_SLC", "OPERA_L2_RTC-S1_V1", "OPERA_L2_CSLC-S1_V1", "OPERA_L3_DSWX-S1_V1", "OPERA_L3_DISP-S1_V1"]
    LABELS = ["HLSL30 (input)", "HLSS30 (input)", "DSWX-HLS (output)",  "DIST-ALERT-HLS (output)", "empty",
              "S1A (input)", "RTC-S1 (output)", "CSLC-S1 (output)", "DSWX-S1 (output)", "DISP-S1 (output)"]

    NA_COLLECTIONS = ["SENTINEL-1A_SLC", "OPERA_L2_RTC-S1_V1", "OPERA_L2_CSLC-S1_V1", "OPERA_L3_DSWX-S1_V1", "OPERA_L3_DISP-S1_V1"]

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

    today = datetime.date.today()
    now = strftime("%Y-%m-%d %H:%M:%S", gmtime())
    dates_list = [today - timedelta(days=x) for x in range(NUM_DAYS)]
    dates_list.reverse()

    x_values = list(range(NUM_DAYS))

    fig = plt.figure(figsize=plot_size, dpi=100)
    fig.suptitle(f'# of Products / Day from: {dates_list[0]} to: {dates_list[NUM_DAYS-1]} (Last Updated: {now} GMT)')

    # Use a progress bar if 'quiet' option is set
    outer_progress_bar = tqdm(total=len(COLLECTIONS), desc="Overall Progress", leave=True) if quiet else None

    # Create subplots: 2 row, 4 plots/row
    second_row_flag = False
    total_col = 4
    for ic, collection in enumerate(COLLECTIONS):
        if ic + 1 == 5 and not second_row_flag:
            second_row_flag = True
            total_col = 5
            continue
        ax = fig.add_subplot(2, total_col, ic + 1)
        products = [0] * NUM_DAYS
        na_products = [0] * NUM_DAYS
        current_month = dates_list[0].month  # Initialize the current month
        original_color = COLORS[ic]  # Store the original color
        # adjust lightening/darkening on month change based on what the graph represents
        if "output" in LABELS[ic]:
            using_lightened = False
        else:
            using_lightened = False

        collection_name = collection

        inner_progress_bar = tqdm(total=NUM_DAYS, desc=f"Processing {collection}", leave=False) if quiet else None

        for i, date in enumerate(dates_list):
            products[i] = get_products(collection, date, north_america_flag=False)
            logging.info(f"Collection: {collection_name} Day: {date} # of products in CMR: {products[i]}")

            if collection in NA_COLLECTIONS:
                na_products[i] = get_products(collection, date, north_america_flag=True)
                logging.info(f"North America only Collection: {collection_name} Day: {date} # of products in CMR: {na_products[i]}")
            # Check for month change to toggle the color state
            if date.month != current_month:
                using_lightened = not using_lightened  # Toggle the state for the new month
                current_month = date.month  # Update the current month indicator

            # Set the color based on whether the month should use the lightened version
            if using_lightened:
                color = adjust_saturation(original_color, 15, lighten=False)  # False will darken
            else:
                color = original_color  # Use the original color without any modification)

            if collection in NA_COLLECTIONS:
                x_width = 0.4
                na_color = adjust_saturation(original_color, 60, lighten=False)
                ax.bar(x_values[i], products[i], width=bar_width, color=color)
                ax.bar(x_values[i] + x_width, na_products[i], width=bar_width, color=na_color)
            else:
                ax.bar(x_values[i], products[i], width=bar_width, color=color)

            if inner_progress_bar:
                inner_progress_bar.update(1)  # Update the inner progress bar

        if inner_progress_bar:
            inner_progress_bar.close()

        if outer_progress_bar:
            outer_progress_bar.update(1)

        [samples, mean, std_sigma] = get_statistics(products, collection_name, debug=True)

        label = [LABELS[ic]]
        if collection in NA_COLLECTIONS:
            label.append("North+Central America")
        ax.legend(label, loc="upper right", framealpha=0.2)
        ax.axhline(mean, color=(0.0, 0.4, 0.04, 0.85), linestyle='--', linewidth=1.75, label='Mean')
        if "DISP" not in collection:
            ax.axhline(std_sigma[0], color=(0.0, 0.4, 0.04, 0.85), linestyle=':', linewidth=2.0, label='-2-sigma')
            ax.axhline(std_sigma[1], color=(0.0, 0.4, 0.04, 0.85), linestyle=':', linewidth=2.0, label='+2-sigma')

        check_data_points(samples, std_sigma)

        # Set x-ticks to show only odd days
        odd_days = [d for i, d in enumerate(dates_list) if i % 2 == 0]  # Get odd days
        ax.set_xticks(x_values[::2])  # Set ticks to every other index
        ax.set_xticklabels([d.strftime('%d') for d in odd_days], rotation=45)  # Print odd days without leading zeros
        ax.set_xlabel(f"Day of Month")
        ax.set_ylabel("Number of Products")

    # adjust plot positions to allow space for statistical legena
    plt.subplots_adjust(left=0.1, bottom=0.12, right=0.9, top=0.9, hspace=0.4, wspace=0.3)

    # Create custom handles for the global legend
    mean_line = mlines.Line2D([], [], color=(0.0, 0.4, 0.04, 1.0), linestyle='--', linewidth=1.75, label='Mean')

    std_line = mlines.Line2D([], [], color=(0.0, 0.4, 0.04, 1.0), linestyle=':', linewidth=2.0, label='Mean ± 2 STD')

    # Add a global legend for mean and std deviation
    # Note: bbox_to_anchor: 0.5 centers in 'x', 0.0 put it at the bottom of the plot
    fig.legend(handles=[mean_line, std_line], loc='lower center', bbox_to_anchor=(0.52, 0.0), framealpha=0.5, ncol=2,
               facecolor=(1.0, 0.75, 0.63, 0.75), edgecolor='black')

    plt.tight_layout(pad=2.0)
    png_basename = 'opera_daily_products_query'
    png_filename = png_basename + ".png"

    plt.savefig(png_filename, bbox_inches='tight', dpi=400)
#    plt.show()

    if args.quiet:
        outer_progress_bar.close()


def main():
    """Produce OPERA daily products plots"""
    args = get_args()
    setup_logging(args.quiet)
    plot_products(args.quiet)


if __name__ == '__main__':
    main()
