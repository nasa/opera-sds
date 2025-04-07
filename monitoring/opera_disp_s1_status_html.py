import argparse
import datetime
from datetime import timedelta
from time import gmtime, strftime
import logging
import json
import os
import boto3

import numpy as np
from tqdm import tqdm

SCRIPT_PATH = os.path.dirname(os.path.abspath(__file__))


def get_args():
    """
    Parse command-line arguments to configure the script's behavior.

    Returns:
        argparse.Namespace: Contains command-line arguments as attributes.
    """
    parser = argparse.ArgumentParser(description="Retrieve opera disp s1 hist status html.")
    parser.add_argument('-b', '--bucket', default="opera-pst-rs-pop1", help='html bucket')
    parser.add_argument('-r', '--region', default="us-west-2", help='s3 region')
    parser.add_argument('-s', '--s3path', default="processing_status/DISP_S1/opera_disp_s1_hist_status-ops.html", help='html s3 path')
    return parser.parse_args()


def download_hist_s1_html(bucket="opera-pst-rs-pop1", s3_path="processing_status/DISP_S1/opera_disp_s1_hist_status-ops.html", aws_region="us-west-2"):
    '''
    download hist s1 html and have the github action put it on the readme
    for the opera sds github page
    '''
    html_base = os.path.basename(s3_path)
    html_base = "index.html"
    html_path = os.path.join(SCRIPT_PATH, html_base)
    print("downloading to this path: ", html_path)
    s3 = boto3.client('s3', region_name=aws_region)
    s3.download_file(bucket, s3_path, html_path)
    if os.path.exists(html_path):
        print("downloaded target here", html_path)
        return True
    else:
        return False


def main():
    """Retrieve OPERA hist s1 status html"""
    args = get_args()
    download_hist_s1_html(bucket=args.bucket, s3_path=args.s3path, aws_region=args.region)


if __name__ == '__main__':
    main()
