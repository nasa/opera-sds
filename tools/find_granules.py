import boto3
import os
import argparse
from elasticsearch import Elasticsearch

RS_BUCKET = 'opera-int-rs-pop1'


def get_prefix(granule):
    '''
    given granule name retrieve s3 prefix
    '''
    # OPERA_L3_DSWx-HLS_
    par_dir_dash = granule.split("_")[2]
    if "-" in par_dir_dash:
        # ex: DSWx-HLS to DSWx_HLS

        par_dir = par_dir_dash.replace("-", "_")

    # prefix = "products/" + par_dir + "/" + par_dir_dash
    prefix = "products/" + par_dir + "/"
    return prefix


def check_granule_s3(granule, prefix):
    '''
    given granule check if in s3
    '''
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(RS_BUCKET)
    gran_s3_path = prefix + granule
    exist_flag = False

    try:
        s3.Object(bucket, gran_s3_path).load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            # The object does not exist.
            exist_flag = False
        else:
            # Something else has gone wrong.
            raise
    else:
        exist_flag = True
        print(gran_s3_path + " found in s3")

    return exist_flag


def check_granule_grq(granule, host):
    '''
    use elastic search to search grq
    '''
    exist_flag = False
    es = Elasticsearch([host])


    return exist_flag


def find_missing_granules(granule_list, bucket='opera-int-rs-pop1', output_file=None, host=None):
    '''
    given a list of granules create a list of missing and found granules in s3 and grq
    '''
    s3_missing_granule = []
    s3_found_granule = []
    grq_missing_granule = []
    grq_found_granule = []
    both_missing_granule = []
    both_found_granule = []

    gran_list = open(granule_list, "r")
    for granule in gran_list:
        print("checking granule: ", granule)
        # get s3 prefix
        prefix = get_prefix(granule)
        s3_found = check_granule_s3(granule, prefix)

        grq_found = check_granule_grq(granule, host)

        if grq_found and s3_found:
            both_found_granule.append(granule)
        elif not grq_found and not s3_found:
            both_missing_granule.append(granule)
        elif s3_found:
            s3_found_granule.append(granule)
        elif not s3_found:
            s3_missing_granule.append(granule)
        elif grq_found:
            grq_found_granule.append(granule)
        elif not grq_found:
            grq_missing_granule.append(granule)

    return s3_missing_granule, s3_found_granule, grq_missing_granule, grq_found_granule, both_missing_granule, both_found_granule


def report_found_granules(s3_found_granule, grq_found_granule, both_found_granule, output_file=None):
    '''
    stdout print report of found matching granules from input list
    or output results to txt file
    '''
    print("Found Granule Report")
    print("--------------------")
    print()
    if both_found_granule:
        print("Granules found in both s3 and grq")
        print("---------------------------------")
        for granule in both_found_granule:
            print(granule)
        print()
    if s3_found_granule:
        print("Granules found in only s3")
        print("-------------------------")
        for granule in s3_found_granule:
            print(granule)
        print()
    if grq_found_granule:
        print("Granules found in only grq")
        print("--------------------------")
        for granule in grq_found_granule:
            print(granule)
        print()

    return None


def main():
    '''
    return which files are missing given a list
    '''
    parser = argparse.ArgumentParser()
    # Optional key-value arguments:
    parser.add_argument('-i', '--input', help="input list of granules")
    parser.add_argument('-o', '--output_file', default="found_granules_outputs.txt", type=str, help="output file name")
    parser.add_argument('-b', '--bucket', default='opera-int-rs-pop1', help="s3 bucket")
    parser.add_argument('-h', '--host', default='https://opera-int-mozart-pop1.jpl.nasa.gov/grq_es/', help="es host")
    args = parser.parse_args()
    s3_missing_granule, s3_found_granule, grq_missing_granule, grq_found_granule, both_missing_granule, both_found_granule = find_missing_granules(args.input, bucket=args.bucket, output_file=args.output_file, host=args.host)
    report_found_granules(s3_found_granule, grq_found_granule, both_found_granule)


if __name__ == '__main__':
    main()
