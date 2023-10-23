This readme will describe the files contained within or beneath this current directory.

## RTC-S1 Static Layers
Total of 2 files.

1. `rtc_query_bursts_2016-05-01_to_2023-09.csv`
   - Contains the unique RTC-S1 bursts appearing in Sentinel-1 A/B data starting at May 1st 2016 through mid-September 2023.
   - Any bursts that ONLY appear in the Sentinel-1 record before May 2016 will not be included.
   - Contains 297562 burst IDs
   - File does not contain column names.  The column names should be:  `burst ID`, `First Time Seen`, `S1 Granule`
2. `rtc_query_frames_2016-05-01_to_2023-09.csv`
   - Contains the list of unique Sentinel-1 A/B frames that will generate the burst IDs from `rtc_query_bursts_2016-05-01_to_2023-09.csv`.
   - Use this file as the list of input granules for initial RTC-S1-STATIC product generation.
   - Contains 12475 Sentinel-1 granule IDs
   - Single column of data, without a column name at the top.  Column name should be:  `S1 Granule`


## CSLC-S1 Static Layers
Total of 2 files.

1. `cslc_query_bursts_2016-05-01_to_2023-09.csv`
   - Contains the unique CSLC-S1 bursts appearing in Sentinel-1 A/B data starting at May 1st 2016 through mid-September 2023.
   - Any bursts that ONLY appear in the Sentinel-1 record before May 2016 will not be included.
   - Contains 33057 burst IDs
   - File does not contain column names.  The column names should be:  `burst ID`, `First Time Seen`, `S1 Granule`
2. `cslc_query_frames_2016-05-01_to_2023-09.csv`
   - Contains the list of unique Sentinel-1 A/B frames that will generate the burst IDs from `cslc_query_bursts_2016-05-01_to_2023-09.csv`.
   - Use this file as the list of input granules for initial CSLC-S1-STATIC product generation.
   - Contains 1508 Sentinel-1 granule IDs
   - Single column of data, without a column name at the top.  Column name should be:  `S1 Granule`


