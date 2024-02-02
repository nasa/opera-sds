This readme will describe the files contained within or beneath this current directory.

## DSWx-HLS Edge Cases
Total of 4 files.  All tiles within are from the first 6 months of 2022.
1. Unique MGRS tiles that intersect the antimeridian
   - hlss30_antimeridian_tileIDs_v1.txt
   - hlsl30_antimeridian_tileIDs_v1.txt
2. Unique MGRS tiles that are within ~200 km of the antimeridian.  Superset of the above two files.
   - hlsl30_near_antimeridian_tileIDs_v1.txt
   - hlss30_near_antimeridian_tileIDs_v1.txt


## DSWx-S1 Edge Cases
Total of 1 file.
1. RTC-S1 Granule names to use for running ADT's edge cases within the SDS.
   - RTC-S1_granules_for_edge_cases.txt
   - File is a table (CSV-style format, with "|" as column separator)
   - Column names:
     - RTC-S1 granule native-id
     - TileSetID
     - UTC Date
     - Burst ID
     - Notes
   - Each case can be run by starting an on-demand job, with native-id option, using the 'RTC-S1 granule native-id' field.
