# S1 to RTC Mapper

Tool to map Sentinel-1 granules to RTC-S1 products using NASA's Common Metadata Repository (CMR).

## Usage

```bash
python s1_to_rtc_mapper.py <path/to/input_file.txt>
```

### Input

Text file with one Sentinel-1 granule ID per line:
```
S1A_IW_SLC__1SDV_20220310T121213_20220310T121240_042259_050962_1662
S1A_IW_SLC__1SDV_20220326T001026_20220326T001053_042485_051115_D32A
S1A_IW_SLC__1SDV_20220710T171836_20220710T171854_044041_0541C3_2C8D
```

### Output

Two JSON files saved to this directory:
- `rtc_mapping_results.json` - Complete mapping results with metadata
- `missing_rtc.json` - List of S1 granules without RTC products

### Options

```
-o, --output PATH          Custom path for results file
-m, --missing-output PATH  Custom path for missing granules file
--no-report               Skip printing summary report
-h, --help                Show help message
```
