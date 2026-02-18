#!/usr/bin/env python3
"""
Tool to map Sentinel-1 granules to RTC-S1 products using CMR queries.

This tool queries NASA's Common Metadata Repository (CMR) to find RTC-S1
products that were derived from input Sentinel-1 granules.
"""

import requests
import json
import argparse
from typing import List, Dict, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
import time
import sys

RTC_SHORTNAME = "OPERA_L2_RTC-S1_V1"
RTC_CONCEPT_ID = "C2777436413-ASF"

@dataclass
class S1Granule:
    """Represents a Sentinel-1 granule with parsed metadata."""

    granule_id: str
    satellite: str  # S1A or S1B
    mode: str  # IW, EW, etc.
    product_type: str  # SLC, GRD, etc.
    start_time: datetime
    end_time: datetime
    orbit: str

    @classmethod
    def from_granule_id(cls, granule_id: str):
        """
        Parse S1 granule ID to extract metadata.

        Format examples:
        - SLC:  S1A_IW_SLC__1SDV_20220310T121213_20220310T121240_042259_050962_1662
        - GRDH: S1A_IW_GRDH_1SDV_20220310T121213_20220310T121240_042259_050962_1662

        Note: SLC products have double underscores after product type, others have single.
        """
        parts = granule_id.split("_")

        # Remove empty strings caused by double underscores (e.g., in SLC products)
        parts = [p for p in parts if p]

        if len(parts) < 9:
            raise ValueError(f"Invalid S1 granule ID format: {granule_id}. Expected at least 9 parts, got {len(parts)}")

        satellite = parts[0]  # S1A or S1B
        mode = parts[1]  # IW, EW, SM, WV
        product_type = parts[2]  # SLC, GRDH, GRDM, etc.
        # parts[3] is polarization (1SDV, 1SSH, 1SDH, etc.)
        start_time_str = parts[4]  # e.g., 20220310T121213
        end_time_str = parts[5]  # e.g., 20220310T121240
        orbit = parts[6]  # Absolute orbit number
        # parts[7] is mission datatake ID
        # parts[8] is product unique ID

        start_time = datetime.strptime(start_time_str, "%Y%m%dT%H%M%S")
        end_time = datetime.strptime(end_time_str, "%Y%m%dT%H%M%S")

        return cls(
            granule_id=granule_id,
            satellite=satellite,
            mode=mode,
            product_type=product_type,
            start_time=start_time,
            end_time=end_time,
            orbit=orbit,
        )


@dataclass
class RTCMapping:
    """Represents the mapping between S1 and RTC-S1 granules."""

    s1_granule: str
    rtc_granules: List[str]  # Can be multiple RTC granules per S1
    found: bool
    cmr_metadata: Optional[List[Dict]] = None  # List of metadata for each RTC granule
    error: Optional[str] = None


class S1ToRTCMapper:
    """Maps Sentinel-1 granules to RTC-S1 products using CMR."""

    CMR_SEARCH_URL = "https://cmr.earthdata.nasa.gov/search/granules.umm_json"


    def __init__(self):
        self.session = requests.Session()

    def query_cmr_for_rtc(self, s1_granule: S1Granule) -> List[Dict]:
        """
        Query CMR for RTC-S1 granules corresponding to S1 granule.
        Queries by the S1 start/end times and filters results to ensure
        the S1 granule is in the InputGranules metadata.

        Args:
            s1_granule: Parsed S1 granule object

        Returns:
            List of CMR metadata dicts (can be empty, one, or multiple results)
        """
        # Build CMR query parameters
        # Query by the exact temporal range of the S1 granule
        params = {
            "collection_concept_id": RTC_CONCEPT_ID,
            "page_size": 2000,  # May return multiple results
        }

        # Format times for CMR query (ISO 8601)
        start_time_str = s1_granule.start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_time_str = s1_granule.end_time.strftime("%Y-%m-%dT%H:%M:%SZ")

        # Query by temporal range that overlaps with S1 acquisition
        params["temporal"] = f"{start_time_str},{end_time_str}"

        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                response = self.session.get(self.CMR_SEARCH_URL, params=params, timeout=30)
                response.raise_for_status()

                data = response.json()
                entries = data.get("items", [])

                # Filter entries to find those that have our S1 granule in InputGranules
                matching_entries = []

                for entry in entries:
                    # Check if this RTC granule was derived from our S1 granule
                    # Look in the InputGranules metadata
                    input_granules = self._extract_input_granules(entry)
                    # Check if our S1 granule ID is in the input granules list
                    if s1_granule.granule_id in input_granules:
                        matching_entries.append(entry)

                return matching_entries

            except requests.exceptions.RequestException as e:
                if attempt < max_attempts - 1:
                    time.sleep(0.05)
                    continue
                else:
                    print(f"Error querying CMR for {s1_granule.granule_id}: {e}")
                    return []

    def _extract_input_granules(self, cmr_entry: Dict) -> List[str]:
        """
        Extract InputGranules from CMR metadata entry.

        Args:
            cmr_entry: CMR metadata entry

        Returns:
            List of input granule IDs
        """
        return cmr_entry.get("umm", {}).get("InputGranules", [])

    def map_granules(self, s1_granule_ids: List[str]) -> List[RTCMapping]:
        """
        Map a list of S1 granules to RTC-S1 products.
        Makes one CMR query per S1 granule, returns 0-N RTC granules per S1.

        Args:
            s1_granule_ids: List of S1 granule IDs

        Returns:
            List of RTCMapping objects (each may contain multiple RTC granules)
        """
        mappings = []

        print(f"Querying CMR for {len(s1_granule_ids)} granules...")

        for i, granule_id in enumerate(s1_granule_ids, 1):
            try:
                # Parse S1 granule ID
                s1_granule = S1Granule.from_granule_id(granule_id)

                # Query CMR for corresponding RTC products
                print(f"  [{i}/{len(s1_granule_ids)}] Querying {granule_id[:50]}...", end="", flush=True)
                rtc_entries = self.query_cmr_for_rtc(s1_granule)

                if rtc_entries:
                    rtc_granule_ids = [entry.get("meta", {}).get("native-id", "Unknown") for entry in rtc_entries]
                    print(f" FOUND {len(rtc_entries)} RTC granule(s)")
                    mappings.append(
                        RTCMapping(
                            s1_granule=granule_id, rtc_granules=rtc_granule_ids, found=True, cmr_metadata=rtc_entries
                        )
                    )
                else:
                    print(f" NOT FOUND")
                    mappings.append(RTCMapping(s1_granule=granule_id, rtc_granules=[], found=False))

                time.sleep(0.05)

            except Exception as e:
                print(f" ERROR: {e}")
                mappings.append(RTCMapping(s1_granule=granule_id, rtc_granules=[], found=False, error=str(e)))

        found_count = sum(1 for m in mappings if m.found)
        total_rtc = sum(len(m.rtc_granules) for m in mappings)
        print(f"\nCompleted: {found_count}/{len(mappings)} S1 granules mapped to {total_rtc} RTC granules")

        return mappings

    def print_report(self, mappings: List[RTCMapping]):
        """Print a summary report of the mappings."""
        print("\n" + "=" * 80)
        print("S1 to RTC-S1 Mapping Report")
        print("=" * 80 + "\n")

        found_count = sum(1 for m in mappings if m.found)
        missing_count = len(mappings) - found_count
        total_rtc = sum(len(m.rtc_granules) for m in mappings)

        print(f"Total S1 granules: {len(mappings)}")
        print(f"S1 granules with RTC products: {found_count}")
        print(f"S1 granules without RTC products: {missing_count}")
        print(f"Total RTC granules found: {total_rtc}")
        print("\n" + "-" * 80 + "\n")

        if found_count > 0:
            print("FOUND RTC-S1 GRANULES:")
            print("-" * 80)
            for mapping in mappings:
                if mapping.found:
                    print(f"\nS1:  {mapping.s1_granule}")
                    if len(mapping.rtc_granules) == 1:
                        print(f"RTC: {mapping.rtc_granules[0]}")
                    else:
                        print(f"RTC: {len(mapping.rtc_granules)} granules:")
                        for rtc_id in mapping.rtc_granules:
                            print(f"     - {rtc_id}")

        if missing_count > 0:
            print("\n\nMISSING RTC-S1 GRANULES:")
            print("-" * 80)
            for mapping in mappings:
                if not mapping.found:
                    print(f"S1: {mapping.s1_granule}")
                    if mapping.error:
                        print(f"    Error: {mapping.error}")

        print("\n" + "=" * 80 + "\n")


def parse_args():
    """Parse command line arguments."""
    script_dir = Path(__file__).resolve().parent
    default_output = script_dir / "rtc_mapping_results.json"
    default_missing = script_dir / "missing_rtc.json"
    
    parser = argparse.ArgumentParser(
        description="Map Sentinel-1 granules to RTC-S1 products using CMR queries.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
            Examples:
            %(prog)s s1_granules.txt
            %(prog)s /path/to/granules.txt --output results.json
            
            By default, output files are saved to the same directory as this script.
            """
    )
    
    parser.add_argument(
        "input_file",
        type=str,
        help="Path to text file containing S1 granule IDs (one per line)"
    )
    
    parser.add_argument(
        "-o", "--output",
        type=str, 
        default=str(default_output),
        help=f"Output JSON file for mapping results (default: {default_output.name} in script directory)"
    )
    
    parser.add_argument(
        "-m", "--missing-output",
        type=str,
        default=str(default_missing),
        help=f"Output JSON file for missing granules (default: {default_missing.name} in script directory)"
    )
    
    parser.add_argument(
        "--no-report",
        action="store_true",
        help="Skip printing the summary report"
    )
    
    return parser.parse_args()


def main():
    """Main entry point for CLI tool."""
    args = parse_args()
    
    # Validate input file exists
    input_path = Path(args.input_file)
    if not input_path.exists():
        print(f"Error: Input file not found: {args.input_file}", file=sys.stderr)
        sys.exit(1)
    
    # Read granules from input file
    try:
        with open(input_path, "r") as f:
            s1_granules = [line.strip() for line in f if line.strip()]
    except Exception as e:
        print(f"Error reading input file: {e}", file=sys.stderr)
        sys.exit(1)
    
    if not s1_granules:
        print(f"Error: No granules found in {args.input_file}", file=sys.stderr)
        sys.exit(1)
    
    print(f"Loaded {len(s1_granules)} granules from {args.input_file}")

    # Create mapper and process granules
    mapper = S1ToRTCMapper()
    mappings = mapper.map_granules(s1_granules)

    # Print report (unless disabled)
    if not args.no_report:
        mapper.print_report(mappings)

    # Save results to JSON
    results = {
        "mappings": [
            {
                "s1_granule": m.s1_granule,
                "rtc_granules": m.rtc_granules,  # Now a list
                "rtc_count": len(m.rtc_granules),
                "found": m.found,
                "error": m.error,
            }
            for m in mappings
        ],
        "summary": {
            "total_s1_granules": len(mappings),
            "s1_with_rtc": sum(1 for m in mappings if m.found),
            "s1_without_rtc": sum(1 for m in mappings if not m.found),
            "total_rtc_granules": sum(len(m.rtc_granules) for m in mappings),
        },
    }

    try:
        with open(args.output, "w") as f:
            json.dump(results, f, indent=2)
        print(f"Results saved to {args.output}")
    except Exception as e:
        print(f"Error saving results: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Save missing granules
    missing_granules = [m.s1_granule for m in mappings if len(m.rtc_granules) == 0]
    
    if len(missing_granules) > 0:
        try:
            with open(args.missing_output, "w") as f:
                json.dump(missing_granules, f, indent=2)
            print(f"Missing granules saved to {args.missing_output}")
        except Exception as e:
            print(f"Error saving missing granules: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        print("No missing granules to save")


if __name__ == "__main__":
    main()