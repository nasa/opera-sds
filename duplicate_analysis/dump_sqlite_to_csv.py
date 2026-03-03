#!/usr/bin/env python3
"""
Module to convert SQLite database files to CSV format.

This module provides functionality to read SQLite database files and export
their contents to CSV format. It can be used both as a command-line script
and as an importable Python module.

Usage:
  As a command-line script:
    python dump_sqlite_to_csv.py input.sqlite [output.csv]

  As a Python module:
    import dump_sqlite_to_csv
    dump_sqlite_to_csv.convert_sqlite_to_csv('input.sqlite', 'output.csv')
"""

import sqlite3
import csv
import os
import sys
from typing import Optional, List, Dict, Any

def convert_sqlite_to_csv(
    sqlite_file: str,
    csv_file: Optional[str] = None,
    table_name: Optional[str] = None,
    delimiter: str = ',',
    quotechar: str = '"',
    escapechar: Optional[str] = None
) -> None:
    """
    Convert SQLite database to CSV format.

    Args:
        sqlite_file: Path to the SQLite database file
        csv_file: Path to the output CSV file. If None, uses same name as sqlite_file
                 but with .csv extension.
        table_name: Name of the table to export. If None, exports all tables.
        delimiter: Delimiter character for CSV
        quotechar: Quote character for CSV
        escapechar: Escape character for CSV (None for default)

    Returns:
        None

    Raises:
        FileNotFoundError: If sqlite_file doesn't exist
        sqlite3.Error: If there's an error accessing the database
    """
    # Validate input file
    if not os.path.exists(sqlite_file):
        raise FileNotFoundError(f"SQLite file not found: {sqlite_file}")

    # Determine output file if not specified
    if csv_file is None:
        csv_file = os.path.splitext(sqlite_file)[0] + '.csv'

    # Connect to SQLite database
    conn = sqlite3.connect(sqlite_file)
    cursor = conn.cursor()

    try:
        # Get list of tables if table_name not specified
        if table_name is None:
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [row[0] for row in cursor.fetchall() if row[0] != 'sqlite_sequence']
        else:
            tables = [table_name]

        # Process each table
        for table in tables:
            try:
                # Get table data
                cursor.execute(f"SELECT * FROM {table};")
                rows = cursor.fetchall()

                # Get column names
                cursor.execute(f"PRAGMA table_info({table});")
                columns = [row[1] for row in cursor.fetchall()]

                # Write to CSV
                with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f, delimiter=delimiter, quotechar=quotechar,
                                      escapechar=escapechar, quoting=csv.QUOTE_MINIMAL)

                    # Write header
                    writer.writerow(columns)

                    # Write data rows
                    for row in rows:
                        writer.writerow(row)

                print(f"Exported table '{table}' to {csv_file}")

            except sqlite3.Error as e:
                print(f"Warning: Could not export table '{table}': {e}", file=sys.stderr)
                continue

    finally:
        conn.close()

def get_table_names(sqlite_file: str) -> List[str]:
    """
    Get list of table names from SQLite database.

    Args:
        sqlite_file: Path to the SQLite database file

    Returns:
        List of table names

    Raises:
        FileNotFoundError: If sqlite_file doesn't exist
        sqlite3.Error: If there's an error accessing the database
    """
    if not os.path.exists(sqlite_file):
        raise FileNotFoundError(f"SQLite file not found: {sqlite_file}")

    conn = sqlite3.connect(sqlite_file)
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall() if row[0] != 'sqlite_sequence']
        return tables
    finally:
        conn.close()

def main():
    """Command-line interface for the module."""
    import argparse

    parser = argparse.ArgumentParser(
        description='Convert SQLite database to CSV format',
        epilog='Example: python dump_sqlite_to_csv.py database.sqlite output.csv'
    )

    parser.add_argument(
        'sqlite_file',
        help='Path to the SQLite database file'
    )

    parser.add_argument(
        'csv_file',
        nargs='?',
        help='Path to the output CSV file (optional)'
    )

    parser.add_argument(
        '-t', '--table',
        help='Specific table to export (default: all tables)'
    )

    parser.add_argument(
        '-d', '--delimiter',
        default=',',
        help='Delimiter character for CSV (default: comma)'
    )

    parser.add_argument(
        '-q', '--quotechar',
        default='"',
        help='Quote character for CSV (default: double quote)'
    )

    args = parser.parse_args()

    try:
        convert_sqlite_to_csv(
            args.sqlite_file,
            args.csv_file,
            args.table,
            args.delimiter,
            args.quotechar
        )
        print("Conversion completed successfully!")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()