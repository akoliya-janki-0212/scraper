import csv
import os
from pathlib import Path

INPUT_DIR = Path("/Users/yashprajapati/scrap/merge")
OUTPUT_FILE = Path("/Users/yashprajapati/scrap/merge/combined.csv")

def combine_csvs(input_dir: Path, output_file: Path):
    csv_files = sorted(
        f for f in input_dir.glob("*.csv")
        if f.name != output_file.name
    )
    if not csv_files:
        raise FileNotFoundError("No CSV files found")
    if output_file.exists():
        output_file.unlink()

    header_written = False

    with output_file.open("w", newline="", encoding="utf-8") as fout:
        writer = None

        for file in csv_files:
            with file.open("r", newline="", encoding="utf-8") as fin:
                reader = csv.DictReader(fin)

                if reader.fieldnames is None:
                    continue

                # Remove 4th column (index 3) safely using field name
                fieldnames = list(reader.fieldnames)
                if len(fieldnames) > 3:
                    removed_field = fieldnames.pop(3)
                else:
                    removed_field = None

                if not header_written:
                    writer = csv.DictWriter(
                        fout,
                        fieldnames=fieldnames,
                        quoting=csv.QUOTE_MINIMAL,
                        lineterminator="\n"
                    )
                    writer.writeheader()
                    header_written = True

                for row in reader:
                    if removed_field and removed_field in row:
                        row.pop(removed_field, None)
                    writer.writerow(row)

if __name__ == "__main__":
    combine_csvs(INPUT_DIR, OUTPUT_FILE)