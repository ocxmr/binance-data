
import csv
import os
from pathlib import Path

def add_aggtrades_column():
    data_dir = Path("data")
    for root, _, files in os.walk(data_dir):
        for file in files:
            if file.endswith(".csv"):
                file_path = Path(root) / file
                print(f"Processing {file_path}")
                rows = []
                with file_path.open("r", newline="") as f:
                    reader = csv.reader(f)
                    header = next(reader)
                    if "data_type" in header:
                        print(f"Skipping {file_path}, already has data_type column.")
                        continue
                    header.insert(2, "data_type")
                    rows.append(header)
                    for row in reader:
                        row.insert(2, "aggTrades")
                        rows.append(row)

                with file_path.open("w", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerows(rows)
                print(f"Finished processing {file_path}")

if __name__ == "__main__":
    add_aggtrades_column()
