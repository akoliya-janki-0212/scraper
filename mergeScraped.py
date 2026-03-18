import pandas as pd
import glob
import os
from urllib.parse import urlparse

FOLDER_PATH = "/Users/yashprajapati/scrap/cm"
OUTPUT_FILE = os.path.join(FOLDER_PATH, "merged_output.csv")

def extract_competitor(url):
    try:
        domain = urlparse(str(url)).netloc.lower()
        domain = domain.replace("www.", "")
        return domain.split(".")[0]
    except:
        return ""

def merge_csv(folder):
    csv_files = glob.glob(os.path.join(folder, "*.csv"))

    REQUIRED_COLUMNS = [
        "Ref Product URL",
        "Ref Product ID",
        "Ref Variant ID",
        "Ref Price",
        "Ref Main Image",
        "Ref Quantity",
        "Ref Status",
        "Date Scraped",
        "Competitor",
        "Ref SKU",
        "Ref MPN",
        "Ref GTIN",
        "Ref Category",
        "Ref Category URL",
        "Ref Group Attr 1",
        "Ref Group Attr 2",
        "Ref Brand Name",
        "Ref Product Name",
    ]

    COLUMN_ALIASES = {
        "Ref Varient ID": "Ref Variant ID",
        "Ref Variant ID": "Ref Variant ID",
        "Date Scrapped": "Date Scraped",
        "Date Scraped": "Date Scraped",
    }

    first_write = True

    for f in csv_files:
        for chunk in pd.read_csv(f, dtype=str, chunksize=50000):
            # normalize column names
            chunk.rename(columns=COLUMN_ALIASES, inplace=True)

            # drop garbage columns like Unnamed
            chunk = chunk.loc[:, ~chunk.columns.str.contains('^Unnamed')]

            # derive competitor from Ref Product URL
            if "Ref Product URL" in chunk.columns:
                chunk["Competitor"] = chunk["Ref Product URL"].apply(extract_competitor)

            # keep only required columns
            chunk = chunk[[c for c in REQUIRED_COLUMNS if c in chunk.columns]]

            # ensure missing columns exist so final CSV is consistent
            for col in REQUIRED_COLUMNS:
                if col not in chunk.columns:
                    chunk[col] = ""

            # reorder columns
            chunk = chunk[REQUIRED_COLUMNS]

            chunk.to_csv(
                OUTPUT_FILE,
                mode="w" if first_write else "a",
                index=False,
                header=first_write
            )

            first_write = False

    print(f"Saved -> {OUTPUT_FILE}")

merge_csv(FOLDER_PATH)