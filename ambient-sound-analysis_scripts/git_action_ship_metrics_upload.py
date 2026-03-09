
# importing general Python libraries
import datetime as dt
# import logging
# import pytz
import json
# import argparse
# import time
import tempfile

import boto3
import polars as pl

# importing data processing libraries
from orcasound_noise.analysis.partitioned_accessor import PartitionedAccessor
from orcasound_noise.utils import Hydrophone
from orcasound_noise.pipeline.pipeline import ShipAnalysisPipeline


from dotenv import load_dotenv

def update_bookmark(start_date: dt.datetime, end_date: dt.datetime) -> None:
    '''
    Update the bookmark with the last processed start and end dates.
    Args:
        start_date (dt.datetime): The last processed start date.
        end_date (dt.datetime): The last processed end date.
    '''

    save_bucket = "acoustic-sandbox"
    save_folder = "ambient-sound-analysis/ship_metrics"
    file_name = "ship_metrics_bookmark.json"
    s3_bookmark_path = f"{save_folder}/{file_name}"
    json_body = {
        'last_start_date': start_date.isoformat(),
        'last_end_date': end_date.isoformat()
    }
    boto3.client("s3") .put_object(
        Bucket=save_bucket,
        Key=s3_bookmark_path,
        Body=json.dumps(json_body),
        ContentType="application/json"
    )

def main():

    load_dotenv()

    print("M2 Data loading...")
    # For regular weekly data
    ship_pipeline = ShipAnalysisPipeline()
    lf_ais, lf_radar = ship_pipeline.get_raw_data_from_m2()
    print(f"M2 Data loaded.")
    
    print("Sound Data loading...")
    start_date, end_date = ship_pipeline.start_date, ship_pipeline.end_date
    start = dt.datetime.combine(start_date, dt.time.min)
    end = dt.datetime.combine(end_date, dt.time.max)
    ac_orcalab = PartitionedAccessor(Hydrophone.ORCASOUND_LAB, start, end)
    _, lf_bb = ac_orcalab.get_dataframes(lazy=True)

    if "comm_bb" not in lf_bb.collect_schema().names():
        lf_bb = lf_bb.with_columns(
            bb = pl.col("bb_o"),
            comm_bb = pl.lit(1) * pl.col("comm_bb_o"),
            ship_bb = pl.lit(1) * pl.col("ship_bb_o")
        )
    print(f"Sound Data Loaded.")

    print("Ship Metrics Calculating...")
    ship_pipeline.get_ship_metrics_parquet(lf_radar, lf_ais, lf_bb, 
                                             partitioning=True, upload_to_s3=True, 
                                             pqt_folder_override=None)
    print(f"Ship Metrics Calculated.")

    # update bookmark
    print(f"Updating bookmark with last processed dates: {start_date.isoformat()} to {end_date.isoformat()}")
    update_bookmark(start_date, end_date)


if __name__ == "__main__":
    main()
