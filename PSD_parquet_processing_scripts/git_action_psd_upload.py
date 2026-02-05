# importing general Python libraries
import datetime as dt
import pytz
import json
import argparse
import boto3
from botocore.exceptions import ClientError
import tempfile

# importing orcasound_noise libraries
from orcasound_noise.pipeline.pipeline import NoiseAnalysisPipeline
from orcasound_noise.utils import Hydrophone
from orcasound_noise.utils.file_connector import S3FileConnector

# Bookmarking
class Bookmark:
    def __init__(self, hydrophone: Hydrophone, last_processed: dt.datetime = None):
        self.bookmark_path = f"s3://{hydrophone.value.save_bucket}/{hydrophone.value.save_folder}/{hydrophone.value.name}_bookmark.json"
        self.hydrophone = hydrophone.value.name
        self.bucket = hydrophone.value.save_bucket
        self.folder = hydrophone.value.save_folder
        self.last_processed = last_processed
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.client = boto3.client('s3')
    
    def update(self, new_time: dt.datetime):
        self.last_processed = new_time
        with open(f'{self.tmp_dir.name}/{self.hydrophone}_bookmark.json', 'w') as f:
            json.dump({'last_processed': self.last_processed.isoformat()}, f)
        self.client.upload_file(f'{self.tmp_dir.name}/{self.hydrophone}_bookmark.json', self.bucket, f'{self.folder}/{self.hydrophone}_bookmark.json')
        self.tmp_dir.cleanup()
    
    def load(self):
        try:
            self.client.head_object(Bucket=self.bucket, Key=f'{self.folder}/{self.hydrophone}_bookmark.json')
            self.client.download_file(self.bucket, f'{self.folder}/{self.hydrophone}_bookmark.json', f'{self.tmp_dir.name}/{self.hydrophone}_bookmark.json')
            with open(f'{self.tmp_dir.name}/{self.hydrophone}_bookmark.json', 'r') as f:
                data = json.load(f)
                self.last_processed = dt.datetime.fromisoformat(data['last_processed'])
            self.tmp_dir.cleanup()
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                print(f"No existing bookmark found for {self.hydrophone}. Starting fresh.")
                self.last_processed = None

def process_upload_psd(start_time: dt.datetime, end_time: dt.datetime, 
                       hydrophone: Hydrophone, bookmark: Bookmark, file_connector: S3FileConnector):
    
    """
    """
    pipeline = NoiseAnalysisPipeline(hydrophone,
                                     delta_f=1, bands=12,
                                     delta_t=1, mode='safe',
                                     )
    
    psd_path = pipeline.generate_parquet_file(start_time, 
                                        end_time, 
                                        upload_to_s3=True,
                                        partitioning=True)
    
    bookmark.update(end_time)

def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description='Process hydrophone audio data and generate PSD parquet files.')
    parser.add_argument('--hydrophone', 
                       type=str, 
                       default='bush_point',
                       choices=[h.value.name for h in Hydrophone][1:],
                       help='Hydrophone location to process')
    
    args = parser.parse_args()
    hydrophone = Hydrophone[args.hydrophone.upper()]
    
    now = dt.datetime.now(pytz.timezone('US/Pacific'))

    # Load Bookmark
    bookmark = Bookmark(hydrophone)
    bookmark.load()
    start_time = bookmark.last_processed or (now - dt.timedelta(hours=1))
    end_time = now

    file_connector = S3FileConnector(hydrophone)
    process_upload_psd(start_time, end_time, hydrophone, bookmark, file_connector)

if __name__ == '__main__':
    main()