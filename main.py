import os
import time
from google.cloud import storage
import schedule

bucket_name = 'pandora_diagnostic'
credentials_path = 'ornate-course-442519-s9-bfb13539fb48.json'

watched_dir = 'C:/Users/Mini-Pan/Desktop/GCP/logfiles/'

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)

last_modified_times = {}

def upload_files_to_gcp():
    for file_name in os.listdir(watched_dir):
        # Filter only .jpeg files
        if file_name.endswith('.jpeg'):
            file_path = os.path.join(watched_dir, file_name)
            if os.path.isfile(file_path):
                current_modified_time = os.path.getmtime(file_path)

                if file_name not in last_modified_times or last_modified_times[file_name] < current_modified_time:
                    bucket_path = os.path.join('Pan002/', file_name)
                    blob = bucket.blob(bucket_path)
                    blob.upload_from_filename(file_path)
                    print(f"Uploaded {file_path} to {bucket_name}")

                    last_modified_times[file_name] = current_modified_time

def main():
    # Schedule the upload task to run every hour
    schedule.every(1).hour.do(upload_files_to_gcp)
    
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == '__main__':
    main()