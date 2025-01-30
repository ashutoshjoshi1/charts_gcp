import os
import time
import schedule
from google.cloud import storage
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Google Cloud Storage Configuration
bucket_name = 'pandora_diagnostic'
credentials_path = 'ornate-course-442519-s9-bfb13539fb48.json'
watched_dir = 'C:/Blick/data/diagnostic/'

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)

uploaded_files = set()  # Keep track of uploaded files


def upload_file(file_path):
    """Upload a single file to Google Cloud Storage."""
    if file_path in uploaded_files:  # Skip already uploaded files
        return
    
    file_name = os.path.basename(file_path)
    try:
        bucket_path = os.path.join('Pan002/', file_name)
        blob = bucket.blob(bucket_path)
        blob.upload_from_filename(file_path)
        uploaded_files.add(file_path)
        print(f"Uploaded {file_path} to {bucket_name}")
    except Exception as e:
        print(f"Failed to upload {file_path}: {e}")


def upload_all_existing_files():
    """Upload all .jpeg files in the watched directory before starting the watcher."""
    print("Checking for new files to upload before starting watcher...")
    for file_name in os.listdir(watched_dir):
        file_path = os.path.join(watched_dir, file_name)
        if os.path.isfile(file_path) and file_path.endswith('.jpeg'):
            upload_file(file_path)


class FileUploadHandler(FileSystemEventHandler):
    def on_created(self, event):
        """Upload a new file when it is created in the directory."""
        if not event.is_directory and event.src_path.endswith('.jpeg'):
            time.sleep(2)  # Allow time for the file to be fully written
            upload_file(event.src_path)


def run_uploader():
    """Runs the upload process: uploads existing files and starts the watcher."""
    upload_all_existing_files()  # Ensure all past files are uploaded first

    # Start watching for new files
    event_handler = FileUploadHandler()
    observer = Observer()
    observer.schedule(event_handler, path=watched_dir, recursive=False)

    print(f"Watching directory: {watched_dir} for new .jpeg files...")
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()


# **Scheduling Execution**
schedule.every().day.at("02:00").do(run_uploader)  # Runs at 2 AM
schedule.every().day.at("11:00").do(run_uploader)  # Runs at 11 AM
schedule.every().day.at("17:00").do(run_uploader)  # Runs at 5 PM

if __name__ == '__main__':
    print("Scheduler started... Running at 2 AM, 11 AM, and 5 PM")
    while True:
        schedule.run_pending()
        time.sleep(60)  # Wait one minute before checking again
