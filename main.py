import os
from google.cloud import storage
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

bucket_name = 'pandora_diagnostic'
credentials_path = 'ornate-course-442519-s9-bfb13539fb48.json'

watched_dir = 'C:/Users/Mini-Pan/Desktop/GCP/logfiles/'

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)


class FileUploadHandler(FileSystemEventHandler):
    def on_created(self, event):
        # Check if the created file is a .jpeg file
        if not event.is_directory and event.src_path.endswith('.jpeg'):
            file_path = event.src_path
            file_name = os.path.basename(file_path)
            try:
                bucket_path = os.path.join('Pan002/', file_name)
                blob = bucket.blob(bucket_path)
                blob.upload_from_filename(file_path)
                print(f"Uploaded {file_path} to {bucket_name}")
            except Exception as e:
                print(f"Failed to upload {file_path}: {e}")


def main():
    event_handler = FileUploadHandler()
    observer = Observer()
    observer.schedule(event_handler, path=watched_dir, recursive=False)

    print(f"Watching directory: {watched_dir} for new .jpeg files...")
    observer.start()

    try:
        while True:
            time.sleep(1)  # Keep the script running
    except KeyboardInterrupt:
        observer.stop()

    observer.join()


if __name__ == '__main__':
    main()
