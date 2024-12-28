from google.cloud import storage
from config import CLOUD_PROJECT_ID
from io import BytesIO

class GCSService:
    def __init__(self, bucket_name):
        self.client = storage.Client(project=CLOUD_PROJECT_ID)
        self.bucket = self.client.bucket(bucket_name)

    def upload_file(self, filename, file):
        blob = self.bucket.blob(filename)
        blob.upload_from_file(file)

    def delete_file(self, filename):
        blob = self.bucket.blob(filename)
        blob.delete()
    
    def stream_file(self, filename):
        """
        Streams file content from GCS as an in-memory binary stream (BytesIO).
        """
        blob = self.bucket.blob(filename)
        if blob.exists():
            file_stream = BytesIO()
            blob.download_to_file(file_stream)
            file_stream.seek(0)  # Reset stream position to the beginning
            return file_stream
        return None

    def download_to_disk(self, filename, destination_path):
        """
        Downloads file content from GCS and saves it directly to the given path on disk.
        """
        blob = self.bucket.blob(filename)
        if blob.exists():
            blob.download_to_filename(destination_path)
            return destination_path
        return None
