from google.cloud import storage
from config import CLOUD_PROJECT_ID
from io import BytesIO
from google.oauth2 import service_account
import datetime


credentials = service_account.Credentials.from_service_account_file(
    r'C:\Users\user\OneDrive\Desktop\5thSemester\CloudComputing\projects\video-storage-management\first-scout-444113-h2-72968e1f4f18.json')

class GCSService:
    def __init__(self, bucket_name):
        self.client = storage.Client(project=CLOUD_PROJECT_ID, credentials = credentials)
        self.bucket = self.client.bucket(bucket_name)

    def upload_file(self, filename, file):
        blob = self.bucket.blob(filename)
        blob.upload_from_file(file)

    def delete_file(self, filename):
        blob = self.bucket.blob(filename)
        blob.delete()
    
    def get_streaming_blob(self, filename):
        """
        Returns a blob object for streaming if it exists
        """
        blob = self.bucket.get_blob(filename)
        if blob.exists():
            return blob
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

    def generate_download_signed_url_v4(self, blob_name):
        """Generates a v4 signed URL for downloading a blob.

        Note that this method requires a service account key file. You can not use
        this if you are using Application Default Credentials from Google Compute
        Engine or from the Google Cloud SDK.
        """
        # bucket_name = 'your-bucket-name'
        # blob_name = 'your-object-name'

        blob = self.bucket.blob(blob_name)

        url = blob.generate_signed_url(
            version="v4",
            # This URL is valid for 15 minutes
            expiration=datetime.timedelta(minutes=15),
            # Allow GET requests using this URL.
            method="GET",
        )

        print("Generated GET signed URL:")
        print(url)
        print("You can use this URL with any user agent, for example:")
        print(f"curl '{url}'")
        return url