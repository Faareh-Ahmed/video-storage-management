# Video Storage Management Microservice

This microservice provides APIs to manage video storage for authenticated users. It enables video uploads, deletions, and storage status tracking. The microservice integrates with a User Account Management Service for authentication and authorization.

## Features
- **Upload Video**: Uploads video files to Google Cloud Storage.
- **Delete Video**: Deletes video files from Google Cloud Storage.
- **Check Storage Status**: Retrieves the user's current storage usage and details.

## Requirements

### System Requirements
- Python 3.8+
- Flask 2.0+
- MongoDB for user storage management
- Google Cloud Storage bucket

### Python Libraries
- `flask`
- `google-cloud-storage`
- `pymongo`
- `requests`

### Environment Setup
1. Create a Google Cloud Storage bucket and ensure the associated service account has the required permissions (e.g., `Storage Object Admin`).
2. Set up a MongoDB database to store user storage details.
3. Integrate the User Account Management Service for token validation.

## API Endpoints

### 1. Upload Video
**Endpoint:** `/upload`  
**Method:** `POST`

**Headers:**
- `Authorization`: `Bearer <user_token>`

**Body (Form Data):**
- `file`: Video file to be uploaded.

**Responses:**
- **200 OK**: Video uploaded successfully.
  ```json
  {
      "message": "File uploaded successfully",
      "storage_80_alert": false,
      "bandwidth_80_alert": false,
      "storage_percentage_used": <percentage>,
      "bandwidth_checks": {
          "bandwidth_limit_approaching": <boolean>,
          "bandwidth_limit_exceeded": <boolean>
      }
  }
  ```
- **400 Bad Request**: Missing file.
  ```json
  {
      "error": "No file part"
  }
  ```
- **401 Unauthorized**: Invalid or missing token.
  ```json
  {
      "error": "Unauthorized"
  }
  ```
- **403 Forbidden**: Exceeds storage limit or bandwidth limit.
  ```json
  {
      "error": "Exceeds storage limit, cannot upload video..!!"
  }
  ```
  ```json
  {
      "error": "Bandwidth limit exceeded or some internal upload error"
  }
  ```

### 2. Delete Video
**Endpoint:** `/delete-file`  
**Method:** `DELETE`

**Headers:**
- `Authorization`: `Bearer <user_token>`

**Body (JSON):**
- `filename`: Name of the file to be deleted.

**Responses:**
- **200 OK**: File deleted successfully.
  ```json
  {
      "message": "File deleted successfully",
      "email": "<user_email>",
      "file_deleted": "<filename>",
      "file_size": <size>,
      "timestamp": "<timestamp>",
      "date": "<date>",
      "updated_deletion_volume": <updated_volume>,
      "total_deletion_count": <count>
  }
  ```
- **400 Bad Request**: Missing filename.
  ```json
  {
      "error": "No filename provided"
  }
  ```
- **401 Unauthorized**: Invalid or missing token.
  ```json
  {
      "error": "Unauthorized"
  }
  ```
- **500 Internal Server Error**: Error during deletion.
  ```json
  {
      "error": "<error_message>"
  }
  ```


### 3. Get Storage Status
**Endpoint:** `/storage-status`  
**Method:** `GET`

**Headers:**
- `Authorization`: `Bearer <user_token>`

**Responses:**
- **200 OK**: Returns storage status.
  ```json
  {
      "total_storage": 52428800,
      "used_storage": 10485760,
      "storage_percentage": 20.0,
      "files": [
          {
              "filename": "username/video.mp4",
              "size": 10485760
          }
      ]
  }
  ```
- **401 Unauthorized**: Invalid or missing token.
  ```json
  {
      "error": "Unauthorized"
  }
  ```

## Code Structure
- **`upload_blueprint`**: Handles video uploads.
- **`delete_blueprint`**: Handles video deletions.
- **`status_blueprint`**: Handles storage status retrieval.
- **`UserService`**: Validates user tokens via the User Account Management Service.
- **`GCSService`**: Interfaces with Google Cloud Storage for file operations.
- **`MongoService`**: Manages user storage data in MongoDB.

## Deployment

### Local Development
1. Set up environment variables:
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS="path/to/credentials.json"
   export MONGO_URI="mongodb://localhost:27017/"
   export USER_SERVICE_URL="http://user-service-url"
   ```
2. Run the Flask app locally:
   ```bash
   flask run
   ```

### Deployment on GCP
1. Deploy the application using Cloud Run, Compute Engine, or Kubernetes.
2. Assign the appropriate service account to the environment with permissions for:
   - Google Cloud Storage.
   - MongoDB access.
   - User Service token validation.
3. Update the `USER_SERVICE_URL` environment variable to point to the deployed User Service.

## Additional Notes
- **Storage Alerts**: If a user's storage exceeds 80%, an alert is sent.
- **Default Storage**: Each user is initialized with 50MB of storage. Adjust as needed in `mongo_service.initialize_user_storage`.

---
For more details or contributions, please open an issue or submit a pull request.

    '''
        // Basic streaming request
        fetch('/stream/myfile.mp4', {
            headers: {
                'Authorization': 'Bearer your-token-here'
            }
        });

        // Range request (e.g., for video seeking)
        fetch('/stream/myfile.mp4', {
            headers: {
                'Authorization': 'Bearer your-token-here',
                'Range': 'bytes=1000-2000'
            }
        });
    
    '''