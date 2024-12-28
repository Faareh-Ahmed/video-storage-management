from flask import Blueprint, request, jsonify, send_file
from services.user_service import UserService
from services.track_service import TrackService
import io
from flask import Response,send_file
import os
def download_blueprint(gcs_service, mongo_service, user_service_url):
    download_bp = Blueprint('download', __name__)

    @download_bp.route('/download/disk/<filename>', methods=['GET'])
    def download_to_client(filename):
        # Get the token from the request headers
        token = request.headers.get('Authorization', '').split(' ')[-1]
        user = UserService.validate_token(token, user_service_url)

        if not user:
            return jsonify({"error": "Unauthorized"}), 401

        email = user['email']
        username = user['username']
        
        # Check if the user has the file in their storage
        user_storage = mongo_service.find_user_storage(email)

        if not user_storage:
            return jsonify({"error": "User storage not found"}), 404

        # Check if the file is in the user's files list
        file_record = next((file for file in user_storage.get('files', []) if file['filename'] == filename), None)
        
        if not file_record:
            return jsonify({"error": "File not found in user's storage"}), 404

        # Download the file from GCS to a temporary path
        destination_path = f"/tmp/{filename}"  # Example temporary storage location
        downloaded_file = gcs_service.download_to_disk(filename, destination_path)
        
        if not downloaded_file:
            return jsonify({"error": "File not found in storage"}), 404

        # Send the file to the client for download
        response = send_file(destination_path, as_attachment=True)

        # Optionally, clean up the temporary file after sending
        @response.call_on_close
        def cleanup_temp_file():
            try:
                os.remove(destination_path)
            except Exception as e:
                print(f"Error cleaning up temporary file: {e}")

        return response
    
    @download_bp.route('/download/stream/<filename>',methods=['GET'])
    def stream_file(filename):
        # Get the token from the request headers
        token = request.headers.get('Authorization', '').split(' ')[-1]
        user = UserService.validate_token(token, user_service_url)

        if not user:
            return jsonify({"error": "Unauthorized"}), 401

        email = user['email']
        username = user['username']
        
        # Check if the user has the file in their storage
        user_storage = mongo_service.find_user_storage(email)

        if not user_storage:
            return jsonify({"error": "User storage not found"}), 404

        # Check if the file is in the user's files list
        file_record = next((file for file in user_storage.get('files', []) if file['filename'] == filename), None)
        
        if not file_record:
            return jsonify({"error": "File not found in user's storage"}), 404

        # Track the download activity
        # stream_record = TrackService.log_stream(token, filename)

        # if not stream_record:
        #     return jsonify({"error": "Download record couldn't be inserted or updated..!!"}), 400

        # stream the file from GCS
        file_stream = gcs_service.stream_file(filename)
        if not file_stream:
            return jsonify({"error": "File not found"}), 404

        # Determine the appropriate MIME type for the file
        mimetype = "video/mp4"  # Example: Adjust based on the file type
        
        # Stream the file to the client
        return Response(file_stream, mimetype=mimetype)
    return download_bp
