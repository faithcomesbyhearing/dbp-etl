import os
import time
import boto3
import json
import requests
from datetime import datetime, timedelta, timezone

# Global variable for the s3zipper base URL
S3ZIPPER_BASE_URL = "https://api.s3zipper.com"
MAX_WAIT_SECONDS = 3600  # 1 hour
POLL_INTERVAL = 2
STS_EXPIRATION_TIME = 3600  # 1 hour

class S3ZipperService:
    """
    A class to interact with the s3zipper API for zipping files in an S3 bucket.
    This class handles obtaining a Bearer token from s3zipper, fetching temporary AWS STS credentials,
    and creating a zip file of specified files in an S3 bucket. It polls the s3zipper API until the zip
    process is complete (either SUCCESS or FAILURE).
    """
    def __init__(self, s3zipper_user_key, s3zipper_user_secret, s3_client, region='us-west-2'):
        """
        :param s3zipper_user_key: Username or key used for fetching the s3zipper token.
        :param s3zipper_user_secret: Password or secret used for fetching the s3zipper token.
        :param region: AWS region where your S3 bucket resides (default 'us-west-2').
        """
        self.s3zipper_user_key = s3zipper_user_key
        self.s3zipper_user_secret = s3zipper_user_secret
        self.region = region
        
        # We'll store the Bearer token for s3zipper and AWS STS creds here
        self.s3zipper_token = None
        self.aws_access_key_id = None
        self.aws_secret_access_key = None
        self.aws_session_token = None
        self.aws_creds_expiration = None  # a datetime object

        # Pre-fetch the s3zipper token on init
        self.get_s3zipper_token()

        self.s3_client = s3_client
        self._last_progress_print = None

    def get_s3zipper_token(self):
        """Fetch a Bearer token from s3zipper /tokenv2 endpoint."""
        url = f"{S3ZIPPER_BASE_URL}/tokenv2"
        # Depending on your s3zipper’s auth mechanism, you might send JSON or form data.
        # Here we assume form data with s3zipper_user_key and user_secret:
        data = {
            "userKey": self.s3zipper_user_key,
            "userSecret": self.s3zipper_user_secret
        }

        try:
            resp = requests.post(url, data=data, timeout=10)
            resp.raise_for_status()
        except requests.RequestException as e:
            raise RuntimeError(f"Failed to retrieve s3zipper token: {e}")

        token_data = resp.json()  # Should contain { "token": "BearerTokenHere" } or similar
        self.s3zipper_token = token_data.get("token")
        if not self.s3zipper_token:
            raise ValueError("No token returned from s3zipper /tokenv2")

    def get_temp_sts_credentials(self):
        """
        Retrieves AWS temporary security credentials from environment variables.

        This method looks for AWS credentials in the following environment variables:
        - AWS_ACCESS_KEY_ID
        - AWS_SECRET_ACCESS_KEY
        - AWS_SESSION_TOKEN

        These credentials are typically provided by ECS when running in a container
        with a Task Role. The credentials are stored as instance attributes.

        Returns:
            None

        Side effects:
            Sets the following instance attributes:
            - self.aws_access_key_id
            - self.aws_secret_access_key
            - self.aws_session_token
        """
        self.aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
        self.aws_session_token = os.environ.get('AWS_SESSION_TOKEN')

    def create_and_upload_file_mapper(self, bucket_name, file_paths, zip_output_prefix):
        """
        Extracts the directory names from all file_paths, creates a custom file mapper JSON that maps each directory
        to the root directory "/", uploads it to the S3 bucket in the same directory as zip_output_prefix,
        and returns the S3 key.

        A timestamp suffix is added to the fileMapper.json name to avoid overwrites when executed in parallel.

        Example output JSON:
        {
            "paths": [
                { "key": "etl-development-input/ENGESVN2DA/", "name": "/" },
                { "key": "/", "name": "/" }
            ]
        }
        """
        if not file_paths:
            raise ValueError("file_paths list is empty.")

        # Extract unique directory names from file_paths (ensuring each ends with a trailing slash).
        directories = set()
        for file_path in file_paths:
            if '/' in file_path:
                directory = file_path.rsplit('/', 1)[0] + '/'
            else:
                directory = ""  # file is in the root
            directories.add(directory)

        # Replace empty string with "/" to explicitly denote the root directory.
        directories = {directory if directory else "/" for directory in directories}

        # Build the file mapper content mapping each unique directory to "" (empty).
        file_mapper_content = {
            "paths": [{"key": directory, "name": ""} for directory in sorted(directories)]
        }
        json_data = json.dumps(file_mapper_content)

        # Create a timestamp to add as a suffix (using UTC to ensure consistency).
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")

        # Determine the S3 key for fileMapper.json based on the zip_output_prefix location.
        if '/' in zip_output_prefix:
            zip_dir = zip_output_prefix.rsplit('/', 1)[0]
            file_mapper_key = f"{zip_dir}/fileMapper_{timestamp}.json"
        else:
            file_mapper_key = f"fileMapper_{timestamp}.json"

        self.s3_client.put_object(
            Bucket=bucket_name,
            Key=file_mapper_key,
            Body=json_data,
            ContentType='application/json'
        )
        # print(f"Uploaded fileMapper.json to s3://{bucket_name}/{file_mapper_key}")
        return file_mapper_key

    def delete_file_mapper(self, bucket_name, file_mapper_key):
        """
        Deletes the fileMapper.json from the S3 bucket to avoid conflicts.
        """
        self.s3_client.delete_object(Bucket=bucket_name, Key=file_mapper_key)
        # print(f"Deleted fileMapper.json from s3://{bucket_name}/{file_mapper_key}")

    # def zip_files(self, bucket_name, file_paths, zip_file_name):
    def zip_files(self, bucket_name, file_paths, zip_output_prefix):
        """
        Create a zip of specified `file_paths` in `bucket_name` using s3zipper's /v2/zipstart endpoint
        and poll /v2/zipstate until SUCCESS or FAILURE.
        """
        # Initialize the last progress print time
        self._last_progress_print = None

        # 1. Get temp credentials
        self.get_temp_sts_credentials()

        # 2. Create and upload fileMapper.json to S3.
        file_mapper_key = self.create_and_upload_file_mapper(bucket_name, file_paths, zip_output_prefix)

         # 3. Prepare to call /v2/zipstart.
        start_url = f"{S3ZIPPER_BASE_URL}/v2/zipstart"
        headers = {
            "Authorization": f"Bearer {self.s3zipper_token}",
            "Content-Type": "application/json"
        }
        payload = {
            "awsKey": self.aws_access_key_id,
            "awsSecret": self.aws_secret_access_key,
            "awsToken": self.aws_session_token,
            "awsBucket": bucket_name,
            "awsRegion": self.region,
            "filePaths": file_paths,
            "zipTo": zip_output_prefix,
            "usePresignUrl": "false",
            "fileMapper": f"{bucket_name}/{file_mapper_key}",
        }

        print("Starting zip process with payload:",
              "bucket:", bucket_name,
              "file_paths:", file_paths,
              "zip_output_prefix:", zip_output_prefix,
              "fileMapper:", f"{bucket_name}/{file_mapper_key}")
        try:
            resp = requests.post(start_url, json=payload, headers=headers, timeout=120)
            resp.raise_for_status()
        except requests.RequestException as e:
            # Ensure the fileMapper is removed if zipstart fails.
            self.delete_file_mapper(bucket_name, file_mapper_key)
            raise RuntimeError(f"zipstart request failed: {e}")

        # The response is something like:
        # {
        #   "message": "STARTED",
        #   "size": "29 B",
        #   "taskUUID": [
        #       { "streams3v2": "zipper_s3_65798267-3e04-43f3-b4bd-74c63585c119" }
        #   ]
        # }
        start_data = resp.json()
        task_uuid_list = start_data.get("taskUUID", [])
        if not task_uuid_list or len(task_uuid_list) == 0:
            print("zipstart response:", start_data)
            self.delete_file_mapper(bucket_name, file_mapper_key)
            raise RuntimeError("No taskUUID returned from zipstart response.")

        # We expect the first element to contain a key "streams3v2", whose value is our "zipper_s3_..." string
        first_item = task_uuid_list[0]
        if "streams3v2" not in first_item:
            self.delete_file_mapper(bucket_name, file_mapper_key)
            raise RuntimeError("Expected 'streams3v2' field not found in taskUUID array.")

        task_uuid = first_item["streams3v2"]

        try:
            # 4. Poll /v2/zipstate until SUCCESS or FAILURE.
            final_state = self._poll_zip_state(task_uuid, headers)
        finally:
            # 5. Remove fileMapper.json from S3 regardless of outcome.
            self.delete_file_mapper(bucket_name, file_mapper_key)

        return final_state

    def _poll_zip_state(self, task_uuid, headers, max_wait_seconds=MAX_WAIT_SECONDS, poll_interval=POLL_INTERVAL):
        """
        Polls the /v2/zipstate endpoint until it returns SUCCESS or FAILURE.
        Retries if it sees FAILURE, up to 3 total attempts.

        We now send a JSON body that looks like:
        {
            "message": "STARTED",
            "chainTaskUUID": [
                { "streams3": "<zipper_s3_...>" },
                { "email": "" }
            ]
        }

        The response is expected to look like (example):
        {
        "TaskUUID": "zipper_s3_65798267-3e04-43f3-b4bd-74c63585c119",
        "TaskName": "streams3",
        "State": "SUCCESS",
        "Results": [...],
        "Error": "",
        ...
        }

        :param task_uuid: The "zipper_s3_..." string from zipstart
        :param headers: Headers for the s3zipper request (including Bearer token)
        :param max_wait_seconds: Max total wait time (e.g., 600 for 10 minutes)
        :param poll_interval: Seconds to wait between checks
        :return: Final JSON response from zipstate if SUCCESS, or raises RuntimeError on repeated FAILURE/timeouts
        """
        zip_state_url = f"{S3ZIPPER_BASE_URL}/v2/zipstate"
        attempts = 0
        start_time = time.time()
        progress_time = (time.time() - start_time)

        while attempts < 3:  # up to 3 tries if we see FAILURE
            # Keep polling until max_wait_seconds has passed
            # while (time.time() - start_time) < max_wait_seconds:
            while progress_time < max_wait_seconds:
                try:
                    # Build the new payload with "message" and "chainTaskUUID"
                    payload = {
                        "chainTaskUUID": [
                            {"streams3": task_uuid},
                            {"email": ""}
                        ]
                    }
                    resp = requests.post(zip_state_url, json=payload, headers=headers, timeout=10)
                    resp.raise_for_status()
                except requests.RequestException as e:
                    raise RuntimeError(f"zipstate request failed: {e}")

                state_data = resp.json()
                state = state_data.get("State")  # e.g. "SUCCESS", "FAILURE", etc.
                # Print progress every minute instead of every poll interval
                current_time = time.time()
                # if not hasattr(self, '_last_progress_print') or current_time - self._last_progress_print >= poll_interval * 30:
                if self._last_progress_print is None or current_time - self._last_progress_print >= poll_interval * 30:
                    # Print the progress time in minutes
                    print("s3Zipper State:", state, "progress time:", progress_time/60, "minutes")
                    self._last_progress_print = current_time

                if state == "SUCCESS":
                    # Finished successfully
                    return state_data
                elif state == "FAILURE":
                    # We will retry if we haven’t hit attempts limit
                    # break
                    progress_time = max_wait_seconds
                    error_data = state_data.get("Error")
                    print("Zip process failed.")
                    print(error_data)
                    continue
                else:
                    # Possibly "CREATING" or other statuses; wait then poll again
                    time.sleep(poll_interval)

                progress_time = time.time() - start_time

            # If we reach here, it means either FAILURE was returned or we timed out
            attempts += 1
            if attempts < 3:
                print(f"Retrying zip process (attempt {attempts} of 3).")
            else:
                raise RuntimeError(f"Zip process failed or timed out after {attempts} attempts.")

        # If we got here, we never saw SUCCESS in the allotted time or attempts
        raise RuntimeError(f"Zip process did not reach SUCCESS in {max_wait_seconds} seconds.")


if __name__ == "__main__":
    from Config import Config
    from AWSSession import AWSSession

    config = Config()
    AWSSession.shared() # ensure AWSSession init

    session = boto3.Session(profile_name = config.s3_aws_profile)

    s3_client = session.client('s3')

    # Example usage:
    S3ZIPPER_KEY = "JjfXXXX"
    S3ZIPPER_SECRET = "fe79XXXXX"
    if config.s3_aws_region is None:
        raise ValueError("AWS region is not set in the configuration.")
    service = S3ZipperService(s3zipper_user_key=S3ZIPPER_KEY, s3zipper_user_secret=S3ZIPPER_SECRET, s3_client=s3_client, region=config.s3_aws_region)

    bucket_name = "etl-development-input"
    file_paths = ["etl-development-input/ENGESVN2DA/B01___01_Matthew_____ENGESVN2DA.mp3", "etl-development-input/ENGESVN2DA/B01___02_Matthew_____ENGESVN2DA.mp3", "B27___21_Revelation__ENGESVN2DA.mp3", "B27___22_Revelation__ENGESVN2DA.mp3"]
    zip_output_prefix = "s3zipper/python-test.zip"

    try:
        result = service.zip_files(bucket_name, file_paths, zip_output_prefix)
        # The final result will be a ZipStateResponse struct and you can access to the zip file URL in Results[0].Value, however, this is a signed URL and it will expire after a time as s3zipper documentation says that the URL will be always signed and will expire after a time (maximun 1 year). My proposal will be once the result is SUCCESS, we can use our CDN URL, the path of the zip file (zipOutputPrefix) and create a new signed URL without the expiration time.
        print("Final Zip State:", result)
    except Exception as e:
        print("Error:", e)
