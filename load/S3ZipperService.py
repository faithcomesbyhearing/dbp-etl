import time
import json
import requests
from datetime import datetime, timezone
from RunStatus import RunStatus
from AWSSession import AWSSession

# Global variable for the s3zipper base URL
S3ZIPPER_BASE_URL = "https://api.s3zipper.com"
MAX_WAIT_SECONDS = 10800  # 3 hours
POLL_INTERVAL = 2

class S3ZipperService:
    """
    A class to interact with the s3zipper API for zipping files in an S3 bucket.
    This class handles obtaining a Bearer token from s3zipper, fetching temporary AWS STS credentials,
    and creating a zip file of specified files in an S3 bucket. It polls the s3zipper API until the zip
    process is complete (either SUCCESS or FAILURE).
    """
    # S3Zipper API State Constants
    STATE_PENDING = "PENDING"
    STATE_RECEIVED = "RECEIVED"
    STATE_STARTED = "STARTED"
    STATE_RETRY = "RETRY"
    STATE_SUCCESS = "SUCCESS"
    STATE_FAILURE = "FAILURE"

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
        Retrieves AWS temporary security credentials from the AWSSession's session object.

        This method gets credentials from the same session used by the s3_client,
        ensuring consistent credentials across all AWS operations.

        Returns:
            None

        Side effects:
            Sets the following instance attributes:
            - self.aws_access_key_id
            - self.aws_secret_access_key
            - self.aws_session_token
        """
        # Get credentials from the AWSSession's session
        session = AWSSession.shared().session
        credentials = session.get_credentials()

        # The credentials object will automatically refresh if they're temporary and expired
        self.aws_access_key_id = credentials.access_key
        self.aws_secret_access_key = credentials.secret_key
        self.aws_session_token = credentials.token

    def create_and_upload_file_mapper(self, bucket_name, file_paths, zip_output_prefix):
        """
        Extracts the directory names from all file_paths, creates a custom file mapper JSON that maps each directory
        to the root directory "/", uploads it to the S3 bucket in the same directory as zip_output_prefix,
        and returns the S3 key.

        A timestamp suffix is added to the fileMapper.json name to avoid overwrites when executed in parallel.

        Example output JSON:
        {
            "paths": [
                { "key": "dbp-staging/audio/ENGESV/ENGESVN2DA/", "name": "/" },
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
            ContentType='application/json',
            ACL='bucket-owner-full-control',
        )
        # print(f"Uploaded fileMapper.json to s3://{bucket_name}/{file_mapper_key}")
        return file_mapper_key

    def _retryable_post(self, url, payload, headers, timeout=240, retries=2):
        for attempt in range(retries):
            try:
                resp = requests.post(url, json=payload, headers=headers, timeout=timeout)
                resp.raise_for_status()
                return resp
            except requests.Timeout:
                print(f"Request timed out (attempt {attempt + 1} of {retries}). Retrying...")
                if attempt == retries - 1:
                    raise RuntimeError("Request failed after multiple timeouts.")
                time.sleep(10)  # Wait 10 seconds before retrying
            except requests.RequestException as e:
                raise RuntimeError(f"Request failed: {e}")

    def delete_file_mapper(self, bucket_name, file_mapper_key):
        """
        Deletes the fileMapper.json from the S3 bucket to avoid conflicts.
        """
        self.s3_client.delete_object(Bucket=bucket_name, Key=file_mapper_key)
        # print(f"Deleted fileMapper.json from s3://{bucket_name}/{file_mapper_key}")

    def zip_files(self, bucket_name, file_paths, zip_output_prefix):
        """
        Create a zip of specified `file_paths` in `bucket_name` using s3zipper's /v2/zipstart endpoint
        and poll /v2/zipstate until SUCCESS or FAILURE.

        IMPORTANT: AWS STS credentials passed to S3Zipper must remain valid for the entire operation.
        If credentials expire during processing, S3Zipper will fail with ExpiredToken error.
        Credentials must be valid for at least MAX_WAIT_SECONDS.

        :param bucket_name: S3 bucket name
        :param file_paths: List of S3 object keys to zip
        :param zip_output_prefix: S3 key where the zip file will be saved
        :return: Final state response from S3Zipper
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
            "Content-Type": "application/json",
            "x-amz-acl": "bucket-owner-full-control",
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

        RunStatus.statusMap[RunStatus.ZIP_PROCESSING] = RunStatus.NOT_DONE
        print("Starting zip process with payload:",
              "bucket:", bucket_name,
              "file_paths:", file_paths,
              "zip_output_prefix:", zip_output_prefix,
              "fileMapper:", f"{bucket_name}/{file_mapper_key}")
        try:
            resp = self._retryable_post(start_url, payload, headers)
        except requests.RequestException as e:
            # Ensure the fileMapper is removed if zipstart fails.
            self.delete_file_mapper(bucket_name, file_mapper_key)
            RunStatus.set(RunStatus.ZIP_PROCESSING, False)
            raise RuntimeError(f"zipstart request failed: {e}")

        # The response is something like:
        # {
        #   "message": "STARTED",
        #   "size": "29 B",
        #   "taskUUID": [
        #       { "streams3v2": "zipper_s3_65798267-3e04-43f3-b4bd-74c63585c119" }
        #   ]
        # }
        start_data = resp.json() if resp is not None else {}
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

        if final_state.get("State") == self.STATE_SUCCESS:
            RunStatus.set(RunStatus.ZIP_PROCESSING, True)
        else:
            RunStatus.set(RunStatus.ZIP_PROCESSING, False)

        return final_state

    def _handle_failure_attempt(self, failure_count, max_failures, poll_interval, error_message=None):
        """
        Handles a failure attempt (timeout or FAILURE state) by incrementing the counter
        and optionally preparing for a retry.

        :param failure_count: Current failure count
        :param max_failures: Maximum allowed failures
        :param poll_interval: Seconds to wait before retrying
        :param error_message: Optional error message (if None, indicates a timeout)
        :return: Updated failure_count
        """
        failure_count += 1

        if error_message:
            print(f"Zip process failed (attempt {failure_count} of {max_failures}): {error_message}")
        else:
            print(f"Request timed out while polling zipstate (attempt {failure_count} of {max_failures})")

        if failure_count < max_failures:
            print(f"Retrying zip process (attempt {failure_count + 1} of {max_failures})...")
            time.sleep(poll_interval)

        return failure_count

    def _poll_zip_state(self, task_uuid, headers, max_wait_seconds=MAX_WAIT_SECONDS, poll_interval=POLL_INTERVAL):
        """
        Polls the /v2/zipstate endpoint until it returns SUCCESS or FAILURE.

        Retry behavior (max 3 total attempts):
        - FAILURE state: Counts as 1 failure attempt, retries if attempts < max_failures
        - TIMEOUT errors: Counts as 1 failure attempt, retries if attempts < max_failures
        - Intermediate states (PENDING, RECEIVED, STARTED, RETRY): Continue polling without counting as failures
        - Overall timeout: Respects max_wait_seconds limit for the entire operation

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

        Possible states:
        - "PENDING" - initial state of a task
        - "RECEIVED" - task has been received
        - "STARTED" - task has started processing
        - "RETRY" - failed task has been scheduled for retry
        - "SUCCESS" - task has been processed successfully
        - "FAILURE" - task processing has failed

        :param task_uuid: The "zipper_s3_..." string from zipstart
        :param headers: Headers for the s3zipper request (including Bearer token)
        :param max_wait_seconds: Max total wait time for the entire operation (e.g., 3600 for 1 hour)
        :param poll_interval: Seconds to wait between checks
        :return: Final JSON response from zipstate if SUCCESS
        :raises RuntimeError: On repeated FAILURE states (3 attempts), timeout exceeding max_wait_seconds,
                              or other request failures
        """
        zip_state_url = f"{S3ZIPPER_BASE_URL}/v2/zipstate"
        failure_count = 0
        max_failures = 3
        start_time = time.time()

        # Loop continues while we haven't exceeded failure limit and overall timeout
        while failure_count < max_failures:
            elapsed_time = time.time() - start_time

            try:
                # Build the payload
                payload = {
                    "chainTaskUUID": [
                        {"streams3": task_uuid},
                        {"email": ""}
                    ]
                }
                resp = self._retryable_post(zip_state_url, payload, headers)
            except requests.Timeout:
                # Timeout counts as a failure attempt
                failure_count = self._handle_failure_attempt(failure_count, max_failures, poll_interval)
                # Check timeout after handling failure attempt
                elapsed_time = time.time() - start_time
                if elapsed_time > max_wait_seconds:
                    raise RuntimeError(
                        f"Zip process did not reach SUCCESS in {max_wait_seconds} seconds. "
                        f"Elapsed: {elapsed_time:.0f}s. Failure count: {failure_count}"
                    )
                continue
            except requests.RequestException as e:
                raise RuntimeError(f"zipstate request failed: {e}")

            state_data = resp.json() if resp is not None else {}
            state = state_data.get("State")

            # Print progress every ~1 minute
            current_time = time.time()
            if self._last_progress_print is None or current_time - self._last_progress_print >= poll_interval * 30:
                # Recalculate elapsed_time for accurate progress reporting
                elapsed_time = time.time() - start_time
                print(
                    f"s3Zipper State: {state} | "
                    f"Elapsed: {elapsed_time/60:.1f} minutes | "
                    f"Max wait: {max_wait_seconds/60:.1f} minutes"
                )
                self._last_progress_print = current_time

            if state == self.STATE_SUCCESS:
                # Finished successfully
                print("Zip process completed successfully!")
                return state_data
            elif state == self.STATE_FAILURE:
                # Increment failure count and optionally retry
                error_data = state_data.get("Error", "Unknown error")
                failure_count = self._handle_failure_attempt(failure_count, max_failures, poll_interval, error_data)
            elif state in [self.STATE_PENDING, self.STATE_RECEIVED, self.STATE_STARTED, self.STATE_RETRY]:
                # These are intermediate states - continue polling
                time.sleep(poll_interval)
            else:
                # Unknown state - log and continue polling
                print(f"Unknown zip state: {state}. Continuing to poll...")
                time.sleep(poll_interval)

            # Check overall timeout after handling all state cases
            # This ensures we always attempt to get the state before failing on timeout
            elapsed_time = time.time() - start_time
            if elapsed_time > max_wait_seconds:
                raise RuntimeError(
                    f"Zip process did not reach SUCCESS in {max_wait_seconds} seconds. "
                    f"Elapsed: {elapsed_time:.0f}s. Failure count: {failure_count}"
                )

        # If we exit the loop, we've reached max_failures without success
        raise RuntimeError(
            f"Zip process failed after {failure_count} attempts. "
            f"Check logs for detailed error information."
        )

if __name__ == "__main__":
    import sys
    from Config import Config
    from AWSSession import AWSSession
    from RunStatus import RunStatus

    # Usage: python3 load/S3ZipperService.py <config_profile> <bucket_name> <file_paths_json> [zip_output_prefix]
    # Example: python3 load/S3ZipperService.py test etl-development-input '["path/to/file1.mp3", "path/to/file2.mp3"]' s3zipper/output.zip

    if len(sys.argv) < 4:
        print("Usage: python3 load/S3ZipperService.py <config_profile> <bucket_name> <file_paths_json> [zip_output_prefix]")
        print("Example: python3 load/S3ZipperService.py test etl-development-input '[\"audio/file.mp3\"]' s3zipper/test.zip")
        sys.exit(1)

    config_profile = sys.argv[1]
    bucket_name = sys.argv[2]
    file_paths_json = sys.argv[3]
    zip_output_prefix = sys.argv[4] if len(sys.argv) > 4 else "s3zipper/python-test.zip"

    # Parse file_paths from JSON
    try:
        file_paths = json.loads(file_paths_json)
        if not isinstance(file_paths, list):
            raise ValueError("file_paths must be a JSON array")
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON for file_paths: {e}")
        sys.exit(1)

    print(f"Config Profile: {config_profile}")
    print(f"Bucket Name: {bucket_name}")
    print(f"File Paths: {file_paths}")
    print(f"Zip Output Prefix: {zip_output_prefix}")

    # Initialize config with profile
    config = Config.shared()
    s3_client = AWSSession.shared().s3Client

    s3zipper_user_key = config.s3_zipper_user_key
    s3zipper_user_secret = config.s3_zipper_user_secret

    if config.s3_aws_region is None:
        raise ValueError("AWS region is not set in the configuration.")

    service = S3ZipperService(
        s3zipper_user_key=s3zipper_user_key,
        s3zipper_user_secret=s3zipper_user_secret,
        s3_client=s3_client,
        region=config.s3_aws_region,
    )

    try:
        result = service.zip_files(bucket_name, file_paths, zip_output_prefix)
        print("=" * 80)
        print("SUCCESS: Zip file created successfully")
        print("Final Zip State:", result)
        print("=" * 80)
    except Exception as e:
        print("=" * 80)
        print("ERROR: Zip process failed")
        print("Error:", e)
        print("=" * 80)
        sys.exit(1)

    RunStatus.exit()

# time python3 load/S3ZipperService.py test etl-development-input '["etl-development-input/audio/ENGESV/ENGESVN2DA/B01___01_Matthew_____ENGESVN2DA.mp3"]'
# time python3 load/S3ZipperService.py test dbp-staging '["dbp-staging/audio/ENGESV/ENGESVN2DA/B01___01_Matthew_____ENGESVN2DA.mp3"]'
# time python3 load/S3ZipperService.py test dbp-vid-staging '["dbp-vid-staging/video/ENGESV/ENGESVP2DV/English_ESV_MRK_9-1-13.mp4"]'

# time python3 load/S3ZipperService.py test dbp-vid-staging '["dbp-vid-staging/video/OBOCOV/OBOCOVS2DV/COVENANT_SEGMENT 01_Manobo-Obo_web.mp4", "dbp-vid-staging/video/OBOCOV/OBOCOVS2DV/COVENANT_SEGMENT 02_Manobo-Obo_web.mp4", "dbp-vid-staging/video/OBOCOV/OBOCOVS2DV/COVENANT_SEGMENT 03_Manobo-Obo_web.mp4", "dbp-vid-staging/video/OBOCOV/OBOCOVS2DV/COVENANT_SEGMENT 04_Manobo-Obo_web.mp4", "dbp-vid-staging/video/OBOCOV/OBOCOVS2DV/COVENANT_SEGMENT 05_Manobo-Obo_web.mp4", "dbp-vid-staging/video/OBOCOV/OBOCOVS2DV/COVENANT_SEGMENT 06_Manobo-Obo_web.mp4", "dbp-vid-staging/video/OBOCOV/OBOCOVS2DV/COVENANT_SEGMENT 07_Manobo-Obo_web.mp4", "dbp-vid-staging/video/OBOCOV/OBOCOVS2DV/COVENANT_SEGMENT 08_Manobo-Obo_web.mp4", "dbp-vid-staging/video/OBOCOV/OBOCOVS2DV/COVENANT_SEGMENT 09_Manobo-Obo_web.mp4", "dbp-vid-staging/video/OBOCOV/OBOCOVS2DV/COVENANT_SEGMENT 10_Manobo-Obo_web.mp4", "dbp-vid-staging/video/OBOCOV/OBOCOVS2DV/COVENANT_SEGMENT 11_Manobo-Obo_web.mp4", "dbp-vid-staging/video/OBOCOV/OBOCOVS2DV/COVENANT_SEGMENT 12_Manobo-Obo_web.mp4", "dbp-vid-staging/video/OBOCOV/OBOCOVS2DV/copyright_COV.pdf"]' s3zipper/python-test-complete-video.zip
