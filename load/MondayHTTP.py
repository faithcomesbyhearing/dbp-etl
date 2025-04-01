import json
import logging
import re
import requests
import time
from typing import Optional

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class MondayHTTP:
    """
    Equivalent to the Go struct for parsing known Monday API response fields.
    """
    def __init__(self, error_message="", error_code="", status_code=0, account_id=0):
        self.error_message = error_message
        self.error_code = error_code
        self.status_code = status_code
        self.account_id = account_id

    def __repr__(self):
        return (f"MondayHTTP(error_message={self.error_message}, "
                f"error_code={self.error_code}, status_code={self.status_code}, "
                f"account_id={self.account_id})")


class HTTP:
    """
    Analogous to the Go 'HTTP' interface with one main method:
    MakeMondayAPICall(ctx, requestString []byte) ([]byte, error)
    In Python, we'll just define a base class or interface with a method signature.
    """

    def make_monday_api_call(self, request_string: bytes) -> bytes:
        """
        Should be overridden to implement the actual HTTP logic.
        """
        raise NotImplementedError("Subclasses must implement this method.")


class HTTPService(HTTP):
    """
    The Python equivalent of the Go 'HTTPService' struct:
      - Holds the Monday.com API key and URL
      - Sends requests with retries
      - Unescapes JSON strings
      - Checks responses for errors
    """

    def __init__(self, api_key: str, api_url: str):
        """
        :param api_key: Your Monday.com API token
        :param api_url: The Monday.com GraphQL endpoint, e.g. "https://api.monday.com/v2"
        """
        self.api_key = api_key
        self.api_url = api_url

    def make_monday_api_call(self, request_string: bytes) -> bytes:
        """
        Calls self._send_request(...) with minimal or no unescaping.
        """
        # Optionally see the “raw” request string for debugging:
        logger.debug("make_monday_api_call: raw requestString=%s", request_string.decode("utf-8"))

        # Possibly remove only literal \n or \t if you want:
        cleaned_str = self._unescape_json_string(request_string.decode("utf-8"))
        logger.debug("make_monday_api_call: cleaned requestString=%s", cleaned_str)

        max_retries = 5
        delay = 1.0

        for attempt in range(max_retries):
            try:
                response_body = self._send_request(cleaned_str.encode("utf-8"))
                if response_body is None:
                    raise ValueError("nil or empty response from server")

                self._check_response(response_body)
                return response_body

            except Exception as e:
                if self._is_retryable_error(e):
                    logger.debug("Retryable error (attempt %d/%d): %s. Sleeping %ds...",
                                attempt+1, max_retries, e, delay)
                    time.sleep(delay)
                    delay *= 2
                else:
                    raise

        raise RuntimeError(f"make_monday_api_call failed after {max_retries} attempts")


    def _send_request(self, request_body: bytes) -> Optional[bytes]:
        """
        Sends the actual HTTP POST request to Monday.com (like sendRequest in Go).
        Returns response body as bytes.
        """

        headers = {
            "Authorization": self.api_key,
            "Content-Type": "application/json",
            "API-Version": "2023-10"  # optional custom header
        }

        try:
            resp = requests.post(
                self.api_url,
                data=request_body,
                headers=headers,
                timeout=30
            )
            logger.debug("HTTP status code: %s", resp.status_code)
            logger.debug("HTTP response text: %s", resp.text)
        except requests.RequestException as e:
            logger.error("sendRequest: The HTTP request failed with error: %s", str(e))
            raise

        if not resp:
            logger.error("sendRequest: The HTTP response was nil/empty.")
            return None

        return resp.content  # return raw bytes (like Go code)

    def _check_response(self, response_body: bytes):
        """
        Parses the JSON and checks for known error fields.
        Equivalent to the Go code's checkResponse().
        """
        try:
            decoded = json.loads(response_body)
        except json.JSONDecodeError as e:
            logger.error("_check_response: Failed to unmarshal response: %s", str(e))
            raise

        # Check if the Monday API returned an error at the top level
        # In the Go code, we had MondayHTTP with fields error_message, error_code, status_code, ...
        # But Monday.com often returns "errors" in GraphQL responses.
        # We'll do a simple check for a standard error pattern:
        # e.g. {"error_message": "...", "error_code": "...", "status_code": 400}
        # or top-level "errors": [...]
        possible_error = decoded.get("error_message") or decoded.get("error_code")
        status_code = decoded.get("status_code", 0)
        if possible_error and status_code >= 300:
            logger.error("Monday API returned error_code=%s status_code=%s error_message=%s",
                         decoded.get("error_code"), status_code, decoded.get("error_message"))
            raise RuntimeError(decoded.get("error_message", "Unknown Monday API error"))

        # Also, Monday GraphQL can return "errors": [...]
        if "errors" in decoded:
            errors = decoded["errors"]
            if len(errors) > 0:
                err_msg = errors[0].get("message", "Unknown GraphQL error")
                logger.error("_check_response: Monday GraphQL error: %s", err_msg)
                raise RuntimeError(err_msg)

    def _unescape_json_string(self, s: str) -> str:
        """
        Minimal approach:
        - Do NOT remove backslashes or escaped quotes.
        - Optionally remove literal \n and \t if you want to clean up log output.

        If you want to keep some cleanup:
        """
        # If you must remove literal "\n" or "\t" from the raw string:
        s = s.replace('\\n', '').replace('\\t', '')
        return s


    def _is_retryable_error(self, err: Exception) -> bool:
        """
        Equivalent to isRetryableError in Go. Check if the error is a timeout or transient network issue.
        """
        err_str = str(err).lower()
        if "timeout" in err_str or "timed out" in err_str:
            return True
        if "temporary network" in err_str:
            return True
        if "connection reset" in err_str or "connection aborted" in err_str:
            return True
        return False
