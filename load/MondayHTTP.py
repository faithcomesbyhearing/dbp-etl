import json
import logging
import requests
import time
import random
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


class HTTPRetryableError(Exception):
    """Raised for HTTP status codes that should be retried."""
    def __init__(self, status_code: int, retry_after: Optional[float] = None, message: Optional[str] = None):
        super().__init__(message or f"HTTP {status_code}")
        self.status_code = status_code
        self.retry_after = retry_after

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
        cleaned = self._unescape_json_string(request_string.decode("utf-8"))
        logger.debug("make_monday_api_call: cleaned requestString=%s", cleaned)

        max_retries = 5
        base_delay = 1.0

        for attempt in range(1, max_retries + 1):
            try:
                body = self._send_request(cleaned.encode("utf-8"))
                if body is None:
                    # This is a non-retryable error e.g 401 Unauthorized, 403 Forbidden
                    raise ValueError("Empty response from server")

                self._check_response(body)
                return body

            except HTTPRetryableError as e:
                # Determine how long to sleep
                delay = e.retry_after if e.retry_after is not None else base_delay * (2 ** (attempt - 1))
                # Add jitter: ±50%
                jitter = delay * (0.5 + random.random())
                logger.warning(
                    "Retryable HTTP error (status=%s) on attempt %d/%d: %s. "
                    "Sleeping %.1f seconds before retry…",
                    getattr(e, 'status_code', 'N/A'),
                    attempt, max_retries, e,
                    jitter
                )
                time.sleep(jitter)

            except Exception as e:
                # Non-retryable
                logger.error("Non-retryable error on attempt %d/%d: %s", attempt, max_retries, e)
                raise

        msg = f"make_monday_api_call failed after {max_retries} attempts"
        logger.error(msg)
        raise RuntimeError(msg)


    def _send_request(self, request_body: bytes) -> Optional[bytes]:
        """
        Sends the actual HTTP POST request to Monday.com (like sendRequest in Go).
        Returns response body as bytes.
        """

        headers = {
            "Authorization": self.api_key,
            "Content-Type": "application/json",
            "API-Version": "2023-10"
        }

        try:
            resp = requests.post(self.api_url, data=request_body, headers=headers, timeout=30)
            logger.debug("HTTP status code: %s", resp.status_code)
            logger.debug("HTTP response text: %s", resp.text)
        except requests.RequestException as e:
            logger.error("sendRequest: HTTP request failed: %s", e)
            # network-level errors are retryable
            raise HTTPRetryableError(0, message=str(e))

        # Handle specific status codes
        if resp.status_code == 401:
            logger.error("sendRequest: Unauthorized (401). Invalid API key? Response: %s", resp.text)
            return None
        
        if resp.status_code == 403:
            logger.error("sendRequest: Forbidden (403). Check your permissions. Response: %s", resp.text)
            return None

        if resp.status_code == 404:
            logger.error("sendRequest: Not Found (404). Check the URL or endpoint.")
            return None

        if resp.status_code == 400:
            logger.error("sendRequest: Bad Request (400). Check the request format.")
            return None

        if resp.status_code == 429:
            retry_after = None
            if 'Retry-After' in resp.headers:
                try:
                    retry_after = float(resp.headers['Retry-After'])
                except ValueError:
                    pass
            logger.warning("sendRequest: Rate limited (429). Retry-After=%s sec. Response: %s",
                           retry_after, resp.text)
            raise HTTPRetryableError(429, retry_after)

        if resp.status_code in (500, 502, 503, 504):
            retry_after = None
            if 'Retry-After' in resp.headers:
                try:
                    retry_after = float(resp.headers['Retry-After'])
                except ValueError:
                    pass
            logger.warning("sendRequest: Server error %s. Retry-After=%s sec. Response: %s",
                           resp.status_code, retry_after, resp.text)
            raise HTTPRetryableError(resp.status_code, retry_after)

        if not resp.ok:
            # 4xx other than 401/429: client error, not retryable
            logger.error("sendRequest: Unexpected status %s. Response: %s",
                         resp.status_code, resp.text)
            return None

        return resp.content

    def _check_response(self, response_body: bytes):
        """
        Parses the JSON and checks for known error fields.
        Equivalent to the Go code's checkResponse().
        """
        try:
            decoded = json.loads(response_body)
        except json.JSONDecodeError as e:
            logger.error("_check_response: JSON decode failed: %s", e)
            raise

        # GraphQL “errors” block
        if "errors" in decoded and decoded["errors"]:
            msg = decoded["errors"][0].get("message", "Unknown GraphQL error")
            logger.error("_check_response: GraphQL error: %s", msg)
            raise RuntimeError(msg)

        # monday.com legacy error fields
        err_msg = decoded.get("error_message")
        err_code = decoded.get("error_code")
        status  = decoded.get("status_code", 0)
        if (err_msg or err_code) and status >= 300:
            logger.error("Monday API error_code=%s status_code=%s message=%s",
                         err_code, status, err_msg)
            raise RuntimeError(err_msg or "Monday API error")

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
