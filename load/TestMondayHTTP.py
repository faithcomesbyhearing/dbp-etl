import unittest
from unittest import mock
import json
import requests

from MondayHTTP import HTTPService

# DummyResponse simulates requests.Response for testing
class DummyResponse:
    def __init__(self, status_code, text="", headers=None):
        self.status_code = status_code
        self.text = text
        self.headers = headers or {}

    @property
    def ok(self):
        return 200 <= self.status_code < 300

    @property
    def content(self):
        return self.text.encode("utf-8")

class HTTPServiceTest(unittest.TestCase):
    def setUp(self):
        # Create service instance
        self.svc = HTTPService("test-key", "http://example.com")
        # Patch time.sleep to no-op and random.random for deterministic jitter
        patch_sleep = mock.patch('time.sleep', lambda s: None)
        patch_jitter = mock.patch('random.random', lambda: 0.5)
        self.addCleanup(patch_sleep.stop)
        self.addCleanup(patch_jitter.stop)
        patch_sleep.start()
        patch_jitter.start()

    def make_seq_responses(self, responses):
        """
        Returns a side_effect function that either raises or returns items from responses list.
        """
        seq = list(responses)
        def side_effect(url, data, headers, timeout):
            item = seq.pop(0)
            if isinstance(item, BaseException):
                raise item
            return item
        return side_effect

    def test_429_then_success(self):
        # 429 with Retry-After=0 then success
        responses = [
            DummyResponse(429, text='{"errors":[{"message":"Rate limit"}]}', headers={"Retry-After": "0"}),
            DummyResponse(200, text='{"data":{"ok":true}}'),
        ]
        with mock.patch('requests.post', side_effect=self.make_seq_responses(responses)):
            out = self.svc.make_monday_api_call(b'{}')
            self.assertEqual(json.loads(out), {"data": {"ok": True}})

    def test_server_error_retries_then_success(self):
        for status in (500, 502, 503, 504):
            with self.subTest(status=status):
                responses = [
                    DummyResponse(status, text="Server error"),
                    DummyResponse(200, text='{"data":{"ok":true}}'),
                ]
                with mock.patch('requests.post', side_effect=self.make_seq_responses(responses)):
                    out = self.svc.make_monday_api_call(b'{}')
                    self.assertIs(json.loads(out)["data"]["ok"], True)

    def test_network_error_then_success(self):
        responses = [
            requests.RequestException("conn reset"),
            DummyResponse(200, text='{"data":{"ok":true}}'),
        ]
        with mock.patch('requests.post', side_effect=self.make_seq_responses(responses)):
            out = self.svc.make_monday_api_call(b'{}')
            self.assertIs(json.loads(out)["data"]["ok"], True)

    def test_retry_exhaustion_raises(self):
        # Always return 429 to exhaust retries
        response = DummyResponse(429, text="Still bad", headers={"Retry-After": "0"})
        with mock.patch('requests.post', return_value=response):
            with self.assertRaises(RuntimeError) as cm:
                self.svc.make_monday_api_call(b'{}')
            self.assertIn("failed after", str(cm.exception))

    def test_non_retryable_status_immediate_failure(self):
        for status in (400, 401, 403, 404):
            with self.subTest(status=status):
                response = DummyResponse(status, text="Client error")
                with mock.patch('requests.post', return_value=response):
                    with self.assertRaises(ValueError) as cm:
                        self.svc.make_monday_api_call(b'{}')
                    self.assertIn("Empty response", str(cm.exception))

    def test_429_retry_after_nonzero(self):
        """
        429 Too Many Requests with Retry-After > 0 then success on retry.
        """
        responses = [
            DummyResponse(429, text='{"errors":[{"message":"Rate limit exceeded"}]}', headers={"Retry-After": "4"}),
            DummyResponse(200, text='{"data":{"ok":true}}'),
        ]
        with mock.patch('requests.post', side_effect=self.make_seq_responses(responses)):
            out = self.svc.make_monday_api_call(b'{}')
            self.assertEqual(json.loads(out), {"data": {"ok": True}})

if __name__ == '__main__':
    unittest.main()
