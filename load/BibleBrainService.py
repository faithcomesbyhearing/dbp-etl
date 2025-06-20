import requests
import logging
import time
from typing import List, Optional
from botocore.exceptions import BotoCoreError, ClientError

class Package:
    """
    Represents a package containing products. Provides an ID composed of all product codes.
    """
    def __init__(self, products: List[str]):
        self.products = products

    def id(self) -> str:
        """
        Builds a string by concatenating sanitized product codes with dashes.
        Replaces '/' with '_' in each product code.
        """
        # Assuming products are strings, directly replace '/' with '_'
        sanitized_codes = [prod.replace('/', '_') for prod in self.products]
        return '-'.join(sanitized_codes)

class BibleBrainService:
    """
    Wrapper service for interacting with the BibleBrain API to fetch PDFs
    and upload them to S3, with retry logic for transient errors.
    This service handles fetching PDFs from the BibleBrain API and uploading them
    to an S3 bucket, with built-in retry logic for network issues and HTTP errors.
    It uses exponential backoff for retries and logs all operations.
    """
    RETRY_COUNT = 3
    BACKOFF_FACTOR = 2  # exponential backoff multiplier (seconds)

    def __init__(
        self,
        s3_client,
        base_url: str = "http://dev.biblebrain-service.com",
        timeout: int = 30
    ):
        """
        :param s3_client: A boto3 S3 client instance
        :param base_url: Base URL for the BibleBrain API
        :param timeout: HTTP request timeout in seconds
        """
        self.s3_client = s3_client
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.logger = logging.getLogger(__name__)

    def fetch_pdf(self, product_codes: List[str], mode: str, format: str) -> bytes:
        """
        Fetches a PDF via HTTP GET from the /api/copyright endpoint given a list of product codes,
        retrying up to RETRY_COUNT on timeouts or HTTP 5xx errors.

        :param product_codes: List of product code strings
        :return: PDF content as bytes
        :raises RuntimeError: If the request fails or the response is not PDF
        """
        url = f"{self.base_url}/api/copyright"
        # Pass codes as repeated query params: ?productCodes=code1&productCodes=code2
        params = [("productCode", code) for code in product_codes] + [("mode", mode), ("format", format)]
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/pdf"
        }

        for attempt in range(self.RETRY_COUNT):
            try:
                self.logger.debug(
                    "Attempt %d: GET %s with params %s",
                    attempt + 1, url, params
                )
                response = requests.get(url, params=params, headers=headers, timeout=self.timeout)
            except requests.Timeout as e:
                self.logger.warning("Timeout on attempt %d: %s", attempt + 1, e)
            except requests.RequestException as e:
                self.logger.error("Error fetching PDF on attempt %d: %s", attempt + 1, e)
                raise RuntimeError(f"Failed to fetch PDF: {e}")
            else:
                status = response.status_code
                if status >= 500:
                    self.logger.warning("Server error %d on attempt %d", status, attempt + 1)
                else:
                    try:
                        response.raise_for_status()
                    except requests.RequestException as e:
                        self.logger.error("HTTP error %d: %s", status, e)
                        raise RuntimeError(f"Failed to fetch PDF: {e}")

                    content_type = response.headers.get("Content-Type", "")
                    if not content_type.lower().startswith("application/pdf"):
                        self.logger.error("Unexpected content type: %s", content_type)
                        raise RuntimeError(f"Expected PDF response, got: {content_type}")

                    return response.content

            # Exponential backoff before retrying
            if attempt < self.RETRY_COUNT - 1:
                backoff = self.BACKOFF_FACTOR ** attempt
                self.logger.debug("Sleeping for %d seconds before retry", backoff)
                time.sleep(backoff)

        raise RuntimeError(f"Failed to fetch PDF after {self.RETRY_COUNT} attempts")

    def upload_pdf(
        self,
        bucket_name: str,
        key: str,
        pdf_bytes: bytes,
        extra_args: Optional[dict] = None
    ) -> None:
        """
        Uploads PDF bytes to the specified S3 bucket/key.

        :param bucket_name: Name of the S3 bucket
        :param key: S3 object key (including .pdf extension)
        :param pdf_bytes: PDF content as bytes
        :param extra_args: Additional arguments for put_object (e.g., ACL)
        :raises RuntimeError: If the upload fails
        """
        args = {
            'Bucket': bucket_name,
            'Key': key,
            'Body': pdf_bytes,
            'ContentType': 'application/pdf'
        }
        if extra_args:
            args.update(extra_args)

        self.logger.debug("Uploading PDF to s3://%s/%s", bucket_name, key)
        try:
            self.s3_client.put_object(**args)
        except (BotoCoreError, ClientError) as e:
            self.logger.error("Error uploading PDF to S3: %s", e)
            raise RuntimeError(f"Failed to upload PDF to S3: {e}")

    def fetch_and_upload(
        self,
        product_codes: List[str],
        mode: str,
        format: str,
        bucket_name: str,
        key: str,
        extra_args: Optional[dict] = None
    ) -> None:
        """
        Convenience method: fetches the PDF for the given product codes and uploads it to S3.
        """
        pdf_data = self.fetch_pdf(product_codes, mode=mode, format=format)
        self.upload_pdf(bucket_name, key, pdf_data, extra_args)

if __name__ == "__main__":
    from Config import Config
    from AWSSession import AWSSession
    from RunStatus import RunStatus

    config = Config()
    s3_client = AWSSession.shared().s3Client
    base_url = (
        config.biblebrain_services_base_url
        if config.biblebrain_services_base_url
        else "http://dev.biblebrain-service.com"
    )
    biblebrain_service = BibleBrainService(s3_client=s3_client, base_url=base_url)

    product_codes = ["N2ESV", "N2KJV"]
    pkg = Package(products=product_codes)
    package_id = pkg.id()
    bucket_name = config.s3_bucket or "etl-development-input"
    key = f"pdf/{package_id}.pdf"

    try:
        biblebrain_service.fetch_and_upload(
            product_codes=product_codes,
            mode="audio",
            format="pdf",
            bucket_name=bucket_name,
            key=key,
            extra_args={'ACL': 'bucket-owner-full-control'}
        )
        print(f"Successfully uploaded {key} to s3://{bucket_name}/")
    except RuntimeError as e:
        print(f"Failed to fetch and upload PDF: {e}")


# time python3 load/BibleBrainService.py test
