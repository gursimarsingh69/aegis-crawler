"""
fetcher.py
==========
Async media downloader for the Digital Asset Protection Pipeline.

Consumes MediaItem objects from the fetch_queue, downloads the raw bytes,
validates the content type, and places the enriched item into the
preprocess_queue.

Features:
  - Async downloads via aiohttp
  - Retry logic with exponential back-off (via utils.async_retry)
  - Timeout handling
  - Content-type validation
  - Streaming large files to avoid memory spikes
"""

import asyncio
from typing import Optional

import aiohttp

from .config import PipelineConfig
from .utils import (
    MediaItem,
    async_retry,
    get_logger,
    is_supported_content_type,
    mime_to_extension,
)

logger = get_logger("fetcher")

# Maximum bytes we will accept for a single media file (50 MB)
MAX_CONTENT_BYTES = 50 * 1024 * 1024

# Connection pool settings shared across all fetcher workers
_CONNECTOR: Optional[aiohttp.TCPConnector] = None


def _get_connector() -> aiohttp.TCPConnector:
    """Lazily create a shared TCPConnector with a generous pool."""
    global _CONNECTOR
    if _CONNECTOR is None or _CONNECTOR.closed:
        _CONNECTOR = aiohttp.TCPConnector(
            limit=PipelineConfig.NUM_FETCHER_WORKERS * 4,
            ttl_dns_cache=300,
            ssl=False,  # set True in production if needed
        )
    return _CONNECTOR


_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


# ─────────────────────────────────────────────────────────────────────────────
# Download helper
# ─────────────────────────────────────────────────────────────────────────────

async def _download_bytes(
    url: str,
    session: aiohttp.ClientSession,
    timeout_seconds: int,
) -> tuple[bytes, str]:
    """
    Download a URL and return (raw_bytes, content_type).
    Raises aiohttp.ClientError or asyncio.TimeoutError on failure.
    Raises ValueError if content-type is unsupported or file is too large.
    """
    timeout = aiohttp.ClientTimeout(total=timeout_seconds)
    async with session.get(url, timeout=timeout, allow_redirects=True) as resp:
        resp.raise_for_status()

        content_type: str = resp.headers.get("Content-Type", "")
        if not is_supported_content_type(content_type):
            raise ValueError(
                f"Unsupported content-type '{content_type}' for URL: {url}"
            )

        # Check Content-Length header to skip huge files early
        cl = resp.headers.get("Content-Length")
        if cl and int(cl) > MAX_CONTENT_BYTES:
            raise ValueError(
                f"File too large ({cl} bytes) for URL: {url}"
            )

        # Stream into a buffer
        chunks: list[bytes] = []
        total = 0
        async for chunk in resp.content.iter_chunked(1024 * 64):  # 64 KB chunks
            total += len(chunk)
            if total > MAX_CONTENT_BYTES:
                raise ValueError(
                    f"File exceeded size limit ({MAX_CONTENT_BYTES} bytes) mid-stream: {url}"
                )
            chunks.append(chunk)

        return b"".join(chunks), content_type


# ─────────────────────────────────────────────────────────────────────────────
# Fetcher worker
# ─────────────────────────────────────────────────────────────────────────────

class Fetcher:
    """
    Single fetcher worker.
    Reads from fetch_queue, downloads media, writes to preprocess_queue.
    """

    def __init__(
        self,
        worker_id: int,
        fetch_queue: asyncio.Queue,
        preprocess_queue: asyncio.Queue,
    ) -> None:
        self.worker_id = worker_id
        self.fetch_queue = fetch_queue
        self.preprocess_queue = preprocess_queue
        self._log = get_logger(f"fetcher.worker-{worker_id}")
        self._session: Optional[aiohttp.ClientSession] = None

    def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                connector=_get_connector(),
                connector_owner=False,  # shared connector — don't close it
                headers={"User-Agent": _USER_AGENT},
            )
        return self._session

    @async_retry(
        max_attempts=PipelineConfig.MAX_RETRIES,
        delay_seconds=2.0,
        backoff_factor=2.0,
        exceptions=(aiohttp.ClientError, asyncio.TimeoutError, OSError),
    )
    async def _fetch_with_retry(self, url: str) -> tuple[bytes, str]:
        """Fetch with automatic retry (decorated)."""
        return await _download_bytes(
            url,
            self._get_session(),
            PipelineConfig.REQUEST_TIMEOUT_SECONDS,
        )

    async def process_item(self, item: MediaItem) -> None:
        """Download the media URL in `item` and push to preprocess_queue."""
        self._log.info(
            "[%s] Fetching %s %s",
            item.source, item.media_type, item.media_url,
        )
        try:
            raw_bytes, content_type = await self._fetch_with_retry(item.media_url)
        except ValueError as exc:
            # Non-retryable validation error
            self._log.warning("Skipping item (validation): %s", exc)
            return
        except Exception as exc:
            self._log.error(
                "Failed to download %s after %d retries: %s",
                item.media_url, PipelineConfig.MAX_RETRIES, exc,
            )
            return

        item.raw_bytes = raw_bytes
        item.content_type = content_type
        item.file_extension = mime_to_extension(content_type)

        self._log.info(
            "[%s] STEP 2: Downloaded %.1f KB  ct=%s  ext=%s",
            item.source,
            len(raw_bytes) / 1024,
            content_type,
            item.file_extension,
        )
        await self.preprocess_queue.put(item)

    async def run(self) -> None:
        """Continuously consume fetch_queue and process items."""
        self._log.info("Fetcher worker %d started.", self.worker_id)
        while True:
            try:
                item: MediaItem = await self.fetch_queue.get()
                await self.process_item(item)
            except asyncio.CancelledError:
                self._log.info("Fetcher worker %d shutting down.", self.worker_id)
                break
            except Exception as exc:
                self._log.error("Unexpected error in fetcher: %s", exc, exc_info=True)
            finally:
                try:
                    self.fetch_queue.task_done()
                except ValueError:
                    pass  # task_done called more times than get — ignore

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()


async def close_shared_connector() -> None:
    """Call at pipeline shutdown to release the shared TCP connector."""
    global _CONNECTOR
    if _CONNECTOR and not _CONNECTOR.closed:
        await _CONNECTOR.close()
        _CONNECTOR = None
