"""
utils.py
========
Shared utility functions for the Digital Asset Protection Crawler Pipeline.

Includes:
  - Structured logging setup
  - URL/content deduplication via SHA-256 hashing
  - Base64 encode/decode helpers
  - Retry decorator for async coroutines
  - Media metadata dataclass
"""

import asyncio
import base64
import hashlib
import logging
import time
from dataclasses import dataclass, field
from functools import wraps
from typing import Any, Callable, Coroutine, Optional

from .config import LOG_LEVEL, LOG_FORMAT, LOG_DATE_FORMAT, PipelineConfig


# ── Logging ───────────────────────────────────────────────────────────────────

def setup_logging(log_file: Optional[str] = None) -> None:
    """Configure root logger. Call once at application startup.

    Parameters
    ----------
    log_file:
        Optional path to a file where logs should also be written.
        Parent directories are created automatically.
    """
    from pathlib import Path
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
        format=LOG_FORMAT,
        datefmt=LOG_DATE_FORMAT,
    )
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(log_path, encoding="utf-8")
        fh.setLevel(getattr(logging, LOG_LEVEL.upper(), logging.INFO))
        fh.setFormatter(logging.Formatter(fmt=LOG_FORMAT, datefmt=LOG_DATE_FORMAT))
        logging.getLogger().addHandler(fh)
        logging.getLogger().info("Log file: %s", log_path.resolve())
    # Suppress noisy third-party loggers
    logging.getLogger("asyncpraw").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    logging.getLogger("playwright").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """Return a named logger (module-level use)."""
    return logging.getLogger(name)


# ── Deduplication ─────────────────────────────────────────────────────────────

class SeenCache:
    """
    Thread-safe (asyncio-safe) in-memory set to track seen URL hashes.

    Optionally accepts a TTL (seconds) after which entries are purged.
    Designed so the underlying store can be swapped for Redis in one place.
    """

    def __init__(self, ttl_seconds: Optional[int] = None) -> None:
        self._seen: dict[str, float] = {}   # hash → timestamp
        self._ttl = ttl_seconds
        self._lock = asyncio.Lock()
        self._logger = get_logger("utils.SeenCache")

    def _hash(self, value: str) -> str:
        return hashlib.sha256(value.encode("utf-8")).hexdigest()

    def _evict_expired(self) -> None:
        if self._ttl is None:
            return
        now = time.monotonic()
        expired = [k for k, ts in self._seen.items() if now - ts > self._ttl]
        for k in expired:
            del self._seen[k]
        if expired:
            self._logger.debug("Evicted %d expired cache entries.", len(expired))

    async def is_seen(self, url: str) -> bool:
        """Return True if this URL has already been processed."""
        async with self._lock:
            self._evict_expired()
            return self._hash(url) in self._seen

    async def mark_seen(self, url: str) -> None:
        """Record that this URL has been processed."""
        async with self._lock:
            self._evict_expired()
            self._seen[self._hash(url)] = time.monotonic()

    async def check_and_mark(self, url: str) -> bool:
        """
        Atomically check and mark a URL.
        Returns True if it was already seen (skip), False if new (proceed).
        """
        async with self._lock:
            self._evict_expired()
            h = self._hash(url)
            if h in self._seen:
                return True
            self._seen[h] = time.monotonic()
            return False

    @property
    def size(self) -> int:
        return len(self._seen)


# Shared singleton used across all crawlers
seen_cache = SeenCache(ttl_seconds=3600)   # entries expire after 1 hour


# ── Base64 Helpers ────────────────────────────────────────────────────────────

def bytes_to_b64(data: bytes) -> str:
    """Encode raw bytes to a Base64 string."""
    return base64.b64encode(data).decode("utf-8")


def b64_to_bytes(data: str) -> bytes:
    """Decode a Base64 string back to bytes."""
    return base64.b64decode(data.encode("utf-8"))


# ── Data Model ────────────────────────────────────────────────────────────────

@dataclass
class MediaItem:
    """
    Represents a single media asset flowing through the pipeline.

    Stages add to this object as it progresses:
      Crawler → Fetcher → Preprocessor → API Sender
    """

    # Set by crawler
    post_url: str = ""
    media_url: str = ""
    source: str = ""          # "reddit" | "twitter"
    timestamp: str = ""
    media_type: str = ""      # "image" | "video"
    keyword_matched: str = ""

    # Set by fetcher
    raw_bytes: Optional[bytes] = field(default=None, repr=False)
    content_type: str = ""
    file_extension: str = ""

    # Set by preprocessor (list because video yields multiple frames)
    processed_b64_frames: list[str] = field(default_factory=list, repr=False)

    # Set by API sender
    api_response: Optional[dict[str, Any]] = None
    matched: Optional[bool] = None
    similarity_score: Optional[float] = None

    def to_api_payload(self) -> dict[str, Any]:
        """
        Serialize this item into the JSON payload expected by the Detection API.
        """
        return {
            "url": self.media_url,
            "source": self.source,
            "timestamp": self.timestamp,
            "media_type": self.media_type,
            "processed_data": self.processed_b64_frames,
            "metadata": {
                "post_url": self.post_url,
                "content_type": self.content_type,
                "keyword_matched": self.keyword_matched,
            },
        }


# ── Async Retry Decorator ─────────────────────────────────────────────────────

def async_retry(
    max_attempts: int = PipelineConfig.MAX_RETRIES,
    delay_seconds: float = 2.0,
    backoff_factor: float = 2.0,
    exceptions: tuple[type[Exception], ...] = (Exception,),
):
    """
    Decorator that retries an async function up to `max_attempts` times.

    Uses exponential back-off: delay → delay*factor → delay*factor² …
    Raises the last exception if all attempts fail.
    """
    def decorator(func: Callable[..., Coroutine]) -> Callable[..., Coroutine]:
        logger = get_logger(f"utils.retry.{func.__name__}")

        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            wait = delay_seconds
            last_exc: Optional[Exception] = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return await func(*args, **kwargs)
                except exceptions as exc:
                    last_exc = exc
                    if attempt < max_attempts:
                        logger.warning(
                            "Attempt %d/%d failed for '%s': %s. Retrying in %.1fs…",
                            attempt, max_attempts, func.__name__, exc, wait,
                        )
                        await asyncio.sleep(wait)
                        wait *= backoff_factor
                    else:
                        logger.error(
                            "All %d attempts failed for '%s': %s",
                            max_attempts, func.__name__, exc,
                        )
            raise last_exc  # type: ignore[misc]

        return wrapper
    return decorator


# ── Misc helpers ──────────────────────────────────────────────────────────────

def keyword_matches(text: str, keywords: list[str]) -> str:
    """
    Return the first matching keyword found in `text` (case-insensitive),
    or an empty string if none match.
    """
    lower = text.lower()
    for kw in keywords:
        if kw.lower() in lower:
            return kw
    return ""


def is_supported_content_type(content_type: str) -> bool:
    """Check whether a MIME type is in our supported media set."""
    # Strip parameters like '; charset=utf-8'
    base_type = content_type.split(";")[0].strip().lower()
    return base_type in PipelineConfig.SUPPORTED_MEDIA


def mime_to_extension(content_type: str) -> str:
    """Map a MIME type to the expected file extension."""
    base_type = content_type.split(";")[0].strip().lower()
    return PipelineConfig.SUPPORTED_MEDIA.get(base_type, "bin")
