"""
config.py
=========
Central configuration for the Digital Asset Protection Crawler Pipeline.

All secrets are sourced from the .env file (via python-dotenv).
Runtime tuning values have sensible defaults and can be overridden in .env.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# ── Load .env ────────────────────────────────────────────────────────────────
# Walk up from this file's location (src/crawler_pipeline/) to the project root
# to find the .env file, so the pipeline can be launched from any working directory.
_env_path = Path(__file__).parent.parent.parent / ".env"
load_dotenv(dotenv_path=_env_path)


# ── Helpers ──────────────────────────────────────────────────────────────────
def _require(key: str) -> str:
    """Return env var or raise a clear error if it's missing."""
    val = os.getenv(key, "").strip()
    if not val:
        raise EnvironmentError(
            f"[config] Required environment variable '{key}' is not set. "
            f"Please fill it in your .env file."
        )
    return val


def _get(key: str, default: str = "") -> str:
    return os.getenv(key, default).strip()


def _get_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except ValueError:
        return default


def _get_list(key: str, default: str = "") -> list[str]:
    raw = os.getenv(key, default).strip()
    return [item.strip() for item in raw.split(",") if item.strip()]


# ── Reddit ───────────────────────────────────────────────────────────────────
class RedditConfig:
    CLIENT_ID: str = _get("REDDIT_CLIENT_ID")
    CLIENT_SECRET: str = _get("REDDIT_CLIENT_SECRET")
    USERNAME: str = _get("REDDIT_USERNAME")
    PASSWORD: str = _get("REDDIT_PASSWORD")
    USER_AGENT: str = _get("REDDIT_USER_AGENT", "DigitalAssetProtection/1.0")

    # Are API credentials provided?
    @classmethod
    def api_enabled(cls) -> bool:
        return bool(cls.CLIENT_ID and cls.CLIENT_SECRET)

    SUBREDDITS: list[str] = _get_list("REDDIT_SUBREDDITS", "pics,videos,gifs")
    CRAWL_LIMIT: int = _get_int("REDDIT_CRAWL_LIMIT", 25)
    CRAWL_INTERVAL_SECONDS: float = float(
        _get("REDDIT_CRAWL_INTERVAL_SECONDS", "60")
    )


# ── Twitter / X ──────────────────────────────────────────────────────────────
class TwitterConfig:
    BEARER_TOKEN: str = _get("TWITTER_BEARER_TOKEN")
    API_KEY: str = _get("TWITTER_API_KEY")
    API_SECRET: str = _get("TWITTER_API_SECRET")
    ACCESS_TOKEN: str = _get("TWITTER_ACCESS_TOKEN")
    ACCESS_TOKEN_SECRET: str = _get("TWITTER_ACCESS_TOKEN_SECRET")

    @classmethod
    def api_enabled(cls) -> bool:
        return bool(cls.BEARER_TOKEN)

    SEARCH_LIMIT: int = _get_int("TWITTER_SEARCH_LIMIT", 20)
    CRAWL_INTERVAL_SECONDS: float = float(
        _get("TWITTER_CRAWL_INTERVAL_SECONDS", "90")
    )


# ── Detection API ─────────────────────────────────────────────────────────────
class ApiConfig:
    BASE_URL: str = _get("DETECTION_API_BASE_URL", "http://localhost:3000")
    ENDPOINT: str = _get("DETECTION_API_ENDPOINT", "/api/assets/scan/file")
    API_KEY: str = _get("DETECTION_API_KEY", "")

    @classmethod
    def match_url(cls) -> str:
        return cls.BASE_URL.rstrip("/") + "/" + cls.ENDPOINT.lstrip("/")


# ── Pipeline ──────────────────────────────────────────────────────────────────
class PipelineConfig:
    KEYWORDS: list[str] = _get_list("KEYWORDS", "deepfake,stolen content,copyright,DMCA")

    FETCH_QUEUE_SIZE: int = _get_int("FETCH_QUEUE_SIZE", 100)
    PREPROCESS_QUEUE_SIZE: int = _get_int("PREPROCESS_QUEUE_SIZE", 100)
    API_QUEUE_SIZE: int = _get_int("API_QUEUE_SIZE", 100)

    NUM_FETCHER_WORKERS: int = _get_int("NUM_FETCHER_WORKERS", 4)
    NUM_PREPROCESSOR_WORKERS: int = _get_int("NUM_PREPROCESSOR_WORKERS", 2)
    NUM_API_SENDER_WORKERS: int = _get_int("NUM_API_SENDER_WORKERS", 2)

    MAX_RETRIES: int = _get_int("MAX_RETRIES", 3)
    REQUEST_TIMEOUT_SECONDS: int = _get_int("REQUEST_TIMEOUT_SECONDS", 30)

    # Supported media MIME types → local extension mapping
    SUPPORTED_MEDIA: dict[str, str] = {
        "image/jpeg": "jpg",
        "image/png": "png",
        "image/webp": "webp",
        "image/gif": "gif",
        "video/mp4": "mp4",
        "video/gif": "gif",
    }


# ── Standalone Mode (Social — Reddit + Twitter) ──────────────────────────────
class StandaloneConfig:
    """Settings for the Playwright scrape-and-save standalone mode (Reddit/Twitter)."""

    # Root directory where Reddit/Twitter scraped images are saved.
    # Can be overridden by --output CLI flag or STANDALONE_SUSPICIOUS_DIR env var.
    SUSPICIOUS_DIR: str = _get("STANDALONE_SUSPICIOUS_DIR", "./suspicious")

    # Legacy alias kept so any code still using ASSETS_DIR doesn't break.
    ASSETS_DIR: str = _get("STANDALONE_SUSPICIOUS_DIR", "./suspicious")

    # Max items per source per keyword in one standalone pass.
    STANDALONE_LIMIT: int = _get_int("STANDALONE_LIMIT", 10)


# ── Stock Image Sites (Unsplash / Pexels / Pixabay) ──────────────────────────
class StockConfig:
    """
    Configuration for the stock-image-site scraper.

    API keys are free but require a one-time sign-up:
      Unsplash → https://unsplash.com/developers
      Pexels   → https://www.pexels.com/api/
      Pixabay  → https://pixabay.com/api/docs/
    """

    UNSPLASH_ACCESS_KEY: str = _get("UNSPLASH_ACCESS_KEY", "")
    PEXELS_API_KEY: str = _get("PEXELS_API_KEY", "")
    PIXABAY_API_KEY: str = _get("PIXABAY_API_KEY", "")

    # Root directory where stock images are saved (separate from suspicious/).
    ASSETS_DIR: str = _get("STOCK_ASSETS_DIR", "./assets")

    # Max images per site per keyword in one stock-scrape pass.
    STOCK_LIMIT: int = _get_int("STOCK_LIMIT", 10)


# ── Logging ───────────────────────────────────────────────────────────────────
LOG_LEVEL: str = _get("LOG_LEVEL", "INFO")
LOG_FORMAT: str = (
    "%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s"
)
LOG_DATE_FORMAT: str = "%Y-%m-%d %H:%M:%S"
