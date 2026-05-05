"""
standalone.py
=============
Standalone Playwright scrape-and-save mode (social sources) for the Digital
Asset Protection Crawler Pipeline.

Scrapes Reddit and Twitter/X; saves images to::
    suspicious/<sha256>_<source>.<ext>

Usage (via main.py):
    python main.py --standalone --source social                        # home mode
    python main.py --standalone --source social --mode top             # top mode
    python main.py --standalone --source social --keywords "sports" --limit 20
    python main.py --standalone --source social --mode top --targets targets.json

Modes:
  home  Keyword-based search across Reddit and the Twitter fallback chain.
        This is the default.
  top   Browses specific subreddits and X accounts defined in targets.json
        (or the file passed via --targets). No keyword required.

Output folder: ./suspicious   (configurable via STANDALONE_SUSPICIOUS_DIR env var
               or the --output CLI flag).
"""

import asyncio
import datetime
import hashlib
import json
import re
from pathlib import Path
from typing import Optional

import aiohttp

from .config import PipelineConfig, StandaloneConfig  # StandaloneConfig.SUSPICIOUS_DIR
from .crawler import (
    RedditPlaywrightClient,
    TwitterPlaywrightClient,
    RedditTopPlaywrightClient,
    TwitterAccountPlaywrightClient,
    _REDDIT_IMAGE_RE as _IMAGE_EXT_RE,   # same pattern, reuse the name locally
    _POST_IMAGE_CDN_HOSTS,
    _EXCLUDED_HOSTS,
)
from .utils import get_logger, async_retry

logger = get_logger("standalone")

# Image-only MIME types accepted in standalone mode
_IMAGE_MIME: dict[str, str] = {
    "image/jpeg": "jpg",
    "image/png": "png",
    "image/webp": "webp",
    "image/gif": "gif",
}
# CDN regexes (_IMAGE_EXT_RE, _POST_IMAGE_CDN_HOSTS, _EXCLUDED_HOSTS)
# are imported from crawler.py to avoid duplication.

# Federation bridge / embedded-article post_url patterns to reject.
# Examples that match: web.brid.gy/r/https://mashable.com/...
#                      fed.brid.gy/r/https://bsky.app/...
_BRIDGE_POST_RE = re.compile(
    r"brid\.gy"       # web.brid.gy / fed.brid.gy / activitypub.brid.gy
    r"|\/r\/https?:\/\/",  # embedded redirect URL inside a path segment
    re.IGNORECASE,
)


def _sha256_url(url: str) -> str:
    """Return the SHA-256 hex digest of a URL (used as filename stem)."""
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


def _ext_from_url(url: str) -> Optional[str]:
    """Derive a file extension from a URL, or None if not a post-content image URL."""
    if _EXCLUDED_HOSTS.match(url):
        return None
    m = _IMAGE_EXT_RE.search(url)
    if m:
        return m.group(1).lower().replace("jpeg", "jpg")
    # CDN hosts serve images without an explicit extension; 'jpg' is a safe
    # default — the real extension is confirmed from Content-Type at download.
    if _POST_IMAGE_CDN_HOSTS.match(url):
        return "jpg"
    return None


def _ext_from_content_type(ct: str) -> Optional[str]:
    """Derive a file extension from a Content-Type header."""
    base = ct.split(";")[0].strip().lower()
    return _IMAGE_MIME.get(base)


# ─────────────────────────────────────────────────────────────────────────────
# Manifest helpers
# ─────────────────────────────────────────────────────────────────────────────

class Manifest:
    """
    Thin wrapper around manifest.json.

    Loads on construction, provides O(1) URL-seen lookup, and flushes to disk
    after each save so partial runs are never lost.
    """

    def __init__(self, path: Path) -> None:
        self._path = path
        self._entries: list[dict] = []
        self._seen_urls: set[str] = set()
        self._load()

    def _load(self) -> None:
        if self._path.exists():
            try:
                data = json.loads(self._path.read_text(encoding="utf-8"))
                self._entries = data if isinstance(data, list) else []
                self._seen_urls = {e["media_url"] for e in self._entries if "media_url" in e}
                logger.info("Manifest loaded — %d existing entries.", len(self._entries))
            except (json.JSONDecodeError, OSError) as exc:
                logger.warning("Could not load manifest (%s) — starting fresh.", exc)

    def is_seen(self, media_url: str) -> bool:
        return media_url in self._seen_urls

    def add(self, entry: dict) -> None:
        self._entries.append(entry)
        self._seen_urls.add(entry["media_url"])
        self._flush()

    def _flush(self) -> None:
        try:
            self._path.write_text(
                json.dumps(self._entries, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
        except OSError as exc:
            logger.error("Failed to write manifest: %s", exc)


# ─────────────────────────────────────────────────────────────────────────────
# StandaloneRunner
# ─────────────────────────────────────────────────────────────────────────────

class StandaloneRunner:
    """
    Orchestrates a single Playwright scrape-and-save pass.

    Parameters
    ----------
    output_dir : Path
        Root directory for saved assets (default: ./assets).
    keywords : list[str] | None
        Keywords to search for. Used in 'home' mode. Falls back to PipelineConfig.KEYWORDS.
    limit : int
        Max images per source per keyword (home) or per subreddit/account (top).
    mode : str
        'home' — keyword-based search (default).
        'top'  — browse subreddits and accounts listed in targets_file.
    targets_file : Path | None
        Path to targets.json. Defaults to <project_root>/targets.json.
        Only used in 'top' mode.
    """

    def __init__(
        self,
        output_dir: Optional[Path] = None,
        keywords: Optional[list[str]] = None,
        limit: Optional[int] = None,
        mode: str = "home",
        targets_file: Optional[Path] = None,
    ) -> None:
        # Default output → suspicious/ (Reddit + Twitter images)
        self._output_dir = output_dir or Path(StandaloneConfig.SUSPICIOUS_DIR)
        self._keywords: list[str] = keywords or PipelineConfig.KEYWORDS
        self._limit: int = limit if limit is not None else StandaloneConfig.STANDALONE_LIMIT
        self._mode: str = mode.lower().strip()
        self._targets_file: Path = targets_file or (
            Path(__file__).parents[3] / "targets.json"
        )

        # Home-mode clients
        self._reddit_client = RedditPlaywrightClient()
        self._twitter_client = TwitterPlaywrightClient()
        # Top-mode clients
        self._reddit_top_client = RedditTopPlaywrightClient()
        self._twitter_account_client = TwitterAccountPlaywrightClient()

        # Prepare output directory (flat — no per-source subdirectories)
        self._output_dir.mkdir(parents=True, exist_ok=True)

        self._manifest = Manifest(self._output_dir / "manifest.json")
        self._stats = {"scraped": 0, "downloaded": 0, "skipped_dup": 0, "skipped_type": 0, "errors": 0}

    def _load_targets(self) -> dict:
        """Load and validate targets.json. Returns empty structure on error."""
        default: dict = {"reddit": {"subreddits": [], "sort": "top", "time_filter": "day"},
                          "twitter": {"accounts": []}}
        if not self._targets_file.exists():
            logger.warning(
                "targets.json not found at %s — top mode will scrape nothing. "
                "Create the file or pass --targets <path>.",
                self._targets_file,
            )
            return default
        try:
            data = json.loads(self._targets_file.read_text(encoding="utf-8"))
            logger.info("Loaded targets from %s", self._targets_file)
            return data
        except (json.JSONDecodeError, OSError) as exc:
            logger.error("Could not read targets.json (%s) — using defaults.", exc)
            return default

    # ── Download ──────────────────────────────────────────────────────────────

    @async_retry(max_attempts=3, delay_seconds=2.0, backoff_factor=2.0,
                 exceptions=(aiohttp.ClientError, asyncio.TimeoutError))
    async def _fetch_image(self, url: str, session: aiohttp.ClientSession) -> tuple[bytes, str]:
        """
        Download an image URL.

        Returns (raw_bytes, extension).
        Raises ValueError for unsupported content types (not retried).
        """
        timeout = aiohttp.ClientTimeout(total=30)
        async with session.get(url, timeout=timeout, allow_redirects=True) as resp:
            resp.raise_for_status()
            ct = resp.headers.get("Content-Type", "")
            ext = _ext_from_content_type(ct) or _ext_from_url(str(resp.url))
            if ext is None:
                raise ValueError(f"Unsupported content type: {ct!r}")
            # Guard against huge files (50 MB cap)
            chunks = []
            size = 0
            async for chunk in resp.content.iter_chunked(65536):
                size += len(chunk)
                if size > 50 * 1024 * 1024:
                    raise ValueError("File exceeds 50 MB limit — skipping.")
                chunks.append(chunk)
            return b"".join(chunks), ext

    # ── Save ──────────────────────────────────────────────────────────────────

    async def _save(
        self,
        session: aiohttp.ClientSession,
        source: str,
        post_url: str,
        media_url: str,
        media_type: str,
        keyword: str,
        timestamp: str,
    ) -> None:
        """Download and persist one media item."""
        self._stats["scraped"] += 1

        # Idempotency check
        if self._manifest.is_seen(media_url):
            logger.debug("Already saved, skipping: %s", media_url)
            self._stats["skipped_dup"] += 1
            return

        # Reject bridged web articles — only genuine social posts are wanted.
        # Matches URLs like web.brid.gy/r/https://news-site.com/...
        if _BRIDGE_POST_RE.search(post_url):
            logger.debug("Skipping bridge/article post_url: %s", post_url)
            self._stats["skipped_type"] += 1
            return

        # Images only — quick URL pre-filter before we even make an HTTP call
        if media_type != "image" or _ext_from_url(media_url) is None:
            # Still try fetching; content-type check inside will reject if not image
            pass

        try:
            raw, ext = await self._fetch_image(media_url, session)
        except ValueError as exc:
            logger.warning("Skipping %s — %s", media_url, exc)
            self._stats["skipped_type"] += 1
            return
        except Exception as exc:
            logger.error("Download failed for %s: %s", media_url, exc)
            self._stats["errors"] += 1
            return

        # Build path:  suspicious/<sha256>_<source>.<ext>
        # The source suffix in the filename makes provenance visible without subdirs.
        stem = _sha256_url(media_url)
        filename = f"{stem}_{source}.{ext}"
        abs_path = self._output_dir / filename

        if abs_path.exists():
            logger.debug("File already on disk, skipping: %s", abs_path)
            self._stats["skipped_dup"] += 1
            return

        try:
            abs_path.write_bytes(raw)
        except OSError as exc:
            logger.error("Could not write %s: %s", abs_path, exc)
            self._stats["errors"] += 1
            return

        # Record in manifest
        entry = {
            "file": filename,
            "source": source,
            "post_url": post_url,
            "media_url": media_url,
            "media_type": media_type,
            "keyword_matched": keyword,
            "timestamp": timestamp,
            "saved_at": datetime.datetime.utcnow().isoformat() + "Z",
            "file_size_bytes": len(raw),
        }
        self._manifest.add(entry)
        self._stats["downloaded"] += 1
        logger.info(
            "✅ Saved  [%s] %s → %s  (%d KB)",
            source, media_url, filename, len(raw) // 1024,
        )

    # ── Home mode — keyword search ─────────────────────────────────────────────

    async def _scrape_reddit(self, session: aiohttp.ClientSession) -> None:
        logger.info("─── Reddit Playwright scrape [home mode] ───")
        for keyword in self._keywords:
            logger.info("Scraping Reddit for %r (limit=%d)…", keyword, self._limit)
            try:
                posts = await self._reddit_client.search_submissions(keyword, self._limit)
                for post in posts:
                    await self._save(
                        session=session,
                        source="reddit",
                        post_url=post["post_url"],
                        media_url=post["media_url"],
                        media_type=post["media_type"],
                        keyword=keyword,
                        timestamp=post["timestamp"],
                    )
            except Exception as exc:
                logger.error("Reddit scrape error for keyword=%r: %s", keyword, exc)

    async def _scrape_twitter(self, session: aiohttp.ClientSession) -> None:
        logger.info("─── Twitter/X Playwright scrape [home mode] ───")
        for keyword in self._keywords:
            logger.info("Scraping Twitter for %r (limit=%d)…", keyword, self._limit)
            try:
                posts = await self._twitter_client.search(keyword, self._limit)
                for post in posts:
                    await self._save(
                        session=session,
                        source="twitter",
                        post_url=post["post_url"],
                        media_url=post["media_url"],
                        media_type=post["media_type"],
                        keyword=keyword,
                        timestamp=post["timestamp"],
                    )
            except Exception as exc:
                logger.error("Twitter scrape error for keyword=%r: %s", keyword, exc)

    # ── Top mode — subreddits + accounts ──────────────────────────────────────

    async def _scrape_reddit_top(
        self, session: aiohttp.ClientSession, targets: dict
    ) -> None:
        reddit_cfg = targets.get("reddit", {})
        subreddits: list[str] = reddit_cfg.get("subreddits", [])
        sort: str = reddit_cfg.get("sort", "top")
        time_filter: str = reddit_cfg.get("time_filter", "day")

        if not subreddits:
            logger.warning("top mode: no subreddits defined in targets.json.")
            return

        logger.info(
            "─── Reddit top scrape [top mode] — %d subreddits, sort=%s t=%s ───",
            len(subreddits), sort, time_filter,
        )
        for sub in subreddits:
            logger.info("Scraping r/%s (limit=%d)…", sub, self._limit)
            try:
                posts = await self._reddit_top_client.scrape_subreddit(
                    sub, self._limit, sort=sort, time_filter=time_filter
                )
                for post in posts:
                    await self._save(
                        session=session,
                        source="reddit",
                        post_url=post["post_url"],
                        media_url=post["media_url"],
                        media_type=post["media_type"],
                        keyword=f"r/{sub}",
                        timestamp=post["timestamp"],
                    )
            except Exception as exc:
                logger.error("Reddit top scrape error for r/%s: %s", sub, exc)

    async def _scrape_twitter_top(
        self, session: aiohttp.ClientSession, targets: dict
    ) -> None:
        accounts: list[str] = targets.get("twitter", {}).get("accounts", [])

        if not accounts:
            logger.warning("top mode: no Twitter accounts defined in targets.json.")
            return

        logger.info(
            "─── Twitter account scrape [top mode] — %d accounts ───", len(accounts)
        )
        for account in accounts:
            # Strip leading @ if user included it
            account = account.lstrip("@")
            logger.info("Scraping @%s (limit=%d)…", account, self._limit)
            try:
                posts = await self._twitter_account_client.scrape_account(account, self._limit)
                for post in posts:
                    await self._save(
                        session=session,
                        source="twitter",
                        post_url=post["post_url"],
                        media_url=post["media_url"],
                        media_type=post["media_type"],
                        keyword=f"@{account}",
                        timestamp=post["timestamp"],
                    )
            except Exception as exc:
                logger.error("Twitter account scrape error for @%s: %s", account, exc)


    # ── Main entry ────────────────────────────────────────────────────────────

    async def run(self) -> None:
        """Run a single full scrape-and-save pass in the configured mode."""
        logger.info("=" * 64)
        logger.info("Social scrape (Reddit + Twitter) — Digital Asset Protection Pipeline")
        logger.info("Mode             : %s", self._mode)
        logger.info("Output directory : %s  (suspicious/)", self._output_dir.resolve())
        logger.info("Limit per source : %d per target", self._limit)
        if self._mode == "home":
            logger.info("Keywords         : %s", ", ".join(self._keywords))
        elif self._mode == "top":
            logger.info("Targets file     : %s", self._targets_file)
        logger.info("=" * 64)

        connector = aiohttp.TCPConnector(limit=10)
        async with aiohttp.ClientSession(connector=connector) as session:
            if self._mode == "top":
                targets = self._load_targets()
                await self._scrape_reddit_top(session, targets)
                await self._scrape_twitter_top(session, targets)
            else:
                # Default: home mode
                await self._scrape_reddit(session)
                await self._scrape_twitter(session)

        logger.info("=" * 64)
        logger.info("Standalone scrape complete.")
        logger.info(
            "  Scraped: %d   Downloaded: %d   Duplicates: %d   "
            "Type-skipped: %d   Errors: %d",
            self._stats["scraped"],
            self._stats["downloaded"],
            self._stats["skipped_dup"],
            self._stats["skipped_type"],
            self._stats["errors"],
        )
        logger.info("Manifest → %s", (self._output_dir / "manifest.json").resolve())
        logger.info("=" * 64)
