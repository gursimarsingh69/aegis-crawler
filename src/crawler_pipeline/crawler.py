"""
crawler.py
==========
Crawler module for the Digital Asset Protection Pipeline.

Contains:
  - RedditCrawler   — API primary (asyncpraw) → Playwright fallback
  - TwitterCrawler  — API primary (httpx/tweepy-like) → Playwright fallback
  - BaseCrawler     — shared interface

Each crawler:
  1. Searches for posts matching configured keywords.
  2. Extracts (post_url, media_url, timestamp, media_type).
  3. Skips duplicates via SeenCache.
  4. Pushes MediaItem objects into the shared fetch_queue.
  5. Respects configured crawl interval (rate limiting).
"""

import asyncio
import datetime
import json
import re
from abc import ABC, abstractmethod
from typing import AsyncGenerator, Optional

import aiohttp
import asyncpraw
import asyncpraw.models

try:
    from playwright.async_api import async_playwright, Browser, Page
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

from .config import RedditConfig, TwitterConfig, PipelineConfig
from .utils import MediaItem, SeenCache, get_logger, keyword_matches, seen_cache

logger = get_logger("crawler")


# ─────────────────────────────────────────────────────────────────────────────
# Base
# ─────────────────────────────────────────────────────────────────────────────

class BaseCrawler(ABC):
    """Abstract base class for all source crawlers."""

    source: str = "unknown"

    def __init__(
        self,
        fetch_queue: asyncio.Queue,
        keywords: Optional[list[str]] = None,
        crawl_limit: Optional[int] = None,
    ) -> None:
        self.queue = fetch_queue
        # Use caller-supplied keywords; fall back to .env / PipelineConfig defaults.
        self.keywords: list[str] = keywords or PipelineConfig.KEYWORDS
        self._crawl_limit_override: Optional[int] = crawl_limit
        self._cache: SeenCache = seen_cache
        self._log = get_logger(f"crawler.{self.source}")

    @abstractmethod
    async def crawl_once(self) -> None:
        """Perform one crawl pass."""
        ...

    async def run(self) -> None:
        """Run the crawler in a continuous loop with rate limiting."""
        self._log.info("Starting %s crawler.", self.source)
        while True:
            try:
                await self.crawl_once()
            except Exception as exc:
                self._log.error("Error during crawl pass: %s", exc, exc_info=True)
            interval = self._crawl_interval()
            self._log.debug("Sleeping %.0fs before next crawl pass…", interval)
            await asyncio.sleep(interval)

    def _crawl_interval(self) -> float:
        raise NotImplementedError

    async def _enqueue(self, item: MediaItem) -> None:
        """Deduplicate then enqueue."""
        if await self._cache.check_and_mark(item.media_url):
            self._log.debug("Duplicate skipped: %s", item.media_url)
            return
        self._log.info(
            "[%s] Queuing %s ← %s  (kw=%r)",
            self.source, item.media_type, item.media_url, item.keyword_matched,
        )
        await self.queue.put(item)


# ─────────────────────────────────────────────────────────────────────────────
# Reddit
# ─────────────────────────────────────────────────────────────────────────────

# Image/video extensions we want to follow from Reddit posts
_REDDIT_IMAGE_RE = re.compile(r"\.(jpg|jpeg|png|webp|gif)(\?.*)?$", re.IGNORECASE)
_REDDIT_VIDEO_RE = re.compile(r"\.(mp4|mov)(\?.*)?$", re.IGNORECASE)
_IMGUR_RE = re.compile(r"https?://(?:i\.)?imgur\.com/(\w+)(?:\.\w+)?")

# CDN hosts that serve actual POST CONTENT images (not avatars/icons/thumbnails)
# Deliberately excludes: styles.redditmedia.com, b.thumbs.redditmedia.com,
# www.redditstatic.com (all of which serve UI assets, not post images)
_POST_IMAGE_CDN_HOSTS = re.compile(
    r"https?://(?:"
    r"(?:i|preview|external-preview)\.redd\.it|"
    r"i\.redd\.it|"
    r"pbs\.twimg\.com/media|"
    r"i\.imgur\.com"
    r")",
    re.IGNORECASE,
)

# Hosts we explicitly want to EXCLUDE (community icons, subreddit banners, etc.)
_EXCLUDED_HOSTS = re.compile(
    r"https?://(?:styles|b\.thumbs|www)\.redditmedia\.com",
    re.IGNORECASE,
)


def _classify_url(url: str) -> Optional[str]:
    """Return 'image', 'video', or None if the URL is not post-content media."""
    # Reject UI/icon hosts first
    if _EXCLUDED_HOSTS.match(url):
        return None
    if _REDDIT_IMAGE_RE.search(url):
        return "image"
    if _REDDIT_VIDEO_RE.search(url):
        return "video"
    # Accept known post-image CDN hosts even without a file extension
    if _POST_IMAGE_CDN_HOSTS.match(url):
        return "image"
    return None


def _extract_reddit_media(submission: asyncpraw.models.Submission) -> list[tuple[str, str]]:
    """
    Extract (media_url, media_type) pairs from a Reddit submission.
    Handles: direct links, reddit video (DASH), gallery posts.
    Returns a list because gallery posts can have multiple images.
    """
    results: list[tuple[str, str]] = []
    url: str = submission.url or ""

    # Direct image / video link
    kind = _classify_url(url)
    if kind:
        results.append((url, kind))
        return results

    # Reddit hosted video
    if submission.is_video and hasattr(submission, "media") and submission.media:
        try:
            dash_url: str = (
                submission.media["reddit_video"]["fallback_url"]
            )
            results.append((dash_url, "video"))
            return results
        except (KeyError, TypeError):
            pass

    # Reddit gallery
    if hasattr(submission, "is_gallery") and submission.is_gallery:
        try:
            for item in submission.gallery_data["items"]:
                media_id = item["media_id"]
                meta = submission.media_metadata[media_id]
                if meta.get("e") == "Image":
                    # Highest resolution source
                    img_url = meta["s"]["u"].replace("&amp;", "&")
                    results.append((img_url, "image"))
        except (KeyError, TypeError, AttributeError):
            pass

    # Imgur direct
    m = _IMGUR_RE.match(url)
    if m:
        results.append((f"https://i.imgur.com/{m.group(1)}.jpg", "image"))

    return results


class RedditAPIClient:
    """Wrapper around asyncpraw for Reddit API access."""

    def __init__(self) -> None:
        self._reddit = asyncpraw.Reddit(
            client_id=RedditConfig.CLIENT_ID,
            client_secret=RedditConfig.CLIENT_SECRET,
            username=RedditConfig.USERNAME,
            password=RedditConfig.PASSWORD,
            user_agent=RedditConfig.USER_AGENT,
        )
        self._log = get_logger("crawler.reddit.api")

    async def search_submissions(
        self, subreddit: str, keyword: str, limit: int
    ) -> AsyncGenerator[asyncpraw.models.Submission, None]:
        sub = await self._reddit.subreddit(subreddit)
        async for submission in sub.search(keyword, limit=limit, sort="new"):
            yield submission

    async def close(self) -> None:
        await self._reddit.close()


class RedditPlaywrightClient:
    """Playwright-based fallback scraper for Reddit search results."""

    BASE = "https://www.reddit.com"

    def __init__(self) -> None:
        self._log = get_logger("crawler.reddit.playwright")

    async def search_submissions(
        self, keyword: str, limit: int
    ) -> list[dict]:
        """
        Scrape Reddit search page for the given keyword.
        Returns a list of dicts with keys: post_url, media_url, media_type, timestamp.
        """
        if not PLAYWRIGHT_AVAILABLE:
            self._log.error("Playwright is not installed. Run: playwright install")
            return []

        results: list[dict] = []
        search_url = (
            f"{self.BASE}/search/?q={keyword.replace(' ', '+')}&sort=new&type=link"
        )

        async with async_playwright() as pw:
            browser: Browser = await pw.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                )
            )
            page: Page = await context.new_page()
            try:
                await page.goto(search_url, wait_until="domcontentloaded", timeout=30_000)
                await page.wait_for_timeout(4000)  # wait for Shreddit JS to hydrate

                # ── Reddit Shreddit SPA (2023+) selectors ──────────────────────
                # shreddit-post elements carry a content-href attribute with the
                # post permalink and may contain the preview image inline.
                post_links = await page.eval_on_selector_all(
                    "shreddit-post",
                    """
                    els => els.map(e => {
                        const href = e.getAttribute('content-href') || e.getAttribute('permalink');
                        return href ? (href.startsWith('http') ? href : 'https://www.reddit.com' + href) : null;
                    }).filter(Boolean)
                    """,
                )

                # Fallback: look for ordinary anchor tags that contain /r/ paths
                if not post_links:
                    post_links = await page.eval_on_selector_all(
                        "a[href*='/r/'][href*='/comments/']",
                        "els => [...new Set(els.map(e => e.href))].filter(h => h.includes('/comments/'))",
                    )

                self._log.info("Reddit Playwright: found %d post links for %r", len(post_links), keyword)

                for post_url in post_links[:limit]:
                    post_url = post_url.split("?")[0]
                    try:
                        await page.goto(post_url, wait_until="domcontentloaded", timeout=30_000)
                        await page.wait_for_timeout(3000)

                        # Collect only post-content image URLs (exclude icons/banners)
                        img_srcs = await page.evaluate(
                            """
                            () => {
                                const ALLOWED = ['external-preview.redd.it', 'preview.redd.it', 'i.redd.it', 'i.imgur.com'];
                                const srcs = new Set();
                                
                                // TARGET THE MAIN POST CONTENT ONLY
                                // Shreddit (New Reddit) uses specific tags for the main media
                                const mainMedia = document.querySelector('shreddit-media-viewer, .media-element, [data-click-id="media-resource"]');
                                if (mainMedia) {
                                    mainMedia.querySelectorAll('img').forEach(el => {
                                        try {
                                            const host = new URL(el.src).hostname;
                                            if (ALLOWED.some(a => host.includes(a))) srcs.add(el.src);
                                        } catch(e) {}
                                    });
                                }
                                
                                // Fallback: If no media viewer, look for large images in the article body
                                if (srcs.size === 0) {
                                    document.querySelectorAll('article img').forEach(el => {
                                        if (el.naturalWidth > 200 || el.width > 200) {
                                            try {
                                                const host = new URL(el.src).hostname;
                                                if (ALLOWED.some(a => host.includes(a))) srcs.add(el.src);
                                            } catch(e) {}
                                        }
                                    });
                                }
                                return [...srcs];
                            }
                            """
                        )

                        for src in img_srcs:
                            kind = _classify_url(src)
                            if kind:
                                results.append({
                                    "post_url": post_url,
                                    "media_url": src,
                                    "media_type": kind,
                                    "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
                                })
                                break  # one image per post
                    except Exception as post_exc:
                        self._log.warning("Error visiting post %s: %s", post_url, post_exc)

                    if len(results) >= limit:
                        break

            except Exception as exc:
                self._log.error("Playwright Reddit scrape failed: %s", exc)
            finally:
                await context.close()
                await browser.close()

        return results


class RedditCrawler(BaseCrawler):
    """
    Reddit crawler.
    Primary:  asyncpraw (official API)
    Fallback: Playwright scraper
    """

    source = "reddit"

    def __init__(
        self,
        fetch_queue: asyncio.Queue,
        keywords: Optional[list[str]] = None,
        crawl_limit: Optional[int] = None,
    ) -> None:
        super().__init__(fetch_queue, keywords=keywords, crawl_limit=crawl_limit)
        # Effective limit: caller override >> env var
        self._limit: int = crawl_limit if crawl_limit is not None else RedditConfig.CRAWL_LIMIT
        self._api_client: Optional[RedditAPIClient] = None
        self._playwright_client = RedditPlaywrightClient()

        if RedditConfig.api_enabled():
            self._log.info("Reddit: using API client (asyncpraw).")
            self._api_client = RedditAPIClient()
        else:
            self._log.warning(
                "Reddit: API credentials not set — falling back to Playwright scraper."
            )

    def _crawl_interval(self) -> float:
        return RedditConfig.CRAWL_INTERVAL_SECONDS

    async def crawl_once(self) -> None:
        if self._api_client:
            await self._crawl_via_api()
        else:
            await self._crawl_via_playwright()

    async def _crawl_via_api(self) -> None:
        assert self._api_client is not None
        for subreddit in RedditConfig.SUBREDDITS:
            for keyword in self.keywords:
                self._log.info(
                    "API: searching r/%s for %r (limit=%d)",
                    subreddit, keyword, self._limit,
                )
                try:
                    async for submission in self._api_client.search_submissions(
                        subreddit, keyword, self._limit
                    ):
                        text = f"{submission.title} {submission.selftext}"
                        matched_kw = keyword_matches(text, self.keywords) or keyword
                        pairs = _extract_reddit_media(submission)
                        ts = datetime.datetime.utcfromtimestamp(
                            submission.created_utc
                        ).isoformat() + "Z"
                        for media_url, media_type in pairs:
                            item = MediaItem(
                                post_url=f"https://reddit.com{submission.permalink}",
                                media_url=media_url,
                                source=self.source,
                                timestamp=ts,
                                media_type=media_type,
                                keyword_matched=matched_kw,
                            )
                            await self._enqueue(item)
                        # Small pause between submissions to avoid hammering
                        await asyncio.sleep(0.2)
                except Exception as exc:
                    self._log.error(
                        "API error for r/%s keyword=%r: %s", subreddit, keyword, exc
                    )

    async def _crawl_via_playwright(self) -> None:
        for keyword in self.keywords:
            self._log.info("Playwright: searching Reddit for %r", keyword)
            try:
                posts = await self._playwright_client.search_submissions(
                    keyword, self._limit
                )
                for post in posts:
                    item = MediaItem(
                        post_url=post["post_url"],
                        media_url=post["media_url"],
                        source=self.source,
                        timestamp=post["timestamp"],
                        media_type=post["media_type"],
                        keyword_matched=keyword,
                    )
                    await self._enqueue(item)
            except Exception as exc:
                self._log.error(
                    "Playwright Reddit error for keyword=%r: %s", keyword, exc
                )

    async def close(self) -> None:
        if self._api_client:
            await self._api_client.close()


# ─────────────────────────────────────────────────────────────────────────────
# Twitter / X
# ─────────────────────────────────────────────────────────────────────────────

class TwitterAPIClient:
    """
    Thin async client for Twitter API v2 (Recent Search endpoint).
    Uses aiohttp directly to avoid sync library constraints.
    """

    SEARCH_URL = "https://api.twitter.com/2/tweets/search/recent"

    def __init__(self) -> None:
        self._bearer = TwitterConfig.BEARER_TOKEN
        self._log = get_logger("crawler.twitter.api")

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._bearer}"}

    async def search(
        self,
        keyword: str,
        limit: int,
        session: aiohttp.ClientSession,
    ) -> list[dict]:
        """
        Search recent tweets for a keyword that include media.
        Returns a list of dicts: tweet_id, text, created_at, media_url, media_type.
        """
        params = {
            "query": f"{keyword} has:media -is:retweet lang:en",
            "max_results": min(limit, 100),
            "tweet.fields": "created_at,attachments",
            "expansions": "attachments.media_keys",
            "media.fields": "url,preview_image_url,type,variants",
        }
        try:
            async with session.get(
                self.SEARCH_URL, headers=self._headers(), params=params, timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                if resp.status == 429:
                    self._log.warning("Twitter API rate limit hit. Will retry next cycle.")
                    return []
                if resp.status != 200:
                    body = await resp.text()
                    self._log.error("Twitter API error %d: %s", resp.status, body[:200])
                    return []
                data = await resp.json()
        except aiohttp.ClientError as exc:
            self._log.error("Twitter API request failed: %s", exc)
            return []

        return self._parse_response(data)

    def _parse_response(self, data: dict) -> list[dict]:
        results: list[dict] = []
        tweets = data.get("data", [])
        media_map: dict[str, dict] = {
            m["media_key"]: m
            for m in data.get("includes", {}).get("media", [])
        }

        for tweet in tweets:
            media_keys = (
                tweet.get("attachments", {}).get("media_keys", [])
            )
            for mk in media_keys:
                media = media_map.get(mk, {})
                mtype = media.get("type", "")
                url = None

                if mtype == "photo":
                    url = media.get("url")
                    kind = "image"
                elif mtype in ("video", "animated_gif"):
                    # Pick highest-bitrate MP4 variant
                    variants = media.get("variants", [])
                    mp4s = [
                        v for v in variants
                        if v.get("content_type") == "video/mp4"
                    ]
                    if mp4s:
                        url = max(mp4s, key=lambda v: v.get("bit_rate", 0))["url"]
                    kind = "video"
                else:
                    continue

                if url:
                    results.append({
                        "tweet_id": tweet["id"],
                        "post_url": f"https://twitter.com/i/web/status/{tweet['id']}",
                        "media_url": url,
                        "media_type": kind,
                        "timestamp": tweet.get("created_at", datetime.datetime.utcnow().isoformat() + "Z"),
                    })
        return results


# ─────────────────────────────────────────────────────────────────────────────
# Twitter / X fallback sources
# Each list is tried in order; the first source that yields ≥1 result wins.
# ─────────────────────────────────────────────────────────────────────────────

# Tier-1: Nitter public instances (open-source Twitter frontend, no login needed)
# List maintained at  https://github.com/zedeus/nitter/wiki/Instances
_NITTER_INSTANCES: list[str] = [
    # Generally reliable
    "https://nitter.privacydev.net",
    "https://nitter.poast.org",
    "https://nitter.1d4.us",
    "https://nitter.kavin.rocks",
    "https://nitter.moomoo.me",
    "https://nitter.esmailelbob.xyz",
    "https://nitter.namazso.eu",
    "https://nitter.it",
    "https://nitter.nl",
    "https://nitter.mint.lgbt",
    # Sometimes up
    "https://nitter.net",
    "https://lightbrd.com",
    "https://nitter.space",
    "https://notabird.site",
    "https://nitter.42l.fr",
    "https://nitter.sethforprivacy.com",
    "https://nitter.cutelab.space",
    "https://nitter.cz",
    "https://nitter.unixfox.eu",
    "https://nitter.eu",
    "https://nitter.hu",
]

# Tier-2: Mastodon public instances with searchable hashtag timelines
_MASTODON_INSTANCES: list[str] = [
    "https://mastodon.social",
    "https://mstdn.social",
    "https://fosstodon.org",
    "https://infosec.exchange",
    "https://hachyderm.io",
]

# Federation bridge patterns — URLs like these are web articles cross-posted
# into Mastodon via brid.gy or similar services; skip them entirely.
_BRIDGE_URL_RE = re.compile(
    r"brid\.gy"          # web.brid.gy / fed.brid.gy
    r"|/r/https?://"      # embedded redirect URLs (brid.gy style)
    r"|activitypub\.brid\.gy",
    re.IGNORECASE,
)


class TwitterPlaywrightClient:
    """
    Playwright-based scraper for Twitter/X-related media.

    Fallback chain (tried in order, stops as soon as any tier returns results):
      1. Nitter  — 20+ public Nitter instances (no login)
      2. Mastodon — 5 federated instances with public hashtag timelines
      3. Bing Images — standard Bing image search (no login)
      4. DuckDuckGo Images — DDG image search (no login, less bot-hostile)
      5. twitter.com direct — last resort; logs a clear message if login wall hit
    """

    _UA = (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )

    def __init__(self) -> None:
        self._log = get_logger("crawler.twitter.playwright")

    # ── Public entry point ─────────────────────────────────────────────────────

    async def search(self, keyword: str, limit: int) -> list[dict]:
        """
        Return up to `limit` image dicts for `keyword`.
        Tries each tier in sequence and returns as soon as one succeeds.
        """
        if not PLAYWRIGHT_AVAILABLE:
            self._log.error("Playwright is not installed. Run: playwright install chromium")
            return []

        tiers = [
            ("Nitter",       self._scrape_via_nitter),
            ("Mastodon",     self._scrape_via_mastodon),
            ("Bing Images",  self._scrape_via_bing),
            ("DDG Images",   self._scrape_via_ddg),
            ("Twitter direct", self._scrape_via_twitter_direct),
        ]

        for name, scraper in tiers:
            self._log.info("Twitter fallback tier: %s — keyword=%r", name, keyword)
            try:
                results = await scraper(keyword, limit)
                if results:
                    self._log.info(
                        "✅ Twitter tier '%s' returned %d result(s) for %r.",
                        name, len(results), keyword,
                    )
                    return results
                self._log.info("Tier '%s' returned 0 results — trying next.", name)
            except Exception as exc:
                self._log.warning("Tier '%s' raised: %s — trying next.", name, exc)

        self._log.warning(
            "All Twitter fallback tiers exhausted for %r. "
            "Set TWITTER_BEARER_TOKEN in .env for reliable API access.",
            keyword,
        )
        return []

    # ── Helpers ────────────────────────────────────────────────────────────────

    async def _new_browser_context(self, pw):
        """Launch a headless Chromium browser with a realistic user-agent."""
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=self._UA)
        return browser, context

    def _make_result(self, post_url: str, media_url: str) -> dict:
        return {
            "post_url": post_url,
            "media_url": media_url,
            "media_type": "image",
            "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        }

    # ── Tier 1: Nitter ────────────────────────────────────────────────────────

    async def _scrape_via_nitter(self, keyword: str, limit: int) -> list[dict]:
        """Try each Nitter instance; return results from the first that works."""
        encoded = keyword.replace(" ", "+")
        async with async_playwright() as pw:
            browser, context = await self._new_browser_context(pw)
            page: Page = await context.new_page()
            try:
                for base in _NITTER_INSTANCES:
                    search_url = f"{base}/search?f=tweets&q={encoded}&since_id=&max_position=&cursor="
                    try:
                        self._log.debug("Nitter: trying %s", base)
                        resp = await page.goto(
                            search_url, wait_until="domcontentloaded", timeout=12_000
                        )
                        if resp is None or resp.status >= 400:
                            continue

                        await page.wait_for_timeout(1500)
                        has_tweets = await page.evaluate(
                            "() => document.querySelectorAll('.timeline-item, .tweet-body').length > 0"
                        )
                        if not has_tweets:
                            continue

                        # Scroll for more content
                        for _ in range(2):
                            await page.keyboard.press("End")
                            await page.wait_for_timeout(1000)

                        img_data = await page.evaluate("""
                            () => {
                                const imgs = document.querySelectorAll(
                                    '.tweet-body img.media-image, '
                                    + '.attachments img[src], '
                                    + '.gallery-row img[src], '
                                    + '.still-image img[src]'
                                );
                                return [...imgs].map(el => el.src).filter(Boolean);
                            }
                        """)
                        tweet_links = await page.evaluate("""
                            () => {
                                const links = document.querySelectorAll(
                                    '.tweet-link[href*="/status/"], a[href*="/status/"]'
                                );
                                return [...new Set([...links].map(a => a.href))];
                            }
                        """)

                        self._log.info(
                            "Nitter %s: %d images, %d links for %r",
                            base, len(img_data), len(tweet_links), keyword,
                        )

                        results: list[dict] = []
                        seen: set[str] = set()
                        for i, src in enumerate(img_data[:limit]):
                            if src in seen:
                                continue
                            seen.add(src)
                            raw_link = tweet_links[i] if i < len(tweet_links) else search_url
                            post_url = re.sub(
                                r"https?://[^/]+(/[^/]+/status/\d+)",
                                r"https://twitter.com\1",
                                raw_link,
                            ).split("?")[0]
                            results.append(self._make_result(post_url, src))

                        if results:
                            return results

                    except Exception as exc:
                        self._log.debug("Nitter %s error: %s", base, exc)
                        continue
            finally:
                await context.close()
                await browser.close()

        return []

    # ── Tier 2: Mastodon ──────────────────────────────────────────────────────

    async def _scrape_via_mastodon(self, keyword: str, limit: int) -> list[dict]:
        """
        Search Mastodon public hashtag timelines.
        Uses the REST API (no auth needed for public timelines).
        """
        # Normalise keyword to a hashtag-safe string (alphanumeric only)
        tag = re.sub(r"[^a-zA-Z0-9]", "", keyword.split()[0])
        if not tag:
            return []

        results: list[dict] = []
        seen: set[str] = set()

        async with async_playwright() as pw:
            browser, context = await self._new_browser_context(pw)
            page: Page = await context.new_page()
            try:
                for base in _MASTODON_INSTANCES:
                    api_url = f"{base}/api/v1/timelines/tag/{tag}?limit=40"
                    try:
                        self._log.debug("Mastodon: trying %s/tags/%s", base, tag)
                        resp = await page.goto(
                            api_url, wait_until="domcontentloaded", timeout=12_000
                        )
                        if resp is None or resp.status != 200:
                            continue

                        raw_json = await page.evaluate("() => document.body.innerText")
                        posts = json.loads(raw_json)
                        if not isinstance(posts, list):
                            continue

                        for post in posts:
                            if len(results) >= limit:
                                break
                            post_url = post.get("url", base)

                            # Skip bridged web articles (brid.gy & similar)
                            if _BRIDGE_URL_RE.search(post_url):
                                self._log.debug(
                                    "Mastodon: skipping bridge/article URL: %s", post_url
                                )
                                continue

                            # Only posts that carry actual image attachments
                            for att in post.get("media_attachments", []):
                                if att.get("type") != "image":
                                    continue
                                media_url = att.get("url") or att.get("preview_url", "")
                                if not media_url or media_url in seen:
                                    continue
                                # Sanity-check: media URL must look like a direct image
                                if not re.search(r"\.(jpe?g|png|webp|gif)(\?|$)", media_url, re.IGNORECASE) \
                                        and not media_url.startswith("https://files."):
                                    self._log.debug(
                                        "Mastodon: media URL doesn't look like an image, skipping: %s",
                                        media_url,
                                    )
                                    continue
                                seen.add(media_url)
                                results.append(self._make_result(post_url, media_url))
                                if len(results) >= limit:
                                    break

                        self._log.info(
                            "Mastodon %s: %d results for #%s", base, len(results), tag
                        )
                        if results:
                            return results

                    except Exception as exc:
                        self._log.debug("Mastodon %s error: %s", base, exc)
                        continue
            finally:
                await context.close()
                await browser.close()

        return []

    # ── Tier 3: Bing Images ───────────────────────────────────────────────────

    async def _scrape_via_bing(self, keyword: str, limit: int) -> list[dict]:
        """Search Bing Images for the keyword; no login required."""
        encoded = keyword.replace(" ", "+")
        search_url = (
            f"https://www.bing.com/images/search?q={encoded}"
            f"&qft=+filterui:imagesize-large&form=IRFLTR"
        )
        results: list[dict] = []
        async with async_playwright() as pw:
            browser, context = await self._new_browser_context(pw)
            page: Page = await context.new_page()
            try:
                self._log.debug("Bing Images: searching for %r", keyword)
                await page.goto(search_url, wait_until="domcontentloaded", timeout=20_000)
                await page.wait_for_timeout(3000)

                # Scroll to load more results
                for _ in range(3):
                    await page.keyboard.press("End")
                    await page.wait_for_timeout(1000)

                # Bing stores the original URL in the `m` JSON attribute of .iusc wrappers
                img_data = await page.evaluate(f"""
                    () => {{
                        const results = [];
                        // Method 1: parse the 'm' attribute JSON on .iusc containers
                        document.querySelectorAll('.iusc').forEach(el => {{
                            try {{
                                const m = JSON.parse(el.getAttribute('m') || '{{}}');
                                if (m.murl) results.push({{ src: m.murl, page: m.purl || '' }});
                            }} catch(e) {{}}
                        }});
                        // Method 2: fallback — grab visible thumbnail src
                        if (results.length === 0) {{
                            document.querySelectorAll('img.mimg[src]').forEach(el => {{
                                results.push({{ src: el.src, page: '' }});
                            }});
                        }}
                        return results.slice(0, {limit});
                    }}
                """)

                seen: set[str] = set()
                for item in img_data:
                    src = item.get("src", "")
                    page_url = item.get("page", search_url) or search_url
                    if src and src not in seen and src.startswith("http"):
                        seen.add(src)
                        results.append(self._make_result(page_url, src))

                self._log.info("Bing Images: %d results for %r", len(results), keyword)
            except Exception as exc:
                self._log.warning("Bing Images scrape error: %s", exc)
            finally:
                await context.close()
                await browser.close()

        return results

    # ── Tier 4: DuckDuckGo Images ─────────────────────────────────────────────

    async def _scrape_via_ddg(self, keyword: str, limit: int) -> list[dict]:
        """Search DuckDuckGo Images; less bot-hostile than Google."""
        encoded = keyword.replace(" ", "+")
        # DDG image search SPA entry point
        search_url = f"https://duckduckgo.com/?q={encoded}&iax=images&ia=images"
        results: list[dict] = []
        async with async_playwright() as pw:
            browser, context = await self._new_browser_context(pw)
            page: Page = await context.new_page()
            try:
                self._log.debug("DDG Images: searching for %r", keyword)
                await page.goto(search_url, wait_until="domcontentloaded", timeout=20_000)
                await page.wait_for_timeout(4000)  # SPA needs extra time

                # Scroll to trigger lazy-load
                for _ in range(3):
                    await page.keyboard.press("End")
                    await page.wait_for_timeout(1200)

                img_data = await page.evaluate(f"""
                    () => {{
                        const results = [];
                        // DDG image tiles store the full URL in data-id or as JSON in script tags
                        document.querySelectorAll('[data-testid="result-image-img"], .tile--img img, img[data-src]').forEach(el => {{
                            const src = el.getAttribute('data-src') || el.src || '';
                            if (src && src.startsWith('http') && !src.includes('duckduckgo.com')) {{
                                results.push(src);
                            }}
                        }});
                        // Also scan vqd JSON blobs for image URLs
                        if (results.length === 0) {{
                            const scripts = [...document.querySelectorAll('script')];
                            for (const s of scripts) {{
                                const m = s.textContent.match(/"height":\\d+,"image":"(https?:[^"]+)"/g);
                                if (m) {{
                                    m.forEach(match => {{
                                        const url = match.match(/"image":"(https?:[^"]+)"/);
                                        if (url) results.push(url[1].replace(/\\\\/g, ''));
                                    }});
                                }}
                            }}
                        }}
                        return [...new Set(results)].slice(0, {limit});
                    }}
                """)

                seen: set[str] = set()
                for src in img_data:
                    if src and src not in seen:
                        seen.add(src)
                        results.append(self._make_result(search_url, src))

                self._log.info("DDG Images: %d results for %r", len(results), keyword)
            except Exception as exc:
                self._log.warning("DDG Images scrape error: %s", exc)
            finally:
                await context.close()
                await browser.close()

        return results

    # ── Tier 5: twitter.com direct (last resort) ──────────────────────────────

    async def _scrape_via_twitter_direct(self, keyword: str, limit: int) -> list[dict]:
        """Navigate x.com directly — will fail if the login wall is shown."""
        results: list[dict] = []
        search_url = (
            f"https://x.com/search?q={keyword.replace(' ', '%20')}"
            f"&src=typed_query&f=top"
        )
        async with async_playwright() as pw:
            browser, context = await self._new_browser_context(pw)
            page: Page = await context.new_page()
            try:
                self._log.info("Twitter direct: navigating for %r", keyword)
                await page.goto(search_url, wait_until="domcontentloaded", timeout=30_000)
                await page.wait_for_timeout(5000)

                current_url = page.url
                if "login" in current_url or "i/flow/login" in current_url:
                    self._log.warning(
                        "Twitter direct: login wall hit — all 5 tiers exhausted. "
                        "Set TWITTER_BEARER_TOKEN in .env for reliable API access."
                    )
                    return results

                for _ in range(3):
                    await page.keyboard.press("End")
                    await page.wait_for_timeout(2000)

                img_data = await page.evaluate("""() => {
                    // TARGET ONLY IMAGES INSIDE TWEETS
                    const imgs = document.querySelectorAll('article div[data-testid="tweetPhoto"] img');
                    return [...imgs].map(el => el.src).filter(src => src.includes('pbs.twimg.com/media'));
                }""")
                tweet_urls = await page.evaluate("""() => {
                    return [...new Set(
                        [...document.querySelectorAll('article a[href*="/status/"]')]
                        .map(a => a.href)
                    )];
                }""")

                seen: set[str] = set()
                for i, src in enumerate(img_data[:limit]):
                    src = re.sub(r"[?&]name=\w+", "", src)
                    src = src + ("&" if "?" in src else "?") + "name=large"
                    if src in seen:
                        continue
                    seen.add(src)
                    post_url = (tweet_urls[i] if i < len(tweet_urls) else search_url).split("?")[0]
                    results.append(self._make_result(post_url, src))

            except Exception as exc:
                self._log.error("Twitter direct scrape failed: %s", exc)
            finally:
                await context.close()
                await browser.close()
        return results


# ─────────────────────────────────────────────────────────────────────────────
# Top-mode Playwright clients
# ─────────────────────────────────────────────────────────────────────────────

class RedditTopPlaywrightClient:
    """
    Browse subreddit top-posts pages and extract post-content images.
    Used in 'top' mode — no keyword search, just browses a subreddit's
    top posts for a given time filter.
    """

    _UA = (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )

    def __init__(self) -> None:
        self._log = get_logger("crawler.reddit.top")

    async def scrape_subreddit(
        self,
        subreddit: str,
        limit: int,
        sort: str = "top",
        time_filter: str = "day",
    ) -> list[dict]:
        """
        Browse r/<subreddit>/<sort>?t=<time_filter> and return image posts.

        Returns list of dicts: post_url, media_url, media_type, timestamp, subreddit.
        """
        if not PLAYWRIGHT_AVAILABLE:
            self._log.error("Playwright not installed. Run: playwright install chromium")
            return []

        url = f"https://www.reddit.com/r/{subreddit}/{sort}/?t={time_filter}"
        results: list[dict] = []

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            context = await browser.new_context(user_agent=self._UA)
            page: Page = await context.new_page()
            try:
                self._log.info("Top-mode Reddit: r/%s/%s?t=%s", subreddit, sort, time_filter)
                resp = await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
                if resp is None or resp.status >= 400:
                    self._log.warning("r/%s returned HTTP %s — skipping.", subreddit,
                                      resp.status if resp else "no response")
                    return results

                await page.wait_for_timeout(3000)

                # Scroll to load more posts
                for _ in range(3):
                    await page.keyboard.press("End")
                    await page.wait_for_timeout(1500)

                # Collect post links from the listing page
                post_links = await page.evaluate("""
                    () => {
                        const anchors = document.querySelectorAll(
                            'shreddit-post a[slot="full-post-link"], '
                            + 'a[data-click-id="body"][href*="/comments/"]'
                        );
                        return [...new Set([...anchors].map(a => a.href)
                            .filter(h => h.includes('/comments/')))];
                    }
                """)

                self._log.info("r/%s: found %d post links.", subreddit, len(post_links))

                seen: set[str] = set()
                for post_url in post_links[:limit * 2]:   # over-fetch; many posts lack images
                    if len(results) >= limit:
                        break
                    try:
                        await page.goto(post_url, wait_until="domcontentloaded", timeout=20_000)
                        await page.wait_for_timeout(2000)

                        img_srcs = await page.evaluate("""
                            () => {
                                const ALLOWED = [
                                    'external-preview.redd.it',
                                    'preview.redd.it',
                                    'i.redd.it',
                                    'i.imgur.com',
                                ];
                                const BLOCKED = [
                                    'styles.redditmedia.com',
                                    'thumbs.redditmedia.com',
                                    'redditstatic.com',
                                ];
                                const srcs = new Set();
                                document.querySelectorAll('img[src]').forEach(el => {
                                    try {
                                        const host = new URL(el.src).hostname;
                                        if (ALLOWED.some(a => host.includes(a)) &&
                                            !BLOCKED.some(b => host.includes(b))) {
                                            srcs.add(el.src);
                                        }
                                    } catch(e) {}
                                });
                                return [...srcs];
                            }
                        """)

                        for src in img_srcs:
                            if src not in seen and len(results) < limit:
                                seen.add(src)
                                results.append({
                                    "post_url": post_url.split("?")[0],
                                    "media_url": src,
                                    "media_type": "image",
                                    "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
                                    "subreddit": subreddit,
                                })
                    except Exception as exc:
                        self._log.debug("Post %s failed: %s", post_url, exc)
                        continue

            except Exception as exc:
                self._log.error("r/%s top scrape failed: %s", subreddit, exc)
            finally:
                await context.close()
                await browser.close()

        self._log.info("r/%s: returning %d images.", subreddit, len(results))
        return results


class TwitterAccountPlaywrightClient:
    """
    Scrape the media timeline of specific public Twitter/X accounts.
    Used in 'top' mode — does not require keywords, browses account profiles.

    Falls back to Nitter profile pages when twitter.com redirects to login.
    """

    _UA = (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )

    def __init__(self) -> None:
        self._log = get_logger("crawler.twitter.accounts")

    async def scrape_account(self, account: str, limit: int) -> list[dict]:
        """
        Scrape images from a public account's media timeline.
        `account` should be a username without @.

        Returns list of dicts: post_url, media_url, media_type, timestamp, account.
        """
        if not PLAYWRIGHT_AVAILABLE:
            self._log.error("Playwright not installed. Run: playwright install chromium")
            return []

        # Try Nitter first (no login required), fall back to twitter.com
        results = await self._via_nitter(account, limit)
        if not results:
            self._log.info("Nitter failed for @%s — trying twitter.com directly.", account)
            results = await self._via_twitter(account, limit)
        return results

    async def _via_nitter(self, account: str, limit: int) -> list[dict]:
        """Try each Nitter instance's /account/media page."""
        results: list[dict] = []
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            context = await browser.new_context(user_agent=self._UA)
            page: Page = await context.new_page()
            try:
                for base in _NITTER_INSTANCES:
                    profile_url = f"{base}/{account}/media"
                    try:
                        self._log.debug("Nitter account: %s/@%s/media", base, account)
                        resp = await page.goto(
                            profile_url, wait_until="domcontentloaded", timeout=12_000
                        )
                        if resp is None or resp.status >= 400:
                            continue

                        await page.wait_for_timeout(1500)
                        has_content = await page.evaluate(
                            "() => document.querySelectorAll('.timeline-item, .tweet-body').length > 0"
                        )
                        if not has_content:
                            continue

                        for _ in range(2):
                            await page.keyboard.press("End")
                            await page.wait_for_timeout(1000)

                        img_srcs = await page.evaluate("""
                            () => {
                                const imgs = document.querySelectorAll(
                                    '.tweet-body img.media-image, '
                                    + '.attachments img[src], '
                                    + '.gallery-row img[src]'
                                );
                                return [...imgs].map(el => el.src).filter(Boolean);
                            }
                        """)
                        tweet_links = await page.evaluate("""
                            () => {
                                const links = document.querySelectorAll(
                                    '.tweet-link[href*="/status/"], a[href*="/status/"]'
                                );
                                return [...new Set([...links].map(a => a.href))];
                            }
                        """)

                        seen: set[str] = set()
                        for i, src in enumerate(img_srcs[:limit]):
                            if src in seen:
                                continue
                            seen.add(src)
                            raw_link = tweet_links[i] if i < len(tweet_links) else profile_url
                            post_url = re.sub(
                                r"https?://[^/]+(/[^/]+/status/\d+)",
                                r"https://twitter.com\1",
                                raw_link,
                            ).split("?")[0]
                            results.append({
                                "post_url": post_url,
                                "media_url": src,
                                "media_type": "image",
                                "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
                                "account": account,
                            })

                        if results:
                            self._log.info(
                                "Nitter %s: %d images for @%s", base, len(results), account
                            )
                            return results

                    except Exception as exc:
                        self._log.debug("Nitter %s @%s error: %s", base, account, exc)
                        continue
            finally:
                await context.close()
                await browser.close()
        return results

    async def _via_twitter(self, account: str, limit: int) -> list[dict]:
        """Browse twitter.com/<account>/media — last resort."""
        results: list[dict] = []
        profile_url = f"https://x.com/{account}/media"
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            context = await browser.new_context(user_agent=self._UA)
            page: Page = await context.new_page()
            try:
                self._log.info("Twitter direct: @%s/media", account)
                await page.goto(profile_url, wait_until="domcontentloaded", timeout=30_000)
                await page.wait_for_timeout(5000)

                if "login" in page.url or "i/flow/login" in page.url:
                    self._log.warning(
                        "@%s: login wall hit on twitter.com/media — skipping account.", account
                    )
                    return results

                for _ in range(3):
                    await page.keyboard.press("End")
                    await page.wait_for_timeout(2000)

                img_data = await page.evaluate("""() => {
                    return [...document.querySelectorAll('article img[src*="pbs.twimg.com/media"]')]
                           .map(el => el.src);
                }""")
                tweet_urls = await page.evaluate("""() => {
                    return [...new Set(
                        [...document.querySelectorAll('article a[href*="/status/"]')]
                        .map(a => a.href)
                    )];
                }""")

                seen: set[str] = set()
                for i, src in enumerate(img_data[:limit]):
                    src = re.sub(r"[?&]name=\w+", "", src)
                    src = src + ("&" if "?" in src else "?") + "name=large"
                    if src in seen:
                        continue
                    seen.add(src)
                    post_url = (tweet_urls[i] if i < len(tweet_urls) else profile_url).split("?")[0]
                    results.append({
                        "post_url": post_url,
                        "media_url": src,
                        "media_type": "image",
                        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
                        "account": account,
                    })
            except Exception as exc:
                self._log.error("Twitter direct @%s error: %s", account, exc)
            finally:
                await context.close()
                await browser.close()
        return results


class TwitterCrawler(BaseCrawler):
    """
    Twitter/X crawler.
    Primary:  Twitter API v2 (bearer token)
    Fallback: Playwright scraper
    """

    source = "twitter"

    def __init__(
        self,
        fetch_queue: asyncio.Queue,
        keywords: Optional[list[str]] = None,
        crawl_limit: Optional[int] = None,
    ) -> None:
        super().__init__(fetch_queue, keywords=keywords, crawl_limit=crawl_limit)
        # Effective limit: caller override >> env var
        self._limit: int = crawl_limit if crawl_limit is not None else TwitterConfig.SEARCH_LIMIT
        self._api_client: Optional[TwitterAPIClient] = None
        self._playwright_client = TwitterPlaywrightClient()
        self._session: Optional[aiohttp.ClientSession] = None

        if TwitterConfig.api_enabled():
            self._log.info("Twitter: using API v2 client (bearer token).")
            self._api_client = TwitterAPIClient()
        else:
            self._log.warning(
                "Twitter: bearer token not set — falling back to Playwright scraper."
            )

    def _crawl_interval(self) -> float:
        return TwitterConfig.CRAWL_INTERVAL_SECONDS

    async def crawl_once(self) -> None:
        if self._api_client:
            await self._crawl_via_api()
        else:
            await self._crawl_via_playwright()

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def _crawl_via_api(self) -> None:
        assert self._api_client is not None
        session = await self._get_session()
        for keyword in self.keywords:
            self._log.info(
                "API: searching Twitter for %r (limit=%d)",
                keyword, self._limit,
            )
            try:
                posts = await self._api_client.search(
                    keyword, self._limit, session
                )
                for post in posts:
                    item = MediaItem(
                        post_url=post["post_url"],
                        media_url=post["media_url"],
                        source=self.source,
                        timestamp=post["timestamp"],
                        media_type=post["media_type"],
                        keyword_matched=keyword,
                    )
                    await self._enqueue(item)
                await asyncio.sleep(0.5)  # small courtesy pause between keywords
            except Exception as exc:
                self._log.error(
                    "API error for keyword=%r: %s", keyword, exc, exc_info=True
                )

    async def _crawl_via_playwright(self) -> None:
        for keyword in self.keywords:
            self._log.info("Playwright: searching Twitter for %r", keyword)
            try:
                posts = await self._playwright_client.search(
                    keyword, self._limit
                )
                for post in posts:
                    item = MediaItem(
                        post_url=post["post_url"],
                        media_url=post["media_url"],
                        source=self.source,
                        timestamp=post["timestamp"],
                        media_type=post["media_type"],
                        keyword_matched=keyword,
                    )
                    await self._enqueue(item)
            except Exception as exc:
                self._log.error(
                    "Playwright Twitter error for keyword=%r: %s", keyword, exc
                )

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()


# ─────────────────────────────────────────────────────────────────────────────
# Connectivity self-test
# ─────────────────────────────────────────────────────────────────────────────

async def test_reddit_connectivity() -> dict[str, bool | str]:
    """
    Test Reddit API and Playwright connectivity.
    Returns a dict with 'api' and 'playwright' keys indicating pass/fail.
    """
    result: dict[str, bool | str] = {"api": False, "playwright": False}
    log = get_logger("crawler.test.reddit")

    # Test API
    if RedditConfig.api_enabled():
        try:
            client = RedditAPIClient()
            count = 0
            async for _ in client.search_submissions("pics", "photo", limit=1):
                count += 1
                break
            await client.close()
            result["api"] = True
            log.info("Reddit API: ✓ connected (fetched %d submission).", count)
        except Exception as exc:
            result["api"] = f"FAILED: {exc}"
            log.error("Reddit API: ✗ %s", exc)
    else:
        result["api"] = "SKIPPED (no credentials)"
        log.warning("Reddit API: SKIPPED — no credentials in .env")

    # Test Playwright
    if PLAYWRIGHT_AVAILABLE:
        try:
            pw_client = RedditPlaywrightClient()
            posts = await pw_client.search_submissions("photo", limit=1)
            result["playwright"] = True if posts else "connected but no results"
            log.info("Reddit Playwright: ✓ connected (%d result(s)).", len(posts))
        except Exception as exc:
            result["playwright"] = f"FAILED: {exc}"
            log.error("Reddit Playwright: ✗ %s", exc)
    else:
        result["playwright"] = "SKIPPED (playwright not installed)"
        log.warning("Reddit Playwright: SKIPPED — playwright not installed")

    return result


async def test_twitter_connectivity() -> dict[str, bool | str]:
    """
    Test Twitter API and Playwright connectivity.
    Returns a dict with 'api' and 'playwright' keys indicating pass/fail.
    """
    result: dict[str, bool | str] = {"api": False, "playwright": False}
    log = get_logger("crawler.test.twitter")

    # Test API
    if TwitterConfig.api_enabled():
        try:
            async with aiohttp.ClientSession() as session:
                client = TwitterAPIClient()
                posts = await client.search("photo", limit=1, session=session)
            result["api"] = True
            log.info("Twitter API: ✓ connected (%d result(s)).", len(posts))
        except Exception as exc:
            result["api"] = f"FAILED: {exc}"
            log.error("Twitter API: ✗ %s", exc)
    else:
        result["api"] = "SKIPPED (no bearer token)"
        log.warning("Twitter API: SKIPPED — no TWITTER_BEARER_TOKEN in .env")

    # Test Playwright
    if PLAYWRIGHT_AVAILABLE:
        try:
            pw_client = TwitterPlaywrightClient()
            posts = await pw_client.search("photo", limit=1)
            result["playwright"] = True if posts else "connected but no results"
            log.info("Twitter Playwright: ✓ connected (%d result(s)).", len(posts))
        except Exception as exc:
            result["playwright"] = f"FAILED: {exc}"
            log.error("Twitter Playwright: ✗ %s", exc)
    else:
        result["playwright"] = "SKIPPED (playwright not installed)"
        log.warning("Twitter Playwright: SKIPPED — playwright not installed")

    return result


async def run_connectivity_tests() -> None:
    """Run all crawler connectivity tests and print a summary."""
    log = get_logger("crawler.test")
    log.info("=" * 60)
    log.info("Running crawler connectivity tests…")
    log.info("=" * 60)

    reddit = await test_reddit_connectivity()
    twitter = await test_twitter_connectivity()

    log.info("-" * 60)
    log.info("RESULTS")
    log.info("  Reddit  API:        %s", reddit["api"])
    log.info("  Reddit  Playwright: %s", reddit["playwright"])
    log.info("  Twitter API:        %s", twitter["api"])
    log.info("  Twitter Playwright: %s", twitter["playwright"])
    log.info("=" * 60)
