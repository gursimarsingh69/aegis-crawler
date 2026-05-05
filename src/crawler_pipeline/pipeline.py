"""
pipeline.py
===========
Main orchestrator for the Digital Asset Protection Crawler Pipeline.

Responsibilities:
  1. Create and size all asyncio.Queue instances.
  2. Instantiate and start crawler, fetcher, preprocessor, and API-sender workers.
  3. Run the event loop continuously until interrupted (Ctrl+C / SIGTERM).
  4. Send processed MediaItem payloads to the Detection API.
  5. Log match results and similarity scores.
  6. Perform graceful shutdown on exit.
"""

import asyncio
import signal
import sys
import time
from typing import Optional

import aiohttp

from .config import ApiConfig, PipelineConfig
from .crawler import RedditCrawler, TwitterCrawler, run_connectivity_tests
from .fetcher import Fetcher, close_shared_connector
from .preprocessor import Preprocessor
from .stock_scraper import StockCrawler
from .utils import MediaItem, async_retry, get_logger, setup_logging

logger = get_logger("pipeline")

# ── Shutdown flag ─────────────────────────────────────────────────────────────
_shutdown_event = asyncio.Event()


def _install_signal_handlers(loop: asyncio.AbstractEventLoop) -> None:
    """Register SIGINT / SIGTERM to trigger graceful shutdown."""
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _shutdown_event.set)
        except (NotImplementedError, RuntimeError):
            # Windows does not support add_signal_handler for all signals
            pass


# ─────────────────────────────────────────────────────────────────────────────
# API Sender
# ─────────────────────────────────────────────────────────────────────────────

class APISender:
    """
    Worker that consumes fully-processed MediaItems and POSTs them to the
    Detection API, then logs the result.
    """

    def __init__(
        self,
        worker_id: int,
        api_queue: asyncio.Queue,
        session: aiohttp.ClientSession,
    ) -> None:
        self.worker_id = worker_id
        self.api_queue = api_queue
        self._session = session
        self._log = get_logger(f"api_sender.worker-{worker_id}")
        self._match_url = ApiConfig.match_url()
        self._extra_headers: dict[str, str] = {}
        if ApiConfig.API_KEY:
            self._extra_headers["X-API-Key"] = ApiConfig.API_KEY

    @async_retry(
        max_attempts=PipelineConfig.MAX_RETRIES,
        delay_seconds=2.0,
        backoff_factor=2.0,
        exceptions=(aiohttp.ClientError, asyncio.TimeoutError),
    )
    async def _post(self, item: MediaItem) -> dict:
        """POST the media file to the Backend API with retry."""
        timeout = aiohttp.ClientTimeout(total=PipelineConfig.REQUEST_TIMEOUT_SECONDS)

        # Sub-second timestamp ensures unique filenames during high-speed crawls
        unique_filename = f"scan_{int(time.time() * 1000)}.{item.file_extension}"

        # ROUTING LOGIC:
        # Stock sources (Unsplash, Pexels, Pixabay, Shutterstock, Getty)
        #   → POST /api/assets  (seed the asset vault, auto-name from filename)
        # Social sources (Reddit, Twitter, etc.)
        #   → POST /api/assets/scan/file  (detection: compare + write to Supabase)
        stock_sources = ["unsplash", "pexels", "pixabay", "shutterstock", "getty"]
        is_stock = item.source.lower() in stock_sources

        if is_stock:
            target_url = f"{ApiConfig.BASE_URL}/api/assets"
            self._log.info("[%s] SEED: Registering asset → %s", item.source, target_url)
            data = aiohttp.FormData()
            data.add_field(
                'file',
                item.raw_bytes if item.raw_bytes else b'',
                filename=unique_filename,
                content_type=item.content_type,
            )
            # name and type are optional — backend auto-fills from filename
        else:
            target_url = f"{ApiConfig.BASE_URL}/api/assets/scan/file"
            self._log.info("[%s] SCAN: Sending for detection → %s", item.source, target_url)
            data = aiohttp.FormData()
            data.add_field(
                'file',
                item.raw_bytes if item.raw_bytes else b'',
                filename=unique_filename,
                content_type=item.content_type,
            )
            data.add_field('url', item.media_url or '')
            data.add_field('source', item.source or 'crawler')

        async with self._session.post(
            target_url,
            data=data,
            headers=self._extra_headers,
            timeout=timeout,
        ) as resp:
            resp.raise_for_status()
            return await resp.json()

    def _log_result(self, item: MediaItem) -> None:
        """Pretty-print the detection result using AI reasoning."""
        if item.matched is True:
            self._log.warning(
                "🚨 MATCH FOUND! [%s] %s  Similarity=%d%%  Asset=%s  Reason: %s",
                item.source,
                item.media_url,
                item.similarity_score or 0,
                item.api_response.get("matched_asset") if item.api_response else "Unknown",
                item.api_response.get("reason") if item.api_response else "N/A"
            )
        elif item.matched is False:
            self._log.info(
                "✅ CLEAN  [%s] %s  Similarity=%d%%  Reason: %s",
                item.source,
                item.media_url,
                item.similarity_score or 0,
                item.api_response.get("reason") if item.api_response else "No match found"
            )

    async def send_item(self, item: MediaItem) -> None:
        """Send a single MediaItem to the API and log the response."""
        self._log.info(
            "[%s] Sending %s to Akasha Engine for AI Analysis...",
            item.source, item.media_type
        )
        try:
            response = await self._post(item)
            item.api_response = response
            # Parse standardised response fields from Akasha Engine
            item.matched = response.get("match", False)
            item.similarity_score = response.get("confidence", 0)
            self._log_result(item)
        except Exception as exc:
            self._log.error(
                "API send failed for %s after retries: %s",
                item.media_url, exc,
            )

    async def run(self) -> None:
        """Continuously consume api_queue and send items."""
        self._log.info("API sender worker %d started.", self.worker_id)
        while True:
            try:
                item: MediaItem = await self.api_queue.get()
                await self.send_item(item)
            except asyncio.CancelledError:
                self._log.info("API sender worker %d shutting down.", self.worker_id)
                break
            except Exception as exc:
                self._log.error(
                    "Unexpected error in API sender: %s", exc, exc_info=True
                )
            finally:
                try:
                    self.api_queue.task_done()
                except ValueError:
                    pass


# ─────────────────────────────────────────────────────────────────────────────
# Queue stats printer
# ─────────────────────────────────────────────────────────────────────────────

async def _stats_printer(
    fetch_q: asyncio.Queue,
    preprocess_q: asyncio.Queue,
    api_q: asyncio.Queue,
    interval: float = 60.0,
) -> None:
    """Periodically log queue depths so operators can monitor back-pressure."""
    while True:
        try:
            await asyncio.sleep(interval)
            logger.info(
                "Queue depths — fetch=%d  preprocess=%d  api=%d",
                fetch_q.qsize(), preprocess_q.qsize(), api_q.qsize(),
            )
        except asyncio.CancelledError:
            break


# ─────────────────────────────────────────────────────────────────────────────
# Main pipeline
# ─────────────────────────────────────────────────────────────────────────────

async def run_pipeline(
    keywords: Optional[list[str]] = None,
    crawl_limit: Optional[int] = None,
    source: str = "social",
) -> None:
    """
    Build and run the full async pipeline until a shutdown signal is received.

    Parameters
    ----------
    keywords : list[str] | None
        Topics/keywords to search for. Overrides KEYWORDS from .env when supplied.
    crawl_limit : int | None
        Max results per source per keyword per crawl pass.
        Overrides REDDIT_CRAWL_LIMIT / TWITTER_SEARCH_LIMIT from .env when supplied.
    source : str
        Which crawler source group to use: "social" (Reddit/Twitter) or "stock" (Unsplash/Pexels/Pixabay).
    """
    effective_keywords = keywords or PipelineConfig.KEYWORDS
    logger.info("=" * 64)
    logger.info("Digital Asset Protection — Crawler Pipeline starting…")
    logger.info("Detection API : %s", ApiConfig.match_url())
    logger.info("Keywords      : %s", ", ".join(effective_keywords))
    if crawl_limit is not None:
        logger.info("Crawl limit   : %d per source per keyword", crawl_limit)
    logger.info("Source        : %s", source)
    logger.info("=" * 64)

    # ── Queues ────────────────────────────────────────────────────────────────
    fetch_queue = asyncio.Queue(maxsize=PipelineConfig.FETCH_QUEUE_SIZE)
    preprocess_queue = asyncio.Queue(maxsize=PipelineConfig.PREPROCESS_QUEUE_SIZE)
    api_queue = asyncio.Queue(maxsize=PipelineConfig.API_QUEUE_SIZE)

    # ── Shared aiohttp session for API sender ─────────────────────────────────
    api_session = aiohttp.ClientSession()

    # ── Crawlers ──────────────────────────────────────────────────────────────
    crawlers = []
    if source == "stock":
        crawlers.append(StockCrawler(fetch_queue, keywords=effective_keywords, crawl_limit=crawl_limit))
    else:
        # Default: social
        crawlers.append(RedditCrawler(fetch_queue, keywords=effective_keywords, crawl_limit=crawl_limit))
        crawlers.append(TwitterCrawler(fetch_queue, keywords=effective_keywords, crawl_limit=crawl_limit))

    # ── Workers ───────────────────────────────────────────────────────────────
    fetchers = [
        Fetcher(i, fetch_queue, preprocess_queue)
        for i in range(PipelineConfig.NUM_FETCHER_WORKERS)
    ]
    preprocessors = [
        Preprocessor(i, preprocess_queue, api_queue)
        for i in range(PipelineConfig.NUM_PREPROCESSOR_WORKERS)
    ]
    senders = [
        APISender(i, api_queue, api_session)
        for i in range(PipelineConfig.NUM_API_SENDER_WORKERS)
    ]

    # ── Assemble task list ────────────────────────────────────────────────────
    tasks = [
        # Crawlers (each runs their own continuous loop)
        *[asyncio.create_task(c.run(), name=f"crawler-{c.source}") for c in crawlers],
        # Fetcher workers
        *[asyncio.create_task(f.run(), name=f"fetcher-{f.worker_id}") for f in fetchers],
        # Preprocessor workers
        *[asyncio.create_task(p.run(), name=f"preprocessor-{p.worker_id}") for p in preprocessors],
        # API sender workers
        *[asyncio.create_task(s.run(), name=f"api-sender-{s.worker_id}") for s in senders],
        # Stats printer
        asyncio.create_task(_stats_printer(fetch_queue, preprocess_queue, api_queue), name="stats"),
    ]

    logger.info(
        "Started %d tasks (%d crawlers, %d fetchers, %d preprocessors, %d senders, 1 stats).",
        len(tasks), len(crawlers),
        PipelineConfig.NUM_FETCHER_WORKERS,
        PipelineConfig.NUM_PREPROCESSOR_WORKERS,
        PipelineConfig.NUM_API_SENDER_WORKERS,
    )

    # ── Wait for shutdown signal ──────────────────────────────────────────────
    try:
        await _shutdown_event.wait()
    except asyncio.CancelledError:
        pass

    logger.info("Shutdown signal received — stopping workers…")

    # ── Graceful shutdown ─────────────────────────────────────────────────────
    for task in tasks:
        task.cancel()

    results = await asyncio.gather(*tasks, return_exceptions=True)
    for i, result in enumerate(results):
        if isinstance(result, Exception) and not isinstance(result, asyncio.CancelledError):
            logger.error("Task %d raised on shutdown: %s", i, result)

    # Close resources
    for c in crawlers:
        await c.close()
    for f in fetchers:
        await f.close()
    await close_shared_connector()
    await api_session.close()

    logger.info("Pipeline shut down cleanly. Goodbye.")
