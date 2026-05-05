"""
stock_scraper.py
================
Official API-based scraper for the Digital Asset Protection Crawler Pipeline.

Supports three major stock sites with official, high-speed JSON APIs:
  • Unsplash  — https://unsplash.com/developers
  • Pexels    — https://www.pexels.com/api/
  • Pixabay   — https://pixabay.com/api/docs/
"""

from __future__ import annotations

import asyncio
import datetime
import json
from pathlib import Path
from typing import Optional

import aiohttp

from .config import PipelineConfig, StockConfig
from .crawler import BaseCrawler
from .utils import get_logger, async_retry, MediaItem

logger = get_logger("stock_scraper")

class UnsplashClient:
    def __init__(self, access_key: str) -> None:
        self._key = access_key
        self._endpoint = "https://api.unsplash.com/search/photos"

    async def search(self, keyword: str, limit: int, session: aiohttp.ClientSession) -> list[dict]:
        if not self._key:
            logger.warning("Unsplash: UNSPLASH_ACCESS_KEY missing. Get one at: https://unsplash.com/developers")
            return []

        params = {"query": keyword, "per_page": min(limit, 30), "client_id": self._key}
        try:
            async with session.get(self._endpoint, params=params, timeout=15) as resp:
                resp.raise_for_status()
                data = await resp.json()
                results = []
                for photo in data.get("results", []):
                    url = photo.get("urls", {}).get("regular")
                    if url:
                        results.append({
                            "media_url": url,
                            "post_url": photo.get("links", {}).get("html"),
                            "site": "unsplash",
                            "keyword": keyword,
                            "timestamp": datetime.datetime.utcnow().isoformat() + "Z"
                        })
                return results
        except Exception as e:
            logger.error("Unsplash error: %s", e)
            return []

class PexelsClient:
    def __init__(self, api_key: str) -> None:
        self._key = api_key
        self._endpoint = "https://api.pexels.com/v1/search"

    async def search(self, keyword: str, limit: int, session: aiohttp.ClientSession) -> list[dict]:
        if not self._key:
            logger.warning("Pexels: PEXELS_API_KEY missing. Get one at: https://www.pexels.com/api/")
            return []

        headers = {"Authorization": self._key}
        params = {"query": keyword, "per_page": min(limit, 80)}
        try:
            async with session.get(self._endpoint, headers=headers, params=params, timeout=15) as resp:
                resp.raise_for_status()
                data = await resp.json()
                results = []
                for photo in data.get("photos", []):
                    url = photo.get("src", {}).get("large2x")
                    if url:
                        results.append({
                            "media_url": url,
                            "post_url": photo.get("url"),
                            "site": "pexels",
                            "keyword": keyword,
                            "timestamp": datetime.datetime.utcnow().isoformat() + "Z"
                        })
                return results
        except Exception as e:
            logger.error("Pexels error: %s", e)
            return []

class PixabayClient:
    def __init__(self, api_key: str) -> None:
        self._key = api_key
        self._endpoint = "https://pixabay.com/api/"

    async def search(self, keyword: str, limit: int, session: aiohttp.ClientSession) -> list[dict]:
        if not self._key:
            logger.warning("Pixabay: PIXABAY_API_KEY missing. Get one at: https://pixabay.com/api/docs/")
            return []

        params = {"key": self._key, "q": keyword, "per_page": min(max(limit, 3), 200), "image_type": "photo"}
        try:
            async with session.get(self._endpoint, params=params, timeout=15) as resp:
                resp.raise_for_status()
                data = await resp.json()
                results = []
                for hit in data.get("hits", []):
                    url = hit.get("largeImageURL")
                    if url:
                        results.append({
                            "media_url": url,
                            "post_url": hit.get("pageURL"),
                            "site": "pixabay",
                            "keyword": keyword,
                            "timestamp": datetime.datetime.utcnow().isoformat() + "Z"
                        })
                return results
        except Exception as e:
            logger.error("Pixabay error: %s", e)
            return []

class StockCrawler(BaseCrawler):
    def __init__(self, fetch_queue: asyncio.Queue, keywords: Optional[list[str]] = None, crawl_limit: Optional[int] = None) -> None:
        super().__init__(fetch_queue, keywords=keywords, crawl_limit=crawl_limit)
        self._limit = crawl_limit or StockConfig.STOCK_LIMIT
        self._clients = [
            UnsplashClient(StockConfig.UNSPLASH_ACCESS_KEY),
            PexelsClient(StockConfig.PEXELS_API_KEY),
            PixabayClient(StockConfig.PIXABAY_API_KEY)
        ]
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def crawl_once(self) -> None:
        session = await self._get_session()
        for keyword in self.keywords:
            self._log.info("Stock API: Searching for %r...", keyword)
            tasks = [client.search(keyword, self._limit, session) for client in self._clients]
            results = await asyncio.gather(*tasks)
            
            for site_results in results:
                for res in site_results:
                    item = MediaItem(
                        post_url=res["post_url"],
                        media_url=res["media_url"],
                        source=res["site"],
                        timestamp=res["timestamp"],
                        media_type="image",
                        keyword_matched=res["keyword"],
                    )
                    self._log.info("[%s] STEP 1: Found & Queuing %s", item.source, item.media_url)
                    await self._enqueue(item)

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

class StockRunner:
    """Standalone runner for API-based stock scraping."""
    def __init__(self, output_dir: Optional[Path] = None, keywords: Optional[list[str]] = None, limit: Optional[int] = None) -> None:
        self._output_dir = output_dir or Path(StockConfig.ASSETS_DIR)
        self._keywords = keywords or PipelineConfig.KEYWORDS
        self._limit = limit or StockConfig.STOCK_LIMIT
        self._clients = [
            UnsplashClient(StockConfig.UNSPLASH_ACCESS_KEY),
            PexelsClient(StockConfig.PEXELS_API_KEY),
            PixabayClient(StockConfig.PIXABAY_API_KEY)
        ]
        self._output_dir.mkdir(parents=True, exist_ok=True)

    async def run(self) -> None:
        logger.info("Stock API Scrape started for keywords: %s", self._keywords)
        async with aiohttp.ClientSession() as session:
            for keyword in self._keywords:
                for client in self._clients:
                    items = await client.search(keyword, self._limit, session)
                    for item in items:
                        # In standalone mode, we just log found URLs
                        logger.info("Found [%s]: %s", item['site'], item['media_url'])
        logger.info("Stock API Scrape complete.")
