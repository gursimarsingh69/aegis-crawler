"""
tests/test_utils.py
===================
Basic unit tests for utility functions and data model.
"""

import asyncio
import pytest

from src.crawler_pipeline.utils import (
    SeenCache,
    MediaItem,
    bytes_to_b64,
    b64_to_bytes,
    keyword_matches,
    is_supported_content_type,
    mime_to_extension,
)


# ── SeenCache ─────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_seen_cache_check_and_mark():
    cache = SeenCache()
    url = "https://example.com/image.jpg"

    # First time: should NOT be seen
    assert await cache.check_and_mark(url) is False
    # Second time: should BE seen
    assert await cache.check_and_mark(url) is True


@pytest.mark.asyncio
async def test_seen_cache_ttl_eviction():
    cache = SeenCache(ttl_seconds=0)  # expires instantly
    url = "https://example.com/video.mp4"
    await cache.mark_seen(url)
    # Even though marked, TTL=0 means it's evicted on next check
    assert await cache.is_seen(url) is False


# ── Base64 helpers ────────────────────────────────────────────────────────────

def test_bytes_to_b64_roundtrip():
    data = b"hello, world!"
    assert b64_to_bytes(bytes_to_b64(data)) == data


# ── keyword_matches ───────────────────────────────────────────────────────────

def test_keyword_matches_found():
    assert keyword_matches("This is a deepfake video", ["deepfake", "DMCA"]) == "deepfake"


def test_keyword_matches_case_insensitive():
    assert keyword_matches("Check DMCA takedown notice", ["dmca"]) == "dmca"


def test_keyword_matches_not_found():
    assert keyword_matches("Cute cat picture", ["deepfake", "DMCA"]) == ""


# ── Content type helpers ──────────────────────────────────────────────────────

def test_is_supported_content_type_image():
    assert is_supported_content_type("image/jpeg") is True
    assert is_supported_content_type("image/jpeg; charset=utf-8") is True


def test_is_supported_content_type_video():
    assert is_supported_content_type("video/mp4") is True


def test_is_supported_content_type_unsupported():
    assert is_supported_content_type("text/html") is False
    assert is_supported_content_type("application/json") is False


def test_mime_to_extension():
    assert mime_to_extension("image/jpeg") == "jpg"
    assert mime_to_extension("video/mp4") == "mp4"
    assert mime_to_extension("application/octet-stream") == "bin"


# ── MediaItem ─────────────────────────────────────────────────────────────────

def test_media_item_to_api_payload():
    item = MediaItem(
        post_url="https://reddit.com/r/test/comments/abc/post",
        media_url="https://i.redd.it/example.jpg",
        source="reddit",
        timestamp="2024-01-01T00:00:00Z",
        media_type="image",
        keyword_matched="deepfake",
        content_type="image/jpeg",
        processed_b64_frames=["base64data"],
    )
    payload = item.to_api_payload()
    assert payload["url"] == item.media_url
    assert payload["source"] == "reddit"
    assert payload["media_type"] == "image"
    assert payload["processed_data"] == ["base64data"]
    assert payload["metadata"]["keyword_matched"] == "deepfake"
