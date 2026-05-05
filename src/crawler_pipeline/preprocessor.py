"""
preprocessor.py
===============
Media preprocessing for the Digital Asset Protection Pipeline.

Consumes MediaItem objects from the preprocess_queue, processes the raw bytes
using OpenCV, and writes the base64-encoded processed frames into
item.processed_b64_frames before placing the item into the api_queue.

Image processing:
  - Decode bytes → OpenCV BGR array
  - Resize to TARGET_SIZE (default 256×256)
  - Convert BGR → RGB
  - Normalize pixel values to [0, 1] as float32
  - Re-encode as JPEG bytes (compact transport format)
  - Base64-encode

Video processing:
  - Write raw bytes to a NamedTemporaryFile
  - Open with cv2.VideoCapture
  - Sample frames at TARGET_FPS (default 1 FPS) or up to MAX_FRAMES
  - Apply the same image pipeline to each frame
  - Clean up temp file

All processing runs in an asyncio.Executor (thread pool) so it doesn't
block the event loop.
"""

import asyncio
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import cv2
import numpy as np

from .config import PipelineConfig
from .utils import MediaItem, bytes_to_b64, get_logger

logger = get_logger("preprocessor")

# ── Constants ─────────────────────────────────────────────────────────────────
TARGET_SIZE: tuple[int, int] = (256, 256)   # (width, height)
TARGET_FPS: float = 1.0                     # frames per second to sample from video
MAX_FRAMES: int = 30                        # cap total frames per video clip
JPEG_QUALITY: int = 85                      # JPEG encode quality for transport

# Shared thread pool for blocking OpenCV calls
_executor = ThreadPoolExecutor(
    max_workers=PipelineConfig.NUM_PREPROCESSOR_WORKERS * 2,
    thread_name_prefix="pp-worker",
)


# ─────────────────────────────────────────────────────────────────────────────
# Low-level OpenCV helpers  (run in thread pool)
# ─────────────────────────────────────────────────────────────────────────────

def _process_image_bytes(raw: bytes) -> Optional[str]:
    """
    Decode, resize, normalise, re-encode, and Base64-encode image bytes.
    Returns a Base64 string, or None if decoding fails.
    """
    arr = np.frombuffer(raw, dtype=np.uint8)
    img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
    if img is None:
        logger.warning("cv2.imdecode returned None — skipping frame.")
        return None

    # Resize
    img = cv2.resize(img, TARGET_SIZE, interpolation=cv2.INTER_AREA)

    # BGR → RGB
    img = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)

    # Normalise to float32 [0, 1], then scale back to uint8 for JPEG encode
    # (Keeps compatibility with detection models that expect normalised input
    #  while still allowing lossless JPEG transport.)
    img_norm = (img.astype(np.float32) / 255.0)
    img_uint8 = (img_norm * 255).astype(np.uint8)

    # Re-encode as JPEG for compact transport
    success, encoded = cv2.imencode(
        ".jpg", cv2.cvtColor(img_uint8, cv2.COLOR_RGB2BGR),
        [int(cv2.IMWRITE_JPEG_QUALITY), JPEG_QUALITY],
    )
    if not success:
        logger.warning("cv2.imencode failed — skipping frame.")
        return None

    return bytes_to_b64(encoded.tobytes())


def _process_video_bytes(raw: bytes, extension: str) -> list[str]:
    """
    Extract frames from video bytes and process each one.
    Returns a list of Base64-encoded processed frame strings.
    """
    frames_b64: list[str] = []

    # Write to a temp file because cv2.VideoCapture can't read from memory
    suffix = f".{extension}" if extension else ".mp4"
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp.write(raw)
        tmp_path = tmp.name

    try:
        cap = cv2.VideoCapture(tmp_path)
        if not cap.isOpened():
            logger.error("cv2.VideoCapture failed to open temp file: %s", tmp_path)
            return []

        source_fps: float = cap.get(cv2.CAP_PROP_FPS) or 24.0
        frame_interval = max(1, int(source_fps / TARGET_FPS))

        frame_idx = 0
        processed = 0

        while processed < MAX_FRAMES:
            ret, frame = cap.read()
            if not ret:
                break
            if frame_idx % frame_interval == 0:
                # Encode BGR frame to JPEG bytes, then reuse image pipeline
                success, buf = cv2.imencode(".jpg", frame)
                if success:
                    b64 = _process_image_bytes(buf.tobytes())
                    if b64:
                        frames_b64.append(b64)
                        processed += 1
            frame_idx += 1

        cap.release()
        logger.debug(
            "Extracted %d frame(s) from video (source_fps=%.1f, interval=%d).",
            len(frames_b64), source_fps, frame_interval,
        )
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass

    return frames_b64


# ─────────────────────────────────────────────────────────────────────────────
# Async wrappers
# ─────────────────────────────────────────────────────────────────────────────

async def preprocess_image(raw: bytes) -> list[str]:
    """Async wrapper: process image bytes in thread pool."""
    loop = asyncio.get_event_loop()
    b64 = await loop.run_in_executor(_executor, _process_image_bytes, raw)
    return [b64] if b64 else []


async def preprocess_video(raw: bytes, extension: str) -> list[str]:
    """Async wrapper: process video bytes in thread pool."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        _executor, _process_video_bytes, raw, extension
    )


# ─────────────────────────────────────────────────────────────────────────────
# Preprocessor worker
# ─────────────────────────────────────────────────────────────────────────────

class Preprocessor:
    """
    Single preprocessor worker.
    Reads from preprocess_queue, runs OpenCV pipeline, writes to api_queue.
    """

    def __init__(
        self,
        worker_id: int,
        preprocess_queue: asyncio.Queue,
        api_queue: asyncio.Queue,
    ) -> None:
        self.worker_id = worker_id
        self.preprocess_queue = preprocess_queue
        self.api_queue = api_queue
        self._log = get_logger(f"preprocessor.worker-{worker_id}")

    async def process_item(self, item: MediaItem) -> None:
        """Run the appropriate preprocessing pipeline for the item."""
        if not item.raw_bytes:
            self._log.warning("Item has no raw bytes — skipping: %s", item.media_url)
            return

        self._log.info(
            "[%s] Preprocessing %s (%s)  size=%.1f KB",
            item.source, item.media_type, item.file_extension,
            len(item.raw_bytes) / 1024,
        )

        try:
            if item.media_type == "image":
                frames = await preprocess_image(item.raw_bytes)
            elif item.media_type == "video":
                frames = await preprocess_video(item.raw_bytes, item.file_extension)
            else:
                self._log.warning("Unknown media_type '%s' — skipping.", item.media_type)
                return

            if not frames:
                self._log.warning(
                    "No processable frames extracted from %s", item.media_url
                )
                return

            item.processed_b64_frames = frames
            # We keep raw_bytes so the APISender can forward the original high-res image to the AI Engine
            # item.raw_bytes = None

            self._log.info(
                "[%s] STEP 3: Preprocessed → %d frame(s) ready for API.",
                item.source, len(frames),
            )
            await self.api_queue.put(item)

        except Exception as exc:
            self._log.error(
                "Preprocessing failed for %s: %s", item.media_url, exc, exc_info=True
            )

    async def run(self) -> None:
        """Continuously consume preprocess_queue and process items."""
        self._log.info("Preprocessor worker %d started.", self.worker_id)
        while True:
            try:
                item: MediaItem = await self.preprocess_queue.get()
                await self.process_item(item)
            except asyncio.CancelledError:
                self._log.info("Preprocessor worker %d shutting down.", self.worker_id)
                break
            except Exception as exc:
                self._log.error(
                    "Unexpected error in preprocessor: %s", exc, exc_info=True
                )
            finally:
                try:
                    self.preprocess_queue.task_done()
                except ValueError:
                    pass
