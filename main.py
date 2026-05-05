"""
main.py
=======
Root entry point for the Digital Asset Protection Crawler Pipeline.

Usage:
    python main.py                                              — full pipeline
    python main.py --test                                       — connectivity tests
    python main.py --keywords "ipl,cricket" --limit 30          — full pipeline with custom topics

    # Social scraper (Reddit + Twitter → suspicious/)
    python main.py --standalone --source social
    python main.py --standalone --source social --mode top
    python main.py --standalone --source social --keywords "deepfake,DMCA" --limit 20

    # Stock scraper (Unsplash/Pexels/Pixabay → assets/)
    python main.py --standalone --source stock
    python main.py --standalone --source stock --keywords "nature,landscape" --limit 10
"""

import argparse
import asyncio
import sys
from pathlib import Path

# Add 'src' to sys.path so we can import 'crawler_pipeline' directly
src_path = str(Path(__file__).parent / "src")
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from crawler_pipeline.utils import setup_logging
from crawler_pipeline.pipeline import run_pipeline, _shutdown_event
from crawler_pipeline.crawler import run_connectivity_tests
from crawler_pipeline.standalone import StandaloneRunner
from crawler_pipeline.stock_scraper import StockRunner

import signal


def _install_signal_handlers(loop: asyncio.AbstractEventLoop) -> None:
    """Register SIGINT / SIGTERM to trigger graceful shutdown."""
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _shutdown_event.set)
        except (NotImplementedError, RuntimeError):
            # Windows does not support add_signal_handler for all signals
            pass


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Digital Asset Protection — Crawler Pipeline"
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Run crawler connectivity tests and exit.",
    )
    parser.add_argument(
        "--standalone",
        action="store_true",
        help="Playwright-only scrape: download images and save to assets/ folder, then exit.",
    )
    parser.add_argument(
        "--output",
        default=None,
        metavar="DIR",
        help="Output directory for standalone mode (default: ./assets).",
    )
    parser.add_argument(
        "--keywords",
        default=None,
        metavar="KW1,KW2",
        help=(
            "Comma-separated keywords/topics to search for. "
            "Applies to both standalone and full pipeline modes (overrides .env KEYWORDS)."
        ),
    )
    parser.add_argument(
        "--mode",
        default="home",
        choices=["home", "top"],
        metavar="MODE",
        help=(
            "Standalone scrape mode: "
            "'home' = keyword-based search (default), "
            "'top' = browse subreddits + accounts from targets.json."
        ),
    )
    parser.add_argument(
        "--limit",
        default=None,
        type=int,
        metavar="N",
        help=(
            "Max images per source per keyword/target. "
            "Applies to both standalone and full pipeline modes."
        ),
    )
    parser.add_argument(
        "--targets",
        default=None,
        metavar="FILE",
        help="Path to targets.json for top mode (default: ./targets.json).",
    )
    parser.add_argument(
        "--source",
        default="social",
        choices=["social", "stock"],
        metavar="SOURCE",
        help=(
            "Standalone scrape source: "
            "'social' = Reddit + Twitter → suspicious/ (default), "
            "'stock'  = Unsplash + Pexels + Pixabay + Shutterstock + Getty → assets/."
        ),
    )
    parser.add_argument(
        "--logfile",
        default=None,
        metavar="PATH",
        help="Optional path for a log file (logs are written to both console and file).",
    )
    args = parser.parse_args()

    setup_logging(log_file=args.logfile)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    if args.test:
        loop.run_until_complete(run_connectivity_tests())
        loop.close()
        sys.exit(0)

    if args.standalone:
        output_dir = Path(args.output) if args.output else None
        keywords = [k.strip() for k in args.keywords.split(",") if k.strip()] if args.keywords else None

        if args.source == "stock":
            # Stock image sites (Unsplash / Pexels / Pixabay) → assets/
            runner: StockRunner | StandaloneRunner = StockRunner(
                output_dir=output_dir,
                keywords=keywords,
                limit=args.limit,
            )
        else:
            # Social sources (Reddit + Twitter) → suspicious/
            targets_file = Path(args.targets) if args.targets else None
            runner = StandaloneRunner(
                output_dir=output_dir,
                keywords=keywords,
                limit=args.limit,
                mode=args.mode,
                targets_file=targets_file,
            )

        loop.run_until_complete(runner.run())
        loop.close()
        sys.exit(0)

    _install_signal_handlers(loop)

    # Parse keywords / limit for full pipeline mode
    pipeline_keywords = (
        [k.strip() for k in args.keywords.split(",") if k.strip()]
        if args.keywords else None
    )

    try:
        loop.run_until_complete(
            run_pipeline(
                keywords=pipeline_keywords,
                crawl_limit=args.limit,
                source=args.source,
            )
        )
    except KeyboardInterrupt:
        from crawler_pipeline.utils import get_logger
        get_logger("main").info("KeyboardInterrupt received.")
    finally:
        pending = asyncio.all_tasks(loop)
        for task in pending:
            task.cancel()
        if pending:
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        loop.close()


if __name__ == "__main__":
    main()
