"""Logging utilities"""
import logging
import sys


def setup_logging(level: str = "INFO") -> None:
    """Setup logging.

        Args:
            level: Log level (DEBUG/INFO/WARNING/ERROR/CRITICAL)

        Example:
            setup_logging(level="DEBUG")
        """
    # suppress redundant logs from third-party libraries
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)

    # set root log level
    log_level = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )