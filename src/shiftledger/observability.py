import logging
import sys

import structlog
from prometheus_client import Counter, Histogram

HTTP_REQUESTS = Counter(
    "shiftledger_http_requests_total", "HTTP requests", ["method", "route", "status"]
)
HTTP_DURATION = Histogram(
    "shiftledger_http_request_duration_seconds", "HTTP latency", ["method", "route"]
)
PUBLISHED_EVENTS = Counter("shiftledger_outbox_published_total", "Acknowledged outbox events")
OUTBOX_FAILURES = Counter("shiftledger_outbox_failures_total", "Failed publishing attempts")
PROCESSED_EVENTS = Counter("shiftledger_processed_events_total", "Events", ["outcome"])


def configure_logging(level: str) -> None:
    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=level)
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
