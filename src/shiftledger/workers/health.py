import sys
import time

from shiftledger.workers.runtime import heartbeat_path


def main() -> None:
    role = sys.argv[1]
    try:
        last_seen = float(heartbeat_path(role).read_text(encoding="ascii"))
        healthy = 0 <= time.time() - last_seen < 45
    except (OSError, ValueError):
        healthy = False
    raise SystemExit(0 if healthy else 1)


if __name__ == "__main__":
    main()
