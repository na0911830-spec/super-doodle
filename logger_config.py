import os
import sys
import logging
import collections

# Store last 100 log lines in a thread-safe deque
RECENT_LOGS = collections.deque(maxlen=100)
FATAL_ERRORS = collections.deque(maxlen=10)

class InMemoryLogHandler(logging.Handler):
    def emit(self, record):
        try:
            log_entry = self.format(record)
            RECENT_LOGS.append(log_entry)
        except Exception:
            self.handleError(record)

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

stdout_handler = logging.StreamHandler(sys.stdout)
file_handler = logging.FileHandler("stream_service.log", encoding="utf-8")
log_handler = InMemoryLogHandler()

formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
stdout_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)
log_handler.setFormatter(formatter)

root_logger = logging.getLogger()
root_logger.setLevel(LOG_LEVEL)
root_logger.addHandler(stdout_handler)
root_logger.addHandler(file_handler)
root_logger.addHandler(log_handler)

log = logging.getLogger("stream-service")
