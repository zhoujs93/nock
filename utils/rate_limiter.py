import time
import random

# put near your other helpers
class _RateLimiter:
    def __init__(self, rps: float):
        self.min_interval = 1.0 / max(1e-6, rps)
        self.next_ok_at = 0.0
    def wait(self):
        now = time.time()
        if now < self.next_ok_at:
            time.sleep(self.next_ok_at - now)
        self.next_ok_at = time.time() + self.min_interval
