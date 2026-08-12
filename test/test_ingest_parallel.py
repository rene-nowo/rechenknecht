"""The two pieces of ingest_parallel.py that are not just ingest.py called from a thread:
the shared rate budget, and the per-thread EDGAR_API. Both are only wrong under concurrency,
so both are exercised with real threads. No network, no database.
"""

import threading
import time
from concurrent.futures import ThreadPoolExecutor

from ingest_parallel import RateLimiter, _GatedRequests, _ThreadLocalEdgar


def test_limiter_paces_across_threads_not_per_thread():
    """8 acquisitions at 20/s cost at least 7 intervals no matter how many threads want them.

    This is the whole point of the shared budget: the SEC ceiling is per IP, so N threads must
    NOT each get their own N requests/second.
    """
    limiter = RateLimiter(per_second=20.0)
    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda _: limiter.acquire(), range(8)))
    elapsed = time.monotonic() - started

    assert elapsed >= 7 * (1 / 20.0) * 0.9, f"8 acquisitions finished in {elapsed:.3f}s — not paced"


def test_limiter_does_not_burst_after_idle():
    """An idle limiter must not hand out a backlog of slots at once — that is exactly the
    shape a per-IP ceiling punishes, and it is what a token bucket with a burst would do."""
    limiter = RateLimiter(per_second=10.0)
    limiter.acquire()
    time.sleep(0.3)  # idle long enough for a bucket to have refilled ~3 tokens

    started = time.monotonic()
    for _ in range(3):
        limiter.acquire()
    elapsed = time.monotonic() - started

    assert elapsed >= 2 * 0.1 * 0.9, f"3 acquisitions after idle took {elapsed:.3f}s — it bursted"


def test_penalize_delays_every_thread():
    """One 429 slows the whole process, not just the thread that received it."""
    limiter = RateLimiter(per_second=1000.0)
    limiter.penalize(0.25)

    started = time.monotonic()
    limiter.acquire()
    elapsed = time.monotonic() - started

    assert elapsed >= 0.2, f"acquire after penalize returned in {elapsed:.3f}s"
    assert limiter.throttle_events == 1


def test_gated_requests_penalizes_on_429_and_delegates_everything_else():
    class FakeResponse:
        def __init__(self, status_code):
            self.status_code = status_code

    class FakeRequests:
        Timeout = RuntimeError  # an attribute that must pass through untouched

        def __init__(self):
            self.calls = []

        def get(self, url, **kwargs):
            self.calls.append(url)
            return FakeResponse(429 if "throttled" in url else 200)

    limiter = RateLimiter(per_second=1000.0)
    real = FakeRequests()
    gated = _GatedRequests(limiter, real, penalty=0.0)

    assert gated.get("https://sec.gov/ok").status_code == 200
    assert limiter.throttle_events == 0
    assert gated.get("https://sec.gov/throttled").status_code == 429
    assert limiter.throttle_events == 1
    assert real.calls == ["https://sec.gov/ok", "https://sec.gov/throttled"]
    assert gated.Timeout is RuntimeError, "non-get attributes must delegate to the real module"


def test_thread_local_edgar_gives_each_thread_its_own_instance():
    """The bug this proxy exists for: EDGAR_API.get_all_data() stores self.ticker/self.cik and
    get_file() reads them back, so one shared instance lets thread A build a URL out of thread
    B's CIK. Two threads must never see the same object."""
    proxy = _ThreadLocalEdgar()
    seen = {}
    barrier = threading.Barrier(2)

    def record(name):
        proxy.cik_map  # first attribute access constructs this thread's instance
        barrier.wait(timeout=30)  # both threads hold their instance at the same time
        seen[name] = id(proxy._local.edgar)

    threads = [threading.Thread(target=record, args=(n,)) for n in ("a", "b")]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=60)

    assert len(seen) == 2, f"threads did not both finish: {seen}"
    assert seen["a"] != seen["b"], "both threads shared one EDGAR_API — the race is still open"


def test_thread_local_edgar_keeps_per_ticker_state_apart():
    """The state that actually races: writing .ticker in one thread must not be visible in
    the other."""
    proxy = _ThreadLocalEdgar()
    barrier = threading.Barrier(2)
    observed = {}

    def use(name):
        proxy._local.edgar = type("Stub", (), {})()
        proxy._local.edgar.ticker = name
        barrier.wait(timeout=30)
        observed[name] = proxy.ticker

    threads = [threading.Thread(target=use, args=(n,)) for n in ("jpm", "bac")]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=60)

    assert observed == {"jpm": "jpm", "bac": "bac"}, f"state leaked between threads: {observed}"
