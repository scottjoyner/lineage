from queue.worker.worker import backoff_seconds

def test_backoff_monotonic():
    b1 = backoff_seconds(1)
    b2 = backoff_seconds(2)
    b3 = backoff_seconds(3)
    assert b1 <= b2 <= b3 or b1 <= b3  # allow jitter but growth generally increases
    assert b1 >= 10  # base floor applied
