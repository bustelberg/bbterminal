"""Small bounded executor: reject overload, and never enqueue before a worker starts."""
from concurrent.futures import Future
from queue import Queue
import threading


class BoundedExecutor:
    """At most max_workers accepted tasks, with persistent daemon workers.

    ThreadPoolExecutor queues before trying to start a worker. If Thread.start
    fails, submit raises but the queued callable can still execute later. Here a
    rejected submission has no side effects and no hidden queue entry.
    """

    def __init__(self, max_workers: int, thread_name_prefix: str):
        self._max_workers = max_workers
        self._prefix = thread_name_prefix
        self._slots = threading.BoundedSemaphore(max_workers)
        self._lock = threading.Lock()
        self._threads: list[threading.Thread] = []
        self._queue: Queue = Queue(maxsize=max_workers)

    def submit(self, work) -> Future:
        if not self._slots.acquire(blocking=False):
            raise RuntimeError("background capacity is full")
        try:
            with self._lock:
                if len(self._threads) < self._max_workers:
                    worker = threading.Thread(
                        target=self._run, daemon=True,
                        name=f"{self._prefix}-{len(self._threads)}",
                    )
                    worker.start()  # failure leaves nothing in the queue
                    self._threads.append(worker)
                future = Future()
                self._queue.put_nowait((future, work))
                return future
        except BaseException:
            self._slots.release()
            raise

    def _run(self) -> None:
        while True:
            future, work = self._queue.get()
            result, failure = None, None
            try:
                if future.set_running_or_notify_cancel():
                    try:
                        result = work()
                    except BaseException as exc:
                        failure = exc
            finally:
                self._slots.release()
                self._queue.task_done()
            # Release capacity BEFORE callbacks can submit the next task.
            if not future.cancelled():
                if failure is not None:
                    future.set_exception(failure)
                else:
                    future.set_result(result)
