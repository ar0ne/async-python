from collections import deque
from typing import Callable, Any

from scheduler import Scheduler


class AsyncQueue:
    def __init__(self) -> None:
        self.items = deque()
        self.waiting = deque()

    def get(self, callback: Callable) -> None:
        if self.items:
            callback(self.items.popleft())
        else:
            self.waiting.append(lambda: self.get(callback))

    def put(self, item: Any | None) -> None:
        self.items.append(item)
        if self.waiting:
            func = self.waiting.popleft()
            shed.call_soon(func)


def producer(q: AsyncQueue, count: int) -> None:
    def _run(n) -> None:
        if n < count:
            print("Producing", n)
            q.put(n)
            shed.call_later(1, lambda: _run(n + 1))
        else:
            print("Producer done")
            q.put(None)

    _run(0)


def consumer(q: AsyncQueue) -> None:
    def _consume(item) -> None:
        if item is None:
            print("Consumer Done")
            pass
        else:
            print("Consuming", item)
            shed.call_soon(lambda: consumer(q))

    q.get(callback=_consume)


if __name__ == "__main__":
    shed = Scheduler()

    aq = AsyncQueue()

    shed.call_soon(lambda: producer(aq, 10))
    shed.call_soon(lambda: consumer(aq))
    shed.run()
