from collections import deque
from typing import Callable, Any

from scheduler import Scheduler


class QueueClosed(Exception): ...


class Result:
    def __init__(self, value: Any | None = None, exc: Exception | None = None) -> None:
        if value is None and exc is None:
            raise ValueError("Result must contain value or exception")
        self._value = value
        self._exc = exc

    def result(self):
        if self._exc:
            raise self._exc
        return self._value


class AsyncQueue:
    def __init__(self) -> None:
        self.items = deque()
        self.waiting = deque()
        self._closed = False

    def close(self) -> None:
        self._closed = True
        if self.waiting and not self.items:
            for f in self.waiting:
                shed.call_soon(lambda: f())

    def get(self, callback: Callable) -> None:
        if self.items:
            callback(Result(value=self.items.popleft()))
        else:
            if self._closed:
                callback(Result(exc=QueueClosed()))
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
            q.close()

    _run(0)


def consumer(q: AsyncQueue) -> None:
    def _consume(item: Result) -> None:
        try:
            val = item.result()
            print("Consuming", val)
            shed.call_soon(lambda: consumer(q))
        except QueueClosed:
            print("Consumer Done")

    q.get(callback=_consume)


if __name__ == "__main__":
    shed = Scheduler()

    aq = AsyncQueue()

    shed.call_soon(lambda: producer(aq, 1))
    shed.call_soon(lambda: consumer(aq))
    shed.run()
