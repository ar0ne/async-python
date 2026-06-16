from collections import deque
from typing import Any, Generator

from ascheduler import Scheduler
from ascheduler import switch


class QueueClosed(BaseException): ...


class AsyncQueue:
    def __init__(self):
        self.items: deque[Any] = deque()
        self.waiting: deque[Generator | None] = deque()
        self.current = None
        self._closed = False

    def close(self) -> None:
        self._closed = True
        if self.waiting and not self.items:
            shed.ready.append(self.waiting.popleft())

    def put(self, item) -> None:
        if self._closed:
            raise QueueClosed()
        self.items.append(item)
        if self.waiting:
            shed.ready.append(self.waiting.popleft())

    async def get(self) -> Any:
        while not self.items:
            if self._closed:
                raise QueueClosed()
            self.waiting.append(shed.current)
            shed.current = None
            await switch()
        return self.items.popleft()


async def consumer(q: AsyncQueue) -> None:
    try:
        while True:
            item = await q.get()
            print("Consumer", item)
            await shed.sleep(1)
    except QueueClosed:
        print("Consumer Done")


async def producer(q: AsyncQueue, stop: int) -> None:
    i = 0
    while i < stop:
        print("Producer", i)
        q.put(i)
        i += 1
        await shed.sleep(1)
    print("Producer Done")
    q.close()


if __name__ == "__main__":
    aq = AsyncQueue()
    shed = Scheduler()
    shed.new_task(consumer(aq))
    shed.new_task(producer(aq, 10))
    shed.run()
