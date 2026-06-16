import heapq
import time
from collections import deque
from typing import Any, Callable


class QueueClosed(BaseException): ...


class Awaitable:
    def __await__(self):
        yield


def switch() -> Awaitable:
    return Awaitable()


class Task:
    # Wrapper for coroutines

    def __init__(self, coro) -> None:
        self._coro = coro

    def __call__(self, *args, **kwargs):
        try:
            shed.current = self
            self._coro.send(None)
            if shed.current:
                shed.ready.append(self)
        except StopIteration:
            pass


class Scheduler:
    current: Callable | None

    def __init__(self):
        self.sleeping = []
        self.ready = deque()
        self.sequence: int = 0

    def call_later(self, delay: int, func: Callable | None) -> None:
        self.sequence += 1
        deadline = time.time() + delay
        heapq.heappush(self.sleeping, (deadline, self.sequence, func))

    def call_soon(self, func: Callable) -> None:
        self.ready.append(func)

    def run(self) -> None:
        while self.ready or self.sleeping:
            if not self.ready:
                deadline, _, coro = heapq.heappop(self.sleeping)
                delta = deadline - time.time()
                if delta > 0:
                    time.sleep(delta)
                self.ready.append(coro)

            while self.ready:
                self.current = self.ready.popleft()
                self.current()

    def new_task(self, coro):
        self.ready.append(Task(coro))

    async def sleep(self, delay):
        self.call_later(delay, self.current)
        self.current = None
        await switch()


class AsyncQueue:
    def __init__(self):
        self.items: deque[Any] = deque()
        self.waiting: deque[Callable | None] = deque()
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


async def countdown(n: int) -> None:
    while n > 0:
        print("DOWN", n)
        await shed.sleep(1)
        n -= 1


async def countup(stop: int) -> None:
    i = 0
    while i < stop:
        print("UP", i)
        await shed.sleep(2)
        i += 1

    return None


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
    shed.new_task(countdown(10))
    shed.new_task(countup(10))
    shed.new_task(producer(aq, 10))
    shed.new_task(consumer(aq))
    shed.run()
