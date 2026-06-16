import heapq
import time
from collections import deque
from typing import Generator


class Awaitable:
    def __await__(self):
        yield


def switch():
    return Awaitable()


class Scheduler:
    def __init__(self) -> None:
        self.ready = deque()
        self.sleeping = []
        self.sequence = 0
        self.current: Generator | None = None

    async def sleep(self, delay) -> None:
        deadline = time.time() + delay
        self.sequence += 1
        heapq.heappush(self.sleeping, (deadline, self.sequence, self.current))
        self.current = None
        await switch()

    def new_task(self, coro) -> None:
        self.ready.append(coro)

    def run(self) -> None:
        while self.ready or self.sleeping:
            if not self.ready:
                deadline, _, coro = heapq.heappop(self.sleeping)
                delay = deadline - time.time()
                if delay > 0:
                    time.sleep(delay)
                self.ready.append(coro)

            self.current = self.ready.popleft()
            try:
                self.current.send(None)  # Send to a coroutine
                if self.current:
                    self.ready.append(self.current)
            except StopIteration:
                pass


async def countdown(n: int) -> None:
    while n > 0:
        print("Countdown", n)
        await shed.sleep(2)
        n -= 1


async def countup(stop: int) -> None:
    i = 0
    while i < stop:
        print("Countup", i)
        await shed.sleep(4)
        i += 1

    return None


if __name__ == "__main__":
    shed = Scheduler()
    shed.new_task(countdown(10))
    shed.new_task(countup(10))
    shed.run()
