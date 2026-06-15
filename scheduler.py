import heapq
import time
from collections import deque
from typing import Callable


class Scheduler:
    def __init__(self):
        self.sleeping = []
        self.ready = deque()
        self.sequence: int = 0

    def call_later(self, delay: int, func: Callable) -> None:
        self.sequence += 1
        deadline = time.time() + delay
        heapq.heappush(self.sleeping, (deadline, self.sequence, func))

    def call_soon(self, func: Callable) -> None:
        self.ready.append(func)

    def run(self) -> None:
        while self.ready or self.sleeping:
            if not self.ready:
                deadline, _, func = heapq.heappop(self.sleeping)
                delta = deadline - time.time()
                if delta > 0:
                    time.sleep(delta)
                self.ready.append(func)

            while self.ready:
                func = self.ready.popleft()
                func()


shed = Scheduler()


def countdown(n: int) -> None:
    if n > 0:
        print("Down", n)
        shed.call_later(4, lambda: countdown(n - 1))


def countup(stop: int) -> None:
    def _run(x: int):
        if x < stop:
            print("Up", x)
            shed.call_later(1, lambda: _run(x + 1))

    _run(0)


shed.call_later(4, lambda: countdown(5))
shed.call_later(1, lambda: countup(20))
shed.run()
