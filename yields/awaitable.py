import time
from collections import deque


class Awaitable:
    def __await__(self):
        yield


def switch():
    return Awaitable()


class Scheduler:
    def __init__(self) -> None:
        self.ready = deque()

    def new_task(self, coro) -> None:
        self.ready.append(coro)

    def run(self) -> None:
        while self.ready:
            try:
                coro = self.ready.popleft()
                coro.send(None)  # Send to a coroutine
                if coro:
                    self.ready.append(coro)
            except StopIteration:
                pass


async def countdown(n: int) -> None:
    while n > 0:
        print("Countdown", n)
        time.sleep(1)
        await switch()
        n -= 1


async def countup(stop: int) -> None:
    i = 0
    while i < stop:
        print("Countup", i)
        time.sleep(1)
        await switch()
        i += 1

    return None


if __name__ == "__main__":
    shed = Scheduler()
    shed.new_task(countdown(10))
    shed.new_task(countup(10))
    shed.run()
