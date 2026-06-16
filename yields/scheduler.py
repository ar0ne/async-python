from collections import deque
from typing import Generator


class Scheduler:
    def __init__(self) -> None:
        self.ready = deque()

    def new_task(self, gen: Generator) -> None:
        self.ready.append(gen)

    def run(self) -> None:
        while self.ready:
            try:
                gen = self.ready.popleft()
                next(gen)
                if gen:
                    self.ready.append(gen)
            except StopIteration:
                pass


def countdown(n: int) -> Generator[int]:
    while n > 0:
        print("Countdown", n)
        yield n
        n -= 1


def countup(stop: int) -> Generator[int]:
    i = 0
    while i < stop:
        print("Countup", i)
        yield i
        i += 1

    return None


if __name__ == "__main__":
    shed = Scheduler()
    shed.new_task(countdown(10))
    shed.new_task(countup(10))
    shed.run()
