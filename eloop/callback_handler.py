import heapq
import time
from collections.abc import Callable
from functools import partial

from eloop.future import Future


class CallbackHandler:
    def __init__(self):
        self._waiting = []
        self._sequence = 0

    def call_later(self, delay: int, func: Callable, *args) -> Future:
        future = Future()
        self._sequence += 1
        heapq.heappush(
            self._waiting,
            (
                delay + time.time(),
                self._sequence,  # increase sequence to ensure few functions won't go simultaneously
                future,  # result will go here
                partial(func, *args),  # our func
            ),
        )
        return future

    def step(self) -> None:
        while self._waiting:
            if self._waiting[0][0] > time.time():
                return
            _, _, future, func = heapq.heappop(self._waiting)
            try:
                future.set_result(func())
            except Exception as exc:
                future.set_exception(exc)
