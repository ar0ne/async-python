import inspect
from collections import deque
from collections.abc import Callable

from eloop.callback_handler import CallbackHandler
from eloop.future import Future


class EventLoop:
    def __init__(self):
        self._events = deque()
        self._callback_handler = CallbackHandler()

    def call_later(self, delay: int, callback: Callable, *args) -> Future:
        return self._callback_handler.call_later(delay, callback, *args)

    def step(self, coro) -> None:
        try:
            next(coro)
        except StopIteration as exc:
            return exc.value
        self._events.append(coro)
        return None

    def run(self) -> None:
        while self._events:
            self._callback_handler.step()
            self.step(self._events.popleft())

    def create_task(self, coro):
        future = Future()

        def wrapper():
            if inspect.isawaitable(coro):
                res = yield from coro.__await__()
            else:
                res = yield from coro
            try:
                future.set_result(res)
            except Exception as exc:
                future.set_exception(exc)

        gen = wrapper()
        self._events.append(gen)
        return future

    def run_until_complete(self, coro):
        future = self.create_task(coro)
        self.run()
        return future.get_result()
