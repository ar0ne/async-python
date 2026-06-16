import logging
from typing import Any, Callable

logger = logging.getLogger(__name__)


class Future:
    def __init__(self) -> None:
        self._exc = None
        self._result = None
        self._done = False
        self._done_callbacks: set[Callable[["Future"], Any]] = set()

    def _handle_callbacks(self):
        for cb in self._done_callbacks:
            try:
                cb(self)
            except Exception:
                logger.exception("Unhandled error in %r", cb)

    def set_result(self, result: Any):
        self._done = True
        self._result = result
        self._handle_callbacks()

    def set_exception(self, exc: Exception):
        self._done = True
        self._exc = exc
        self._handle_callbacks()

    def get_result(self) -> Any:
        if self._exc:
            raise self._exc
        return self._result

    def __iter__(self):
        while not self._done:
            yield None
        return self.get_result()

    def __await__(self):
        yield None
