import heapq
import time
from collections import deque
from select import select
from socket import SOCK_STREAM, AF_INET, socket
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
        self._read_waiting = {}
        self._write_waiting = {}

    def call_later(self, delay: int, func: Callable | None) -> None:
        self.sequence += 1
        deadline = time.time() + delay
        heapq.heappush(self.sleeping, (deadline, self.sequence, func))

    def call_soon(self, func: Callable) -> None:
        self.ready.append(func)

    def run(self) -> None:
        while self.ready or self.sleeping or self._read_waiting or self._write_waiting:
            if not self.ready:
                if self.sleeping:
                    deadline, _, coro = self.sleeping[0]
                    timeout = deadline - time.time()
                    if timeout < 0:
                        timeout = 0
                else:
                    timeout = None

                # I/O
                can_read, can_write, _ = select(
                    self._read_waiting, self._write_waiting, [], timeout
                )

                for fd in can_read:
                    self.ready.append(self._read_waiting.pop(fd))
                for fd in can_write:
                    self.ready.append(self._write_waiting.pop(fd))

                now = time.time()
                while self.sleeping:
                    if now > self.sleeping[0][0]:
                        self.ready.append(heapq.heappop(self.sleeping)[2])
                    else:
                        break

            while self.ready:
                coro = self.ready.popleft()
                coro()

    def new_task(self, coro):
        self.ready.append(Task(coro))

    async def sleep(self, delay):
        self.call_later(delay, self.current)
        self.current = None
        await switch()

    def read_wait(self, fileno, coro):
        self._read_waiting[fileno] = coro

    def write_wait(self, fileno, coro):
        self._write_waiting[fileno] = coro

    async def recv(self, sock, maxbytes):
        self.read_wait(sock, self.current)
        self.current = None
        await switch()
        return sock.recv(maxbytes)

    async def send(self, sock, data):
        self.write_wait(sock, self.current)
        self.current = None
        await switch()
        return sock.send(data)

    async def accept(self, sock):
        self.read_wait(sock, self.current)
        self.current = None
        await switch()
        return sock.accept()


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


async def echo_handler(sock: socket):
    while True:
        data = await shed.recv(sock, 10_000)
        if not data:
            break
        await shed.send(sock, b"Got: " + data)
    print("Connection closed")


async def tcp_server(addr):
    sock = socket(AF_INET, SOCK_STREAM)
    sock.bind(addr)
    sock.listen(1)
    while True:
        client, addr = await shed.accept(sock)
        shed.new_task(echo_handler(client))


if __name__ == "__main__":
    # To test it locally:
    # nc localhost 30000
    #   hello
    #   Got: hello

    aq = AsyncQueue()
    shed = Scheduler()
    shed.new_task(tcp_server(("", 30000)))
    shed.run()
