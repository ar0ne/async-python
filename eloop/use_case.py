import time

from eloop.event_loop import EventLoop


def sleep(delay: int):
    return loop.call_later(delay, lambda: True)


def test():
    yield from sleep(2)
    print(time.monotonic(), "completed")


if __name__ == "__main__":
    loop = EventLoop()
    print("Start", time.monotonic())
    loop.create_task(test())
    loop.create_task(test())
    loop.run_until_complete(test())
    print("Finish", time.time())
