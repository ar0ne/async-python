from collections import deque


def foo():
    yield None


def bar():
    yield from foo()
    yield from foo()
    return 42


def run(coro):
    queue = deque()
    queue.append(coro)
    while queue:
        gen = queue.popleft()
        try:
            res = gen.send(None)
            print(res)
            queue.append(gen)
        except StopIteration as e:
            print("Stopped", e.value)


run(bar())
