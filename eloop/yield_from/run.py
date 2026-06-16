from collections import deque


def foo():
    yield None


def bar():
    yield foo()
    yield foo()
    return 42


def step(gen):
    return step(gen.send(None)) if gen is not None else None


def run(coro):
    queue = deque()
    queue.append(coro)
    while queue:
        try:
            c = queue.popleft()
            res = step(c)
            print(res)
            queue.append(c)
        except StopIteration as e:
            return e.value


print(run(bar()))
