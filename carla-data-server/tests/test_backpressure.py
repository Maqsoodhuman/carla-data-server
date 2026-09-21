"""Unit test for server._enqueue_drop_oldest, the bounded-queue backpressure
primitive CLAUDE.md says must never be removed ("a slow client must not
stall the tick loop").

Added after discovering a live-network test of this behavior is unreliable
over loopback at STUB-mode message sizes: a client that simply pauses
recv() for a few seconds never actually fills the app-level asyncio.Queue,
because the OS kernel's TCP receive buffer absorbs everything first (a few
KB of small JSON messages is nowhere near typical default socket buffer
sizes). The bounded-queue path only engages under genuine sustained TCP-level
backpressure (e.g. a truly stalled reader under large sensor-JPEG payload
throughput) - this direct unit test verifies the actual drop-oldest logic
deterministically, independent of OS socket buffering.
"""

import asyncio

from server import _enqueue_drop_oldest


def test_drop_oldest_keeps_only_the_newest_maxsize_items():
    async def run():
        q = asyncio.Queue(maxsize=10)
        for i in range(25):
            await _enqueue_drop_oldest(q, f"msg-{i}")
        assert q.qsize() == 10

        remaining = []
        while not q.empty():
            remaining.append(q.get_nowait())
        assert remaining == [f"msg-{i}" for i in range(15, 25)]

    asyncio.run(run())


def test_drop_oldest_is_a_noop_below_capacity():
    async def run():
        q = asyncio.Queue(maxsize=10)
        for i in range(5):
            await _enqueue_drop_oldest(q, f"msg-{i}")
        assert q.qsize() == 5

    asyncio.run(run())
