"""Main module."""
import asyncio
from nats.aio.client import Client as NATS
from typing import Dict, List
import json


class Async_PubSub_Nats:
    def __init__(self, server: str, port: int) -> None:
        self.url = f"nats://{server}:{port}"
        self.reconnect_seconds = 10

        pass

    async def connect(self):
        self.nc = NATS()
        await self.nc.connect(
            servers=[self.url],
            reconnect_time_wait=self.reconnect_seconds,
            verbose=True
        )

    async def publish(self, subject: str, payloads: List[Dict]) -> None:
        for p in payloads:
            await self.nc.publish(subject=subject, payload=json.dumps(p).encode())
        await self.nc.flush()

    async def subscribe(self, subject: str, message_handler) -> int:
        sid = await self.nc.subscribe(subject=subject, cb=message_handler)
        return sid

    async def unsubscribe(self, sid) -> None:
        await self.nc.auto_unsubscribe(sid, 1)

    async def close(self) -> None:
        await self.nc.close()
