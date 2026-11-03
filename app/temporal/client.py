"""
Temporal client singleton manager for API routes and background tasks.
Ensures connections are reused across requests rather than reconnected each time.
"""
import asyncio
import logging
from temporalio.client import Client

from app.config import settings

logger = logging.getLogger("analytics_service.temporal.client")


class TemporalClientManager:
    """
    Manages a cached Temporal client connection per asyncio event loop.
    Follows a similar pattern to the Database pool in app/database/db.py.
    """
    def __init__(self):
        self._clients = {}
        self._locks = {}

    @property
    def _connect_lock(self) -> asyncio.Lock:
        loop = asyncio.get_running_loop()
        if loop not in self._locks:
            self._locks[loop] = asyncio.Lock()
        return self._locks[loop]

    async def get_client(self) -> Client:
        """
        Returns a cached Temporal client instance for the current running event loop,
        connecting if one does not already exist.
        """
        loop = asyncio.get_running_loop()
        client = self._clients.get(loop)
        if client is not None:
            return client

        async with self._connect_lock:
            client = self._clients.get(loop)
            if client is not None:
                return client

            logger.info(
                f"Establishing Temporal client connection to {settings.TEMPORAL_HOST} "
                f"(namespace: {settings.TEMPORAL_NAMESPACE})..."
            )
            client = await Client.connect(
                settings.TEMPORAL_HOST,
                namespace=settings.TEMPORAL_NAMESPACE,
            )
            self._clients[loop] = client
            logger.info("Temporal client connection established successfully.")
            return client

    async def close(self) -> None:
        """Closes cached client for the current running loop."""
        try:
            loop = asyncio.get_running_loop()
            self._clients.pop(loop, None)
            self._locks.pop(loop, None)
        except RuntimeError:
            self._clients.clear()
            self._locks.clear()


temporal_client = TemporalClientManager()


async def get_temporal_client() -> Client:
    """Helper function to acquire the shared Temporal client."""
    return await temporal_client.get_client()
