import asyncio
import logging
from pathlib import Path

import asyncpg

from app.config import settings

logger = logging.getLogger("analytics_service.db")

BASE_DIR = Path(__file__).resolve().parents[2]
SCHEMA_FILE = BASE_DIR / "schema.sql"
SEED_PROMPTS_FILE = BASE_DIR / "seed_prompts.sql"
SEED_THEMES_FILE = BASE_DIR / "seed_themes.sql"

class Database:
    def __init__(self):
        self._pools = {}
        self._locks = {}

    @property
    def pool(self):
        try:
            loop = asyncio.get_running_loop()
            return self._pools.get(loop)
        except RuntimeError:
            return None

    @pool.setter
    def pool(self, value):
        try:
            loop = asyncio.get_running_loop()
            if value is None:
                self._pools.pop(loop, None)
            else:
                self._pools[loop] = value
        except RuntimeError:
            pass

    @property
    def _connect_lock(self):
        try:
            loop = asyncio.get_running_loop()
            if loop not in self._locks:
                self._locks[loop] = asyncio.Lock()
            return self._locks[loop]
        except RuntimeError:
            return None

    @_connect_lock.setter
    def _connect_lock(self, value):
        pass

    async def initialize_schema(self) -> None:
        """
        Creates the required database tables if they do not already exist.
        This prevents startup failures when the configured PostgreSQL database is empty.
        """
        if not self.pool:
            raise RuntimeError("Database pool is not initialized. Call connect() first.")

        async with self.pool.acquire() as conn:
            # web/consumer/worker each run as separate processes (run_all.sh) and
            # every one of them calls connect()/initialize_schema() independently
            # at startup — the asyncio.Lock below only serializes coroutines within
            # one process, so without a cross-process lock they race on the same
            # CREATE TABLE/TYPE DDL and one hits a Postgres catalog collision (e.g.
            # duplicate key on pg_type) even with "IF NOT EXISTS", since the
            # existence check and creation aren't atomic across concurrent sessions.
            await conn.execute("SELECT pg_advisory_lock(727384910)")
            try:
                if settings.RESET_DB:
                    if settings.ENVIRONMENT.lower().strip() == "development":
                        await conn.execute("DROP SCHEMA IF EXISTS public CASCADE;")
                        await conn.execute("CREATE SCHEMA public;")
                        logger.warning("Database schema reset requested; dropped and recreated public schema.")
                    else:
                        logger.error(
                            f"RESET_DB=True was requested but ENVIRONMENT={settings.ENVIRONMENT!r} is not "
                            "'development' — refusing to drop the schema. Set ENVIRONMENT=development if "
                            "this is intentional."
                        )

                schema_sql = SCHEMA_FILE.read_text(encoding="utf-8")
                # Only inject IF NOT EXISTS on bare CREATE TABLE/INDEX statements;
                # schema.sql already has IF NOT EXISTS on some indexes — a blind
                # replace would produce "CREATE INDEX IF NOT EXISTS IF NOT EXISTS"
                # which is invalid SQL and causes a syntax error at "NOT".
                schema_sql = schema_sql.replace("CREATE TABLE IF NOT EXISTS ", "__TABLE_ALREADY__")
                schema_sql = schema_sql.replace("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS ")
                schema_sql = schema_sql.replace("__TABLE_ALREADY__", "CREATE TABLE IF NOT EXISTS ")
                schema_sql = schema_sql.replace("CREATE INDEX IF NOT EXISTS ", "__INDEX_ALREADY__")
                schema_sql = schema_sql.replace("CREATE INDEX ", "CREATE INDEX IF NOT EXISTS ")
                schema_sql = schema_sql.replace("__INDEX_ALREADY__", "CREATE INDEX IF NOT EXISTS ")
                await conn.execute(schema_sql)

                # Always run the seed script to keep prompts in sync with seed_prompts.sql
                seed_sql = SEED_PROMPTS_FILE.read_text(encoding="utf-8")
                await conn.execute(seed_sql)

                # Always run the themes seed script to seed initial approved taxonomies
                seed_themes_sql = SEED_THEMES_FILE.read_text(encoding="utf-8")
                await conn.execute(seed_themes_sql)

                logger.info("Database schema initialized successfully.")
            finally:
                await conn.execute("SELECT pg_advisory_unlock(727384910)")

    async def connect(self) -> None:
        """
        Creates the asyncpg connection pool if not already initialized.
        """
        if self.pool:
            return

        async with self._connect_lock:
            if self.pool:
                return
            try:
                self.pool = await asyncpg.create_pool(
                    dsn=settings.DATABASE_URL,
                    min_size=settings.DATABASE_POOL_MIN_SIZE,
                    max_size=settings.DATABASE_POOL_MAX_SIZE
                )
                await self.initialize_schema()
                logger.info("Database connection pool established successfully.")
            except Exception as e:
                logger.error(f"Failed to create database connection pool: {e}")
                raise

    async def disconnect(self) -> None:
        """
        Closes the database connection pool.
        """
        if self.pool:
            await self.pool.close()
            self.pool = None
            logger.info("Database connection pool closed.")

    async def get_connection(self) -> asyncpg.Connection:
        """
        Acquires a connection from the pool. Ensures the pool is connected.
        """
        if not self.pool:
            await self.connect()
        return await self.pool.acquire()

    async def release_connection(self, conn: asyncpg.Connection) -> None:
        """
        Releases a connection back to the pool.
        """
        if self.pool and conn:
            await self.pool.release(conn)

db = Database()
