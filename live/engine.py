"""
Unit Tests for Storage Drivers
Location: tests/unit/core/data/storage/test_storage.py
Desc: Menguji InMemoryStorage dan RedisStorage menggunakan Result pattern.
      Untuk Redis, menggunakan mock redis.asyncio.Redis.
"""

import pytest
import asyncio
import json
from unittest.mock import AsyncMock, patch

from core.shared.result import is_ok, is_err
from core.data.storage.drivers.memory import InMemoryStorage
from core.data.storage.drivers.redis_driver import RedisStorage
from core.data.storage import (
    create_memory_storage,
    create_redis_storage,
    create_redis_storage_and_test,
)


# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture
def memory_storage():
    """Fixture untuk InMemoryStorage."""
    return InMemoryStorage()


@pytest.fixture
def mock_redis():
    """Mock redis.asyncio.Redis dengan AsyncMock."""
    mock = AsyncMock()
    # Konfigurasi default untuk method yang umum
    mock.ping.return_value = True
    mock.xadd.return_value = "123-0"
    mock.xrange.return_value = []
    mock.xtrim.return_value = 0
    mock.publish.return_value = 1
    mock.set.return_value = True
    mock.setex.return_value = True
    mock.get.return_value = None
    mock.delete.return_value = 1
    mock.expire.return_value = True
    mock.flushall.return_value = True
    return mock


@pytest.fixture
def redis_storage(mock_redis):
    """Fixture untuk RedisStorage dengan mock redis."""
    with patch('redis.asyncio.Redis', return_value=mock_redis):
        storage = RedisStorage(host='localhost', port=6379, db=0)
        # Biarkan _redis tetap None, akan dibuat oleh _ensure_connection
        yield storage


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

async def assert_ok(result, expected_value=None):
    """Helper untuk memeriksa Result Ok dan nilai opsional."""
    assert is_ok(result)
    if expected_value is not None:
        assert result.unwrap() == expected_value


async def assert_err(result, error_substring=None):
    """Helper untuk memeriksa Result Err."""
    assert is_err(result)
    if error_substring:
        err_msg = result.unwrap_err()
        assert error_substring in err_msg


# =============================================================================
# IN-MEMORY STORAGE TESTS
# =============================================================================

class TestInMemoryStorage:
    """Test suite untuk InMemoryStorage."""

    @pytest.mark.asyncio
    async def test_stream_add_and_read(self, memory_storage):
        """Test menambah dan membaca stream."""
        # Add entry
        res = await memory_storage.stream_add("test_stream", {"price": 100, "vol": 10})
        await assert_ok(res)
        entry_id = res.unwrap()
        assert isinstance(entry_id, str)

        # Read all
        res = await memory_storage.stream_read("test_stream")
        await assert_ok(res)
        entries = res.unwrap()
        assert len(entries) == 1
        assert entries[0][0] == entry_id
        assert entries[0][1] == {"price": 100, "vol": 10}

        # Read with count
        res = await memory_storage.stream_read("test_stream", count=1)
        await assert_ok(res)
        assert len(res.unwrap()) == 1

        # Read empty stream
        res = await memory_storage.stream_read("nonexistent")
        await assert_ok(res)
        assert res.unwrap() == []

    @pytest.mark.asyncio
    async def test_stream_trim(self, memory_storage):
        """Test trim stream."""
        for i in range(10):
            await memory_storage.stream_add("test_stream", {"i": i})
        res = await memory_storage.stream_trim("test_stream", maxlen=5)
        await assert_ok(res, 5)  # 5 entries deleted

        res = await memory_storage.stream_read("test_stream")
        entries = res.unwrap()
        assert len(entries) == 5
        # entries are oldest first? In our simple impl, we append, and trim from beginning.
        # So remaining are last 5 entries, with i starting from 5.
        assert entries[0][1]["i"] == 5

        # Trim to larger than current
        res = await memory_storage.stream_trim("test_stream", maxlen=10)
        await assert_ok(res, 0)

    @pytest.mark.asyncio
    async def test_pubsub(self, memory_storage):
        """Test publish dan subscribe."""
        messages = []

        async def subscriber():
            async for channel, msg in memory_storage.subscribe("test_channel"):
                messages.append((channel, msg))
                if len(messages) >= 2:
                    break

        task = asyncio.create_task(subscriber())
        await asyncio.sleep(0.05)  # biarkan subscriber terdaftar

        res = await memory_storage.publish("test_channel", "hello")
        await assert_ok(res, 1)

        res = await memory_storage.publish("test_channel", "world")
        await assert_ok(res, 1)

        await task

        assert len(messages) == 2
        assert messages[0] == ("test_channel", "hello")
        assert messages[1] == ("test_channel", "world")

    @pytest.mark.asyncio
    async def test_kv_set_get_delete(self, memory_storage):
        """Test key-value operations."""
        res = await memory_storage.set("key1", {"value": 123})
        await assert_ok(res, None)

        res = await memory_storage.get("key1")
        await assert_ok(res, {"value": 123})

        res = await memory_storage.delete("key1")
        await assert_ok(res, True)

        res = await memory_storage.get("key1")
        await assert_ok(res, None)

        res = await memory_storage.delete("key2")
        await assert_ok(res, False)

    @pytest.mark.asyncio
    async def test_set_with_ttl_and_expire(self, memory_storage):
        """Test TTL dan expire."""
        res = await memory_storage.set_with_ttl("temp", "value", 1)
        await assert_ok(res)

        res = await memory_storage.get("temp")
        await assert_ok(res, "value")

        res = await memory_storage.expire("temp", 2)
        await assert_ok(res, True)

        await asyncio.sleep(1.5)
        res = await memory_storage.get("temp")
        await assert_ok(res, "value")

        await asyncio.sleep(1.0)
        res = await memory_storage.get("temp")
        await assert_ok(res, None)

        res = await memory_storage.expire("nonexistent", 10)
        await assert_ok(res, False)

    @pytest.mark.asyncio
    async def test_health_check(self, memory_storage):
        """Health check selalu True."""
        res = await memory_storage.health_check()
        await assert_ok(res, True)

    @pytest.mark.asyncio
    async def test_flushall(self, memory_storage):
        """Flushall membersihkan semua data."""
        await memory_storage.set("a", 1)
        await memory_storage.stream_add("s", {"x": 1})

        await memory_storage.flushall()

        res = await memory_storage.get("a")
        await assert_ok(res, None)

        res = await memory_storage.stream_read("s")
        await assert_ok(res, [])


# =============================================================================
# REDIS STORAGE TESTS (with mocks)
# =============================================================================

class TestRedisStorage:
    """Test suite untuk RedisStorage dengan mock redis."""

    @pytest.mark.asyncio
    async def test_ensure_connection_success(self, redis_storage, mock_redis):
        """Test koneksi sukses."""
        res = await redis_storage._ensure_connection()
        await assert_ok(res)
        mock_redis.ping.assert_called_once()
        assert redis_storage._redis is mock_redis

    @pytest.mark.asyncio
    async def test_ensure_connection_failure(self, mock_redis):
        """Test koneksi gagal."""
        mock_redis.ping.side_effect = ConnectionError("Cannot connect")
        with patch('redis.asyncio.Redis', return_value=mock_redis):
            storage = RedisStorage()
            res = await storage._ensure_connection()
            await assert_err(res, "Cannot connect")

    @pytest.mark.asyncio
    async def test_stream_add(self, redis_storage, mock_redis):
        """Test stream_add sukses."""
        mock_redis.xadd.return_value = "123456789-0"
        res = await redis_storage.stream_add("mystream", {"price": 100, "vol": 10})
        await assert_ok(res, "123456789-0")
        # Ping dipanggil sekali karena koneksi pertama
        mock_redis.ping.assert_called_once()
        mock_redis.xadd.assert_called_once_with(
            "mystream",
            {"price": "100", "vol": "10"},
            maxlen=None,
            approximate=False
        )

    @pytest.mark.asyncio
    async def test_stream_add_with_maxlen(self, redis_storage, mock_redis):
        """Test stream_add dengan maxlen."""
        # Reset mock agar tidak terpengaruh ping dari test sebelumnya
        mock_redis.reset_mock()
        mock_redis.xadd.return_value = "id"
        res = await redis_storage.stream_add("s", {"a": 1}, maxlen=100, approximate=True)
        await assert_ok(res, "id")
        mock_redis.xadd.assert_called_once_with(
            "s", {"a": "1"}, maxlen=100, approximate=True
        )

    @pytest.mark.asyncio
    async def test_stream_read(self, redis_storage, mock_redis):
        """Test stream_read."""
        mock_redis.reset_mock()
        mock_redis.xrange.return_value = [
            ("1-0", {"price": "100", "vol": "10"}),
            ("2-0", {"price": "101", "vol": "11"}),
        ]
        res = await redis_storage.stream_read("s", start="-", end="+", count=2)
        await assert_ok(res)
        entries = res.unwrap()
        assert len(entries) == 2
        assert entries[0][0] == "1-0"
        assert entries[0][1] == {"price": 100, "vol": 10}
        assert entries[1][1] == {"price": 101, "vol": 11}
        mock_redis.xrange.assert_called_once_with("s", "-", "+", count=2)

    @pytest.mark.asyncio
    async def test_stream_trim(self, redis_storage, mock_redis):
        """Test stream_trim."""
        mock_redis.reset_mock()
        mock_redis.xtrim.return_value = 5
        res = await redis_storage.stream_trim("s", 10, approximate=True)
        await assert_ok(res, 5)
        mock_redis.xtrim.assert_called_once_with("s", 10, approximate=True)

    @pytest.mark.asyncio
    async def test_publish(self, redis_storage, mock_redis):
        """Test publish."""
        mock_redis.reset_mock()
        mock_redis.publish.return_value = 2
        res = await redis_storage.publish("channel", "message")
        await assert_ok(res, 2)
        mock_redis.publish.assert_called_once_with("channel", "message")

    @pytest.mark.asyncio
    async def test_subscribe(self, redis_storage, mock_redis):
        """Test subscribe (mocked pubsub)."""
        # Set _redis manual agar tidak perlu koneksi
        redis_storage._redis = mock_redis
        mock_pubsub = AsyncMock()
        mock_pubsub.subscribe.return_value = None
        mock_pubsub.listen.return_value.__aiter__.return_value = [
            {"type": "message", "channel": "ch", "data": "msg1"},
            {"type": "message", "channel": "ch", "data": "msg2"},
        ]
        mock_pubsub.unsubscribe.return_value = None
        mock_pubsub.close.return_value = None

        mock_redis.pubsub.return_value = mock_pubsub

        messages = []
        async for channel, msg in redis_storage.subscribe("ch"):
            messages.append((channel, msg))
            if len(messages) >= 2:
                break

        assert messages == [("ch", "msg1"), ("ch", "msg2")]
        mock_pubsub.subscribe.assert_called_once_with("ch")
        mock_pubsub.unsubscribe.assert_called_once_with("ch")
        mock_pubsub.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_set(self, redis_storage, mock_redis):
        """Test set."""
        mock_redis.reset_mock()
        res = await redis_storage.set("key", {"nested": [1, 2]})
        await assert_ok(res, None)
        mock_redis.set.assert_called_once_with("key", json.dumps({"nested": [1, 2]}))

    @pytest.mark.asyncio
    async def test_set_with_ttl(self, redis_storage, mock_redis):
        """Test set_with_ttl."""
        mock_redis.reset_mock()
        res = await redis_storage.set_with_ttl("key", "value", 60)
        await assert_ok(res, None)
        mock_redis.setex.assert_called_once_with("key", 60, json.dumps("value"))

    @pytest.mark.asyncio
    async def test_get(self, redis_storage, mock_redis):
        """Test get."""
        mock_redis.reset_mock()
        mock_redis.get.return_value = json.dumps({"data": 123})
        res = await redis_storage.get("key")
        await assert_ok(res, {"data": 123})
        mock_redis.get.assert_called_once_with("key")

        mock_redis.get.return_value = None
        res = await redis_storage.get("nonexistent")
        await assert_ok(res, None)

        mock_redis.get.return_value = "plain string"
        res = await redis_storage.get("key")
        await assert_ok(res, "plain string")

    @pytest.mark.asyncio
    async def test_delete(self, redis_storage, mock_redis):
        """Test delete."""
        mock_redis.reset_mock()
        mock_redis.delete.return_value = 1
        res = await redis_storage.delete("key")
        await assert_ok(res, True)
        mock_redis.delete.assert_called_once_with("key")

        mock_redis.delete.return_value = 0
        res = await redis_storage.delete("key")
        await assert_ok(res, False)

    @pytest.mark.asyncio
    async def test_expire(self, redis_storage, mock_redis):
        """Test expire."""
        mock_redis.reset_mock()
        mock_redis.expire.return_value = True
        res = await redis_storage.expire("key", 30)
        await assert_ok(res, True)
        mock_redis.expire.assert_called_once_with("key", 30)

        mock_redis.expire.return_value = False
        res = await redis_storage.expire("nonexistent", 30)
        await assert_ok(res, False)

    @pytest.mark.asyncio
    async def test_health_check(self, redis_storage, mock_redis):
        """Test health_check."""
        mock_redis.reset_mock()
        res = await redis_storage.health_check()
        await assert_ok(res, True)
        mock_redis.ping.assert_called_once()

    @pytest.mark.asyncio
    async def test_flushall(self, redis_storage, mock_redis):
        """Test flushall."""
        mock_redis.reset_mock()
        res = await redis_storage.flushall()
        await assert_ok(res, None)
        mock_redis.flushall.assert_called_once()


# =============================================================================
# FACTORY TESTS
# =============================================================================

class TestStorageFactory:
    """Test factory functions."""

    def test_create_memory_storage(self):
        """Factory memory selalu sukses."""
        res = create_memory_storage()
        assert is_ok(res)
        storage = res.unwrap()
        assert isinstance(storage, InMemoryStorage)

    def test_create_redis_storage(self, mock_redis):
        """Factory redis tanpa test koneksi."""
        with patch('redis.asyncio.Redis', return_value=mock_redis):
            res = create_redis_storage(host="test")
            assert is_ok(res)
            storage = res.unwrap()
            assert isinstance(storage, RedisStorage)
            # Tidak perlu test koneksi

    @pytest.mark.asyncio
    async def test_create_redis_storage_and_test_success(self, mock_redis):
        """Factory dengan test koneksi sukses."""
        with patch('redis.asyncio.Redis', return_value=mock_redis):
            res = await create_redis_storage_and_test(host="test")
            assert is_ok(res)
            storage = res.unwrap()
            mock_redis.ping.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_redis_storage_and_test_failure(self, mock_redis):
        """Factory dengan test koneksi gagal."""
        mock_redis.ping.side_effect = ConnectionError("fail")
        with patch('redis.asyncio.Redis', return_value=mock_redis):
            res = await create_redis_storage_and_test(host="test")
            assert is_err(res)
            assert "Redis health check failed" in res.unwrap_err()
