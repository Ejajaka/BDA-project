"""Stable, reproducible hashing shared by every sketch.

Python's built-in ``hash()`` is salted per-process (PYTHONHASHSEED), so it is
unusable when we need reproducible sketches across runs / nodes. We therefore
use:

1. ``xxhash`` when available (fast), else
2. ``hashlib.blake2b`` (always available, deterministic).

All hashing paths are deterministic given ``(key, seed)``.
"""
from __future__ import annotations

import hashlib
from typing import Union

try:  # optional acceleration
    import xxhash as _xxhash  # type: ignore

    _HAVE_XXHASH = True
except Exception:  # pragma: no cover - depends on environment
    _xxhash = None  # type: ignore
    _HAVE_XXHASH = False

Key = Union[str, bytes, bytearray, int]

_MASK64 = (1 << 64) - 1
_MASK128 = (1 << 128) - 1
_GOLDEN = 0x9E3779B97F4A7C15


def _to_bytes(key: Key) -> bytes:
    if isinstance(key, bytes):
        return key
    if isinstance(key, bytearray):
        return bytes(key)
    if isinstance(key, str):
        return key.encode("utf-8")
    if isinstance(key, int):
        if key < 0:
            raise ValueError("negative integers cannot be hashed")
        length = (key.bit_length() + 7) // 8 or 1
        return key.to_bytes(length, "little", signed=False)
    return str(key).encode("utf-8")


def _salt(seed: int) -> bytes:
    # blake2b salt must be exactly 16 bytes.
    return hashlib.blake2b(
        (seed & _MASK64).to_bytes(8, "little", signed=False), digest_size=16
    ).digest()


def hash128(key: Key, seed: int = 0) -> int:
    """Return a deterministic 128-bit integer hash of ``key``."""
    data = _to_bytes(key)
    if _HAVE_XXHASH:
        h1 = int(_xxhash.xxh64(data, seed=seed & _MASK64).intdigest())
        h2 = int(_xxhash.xxh64(data, seed=(seed ^ _GOLDEN) & _MASK64).intdigest())
        return ((h1 << 64) | h2) & _MASK128
    digest = hashlib.blake2b(data, digest_size=16, salt=_salt(seed)).digest()
    return int.from_bytes(digest, "little")


def hash64(key: Key, seed: int = 0) -> int:
    """Return a deterministic 64-bit integer hash of ``key``."""
    if _HAVE_XXHASH:
        return int(_xxhash.xxh64(_to_bytes(key), seed=seed & _MASK64).intdigest())
    return hash128(key, seed) & _MASK64


def double_hashing(key: Key, i: int, width: int, seed: int = 0) -> int:
    """Enhanced double hashing (Kirsch-Mitzenmacher).

    Derives ``depth`` independent-ish indices from a single 128-bit hash:
    ``idx_i = (h1 + i * h2) mod width`` with ``h2`` forced odd.
    """
    if width <= 0:
        raise ValueError("width must be positive")
    h = hash128(key, seed)
    h1 = h & _MASK64
    h2 = ((h >> 64) | 1) & _MASK64
    return (h1 + i * h2) % width
