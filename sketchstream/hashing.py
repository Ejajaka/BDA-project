from __future__ import annotations

import hashlib
from typing import Union

# Check whether the optional xxhash library is installed.
# xxhash is generally faster than the built-in hashlib alternative.
try:
    import xxhash as _xxhash
    _HAVE_XXHASH = True
except Exception:
    # If xxhash is unavailable, use hashlib.blake2b instead.
    _xxhash = None
    _HAVE_XXHASH = False


# A key can be a string, bytes, bytearray, or non-negative integer.
Key = Union[str, bytes, bytearray, int]

# Masks used to keep values within 64-bit and 128-bit limits.
# A mask of all 1s keeps only the specified number of lowest bits.
_MASK64 = (1 << 64) - 1
_MASK128 = (1 << 128) - 1

# A hexadecimal constant used to derive a different seed
# for the second hash in the xxhash implementation.
_GOLDEN = 0x9E3779B97F4A7C15


def _to_bytes(key: Key) -> bytes:
    """Convert the input key into bytes before hashing."""

    # If the key is already bytes, return it unchanged.
    if isinstance(key, bytes):
        return key

    # Convert a mutable bytearray into immutable bytes.
    if isinstance(key, bytearray):
        return bytes(key)

    # Encode strings as UTF-8 bytes.
    # Example: "ABC" becomes b"ABC".
    if isinstance(key, str):
        return key.encode("utf-8")

    # Convert non-negative integers into bytes.
    if isinstance(key, int):
        # Negative integers are not supported.
        if key < 0:
            raise ValueError("negative integers cannot be hashed")

        # Calculate how many bytes are needed to represent the integer.
        # Each byte holds 8 bits; use at least one byte for zero.
        length = (key.bit_length() + 7) // 8 or 1

        # Convert the integer to unsigned, little-endian bytes.
        return key.to_bytes(length, "little", signed=False)

    # Fallback for unexpected input types:
    # convert the object to a string and then to UTF-8 bytes.
    return str(key).encode("utf-8")


def _salt(seed: int) -> bytes:
    """Generate a deterministic 16-byte salt from the seed."""

    # Keep only the lowest 64 bits of the seed.
    # Convert the seed into exactly 8 bytes using little-endian order.
    seed_bytes = (seed & _MASK64).to_bytes(
        8, "little", signed=False
    )

    # Hash the seed bytes with BLAKE2b.
    # digest_size=16 requests a 16-byte (128-bit) result.
    # The resulting bytes are used as the salt for the fallback hash.
    return hashlib.blake2b(
        seed_bytes, digest_size=16
    ).digest()


def hash128(key: Key, seed: int = 0) -> int:
    """Return a deterministic 128-bit hash of the input key."""

    # Convert the input key into bytes before hashing.
    data = _to_bytes(key)

    # Fast path: use xxhash if the library is installed.
    if _HAVE_XXHASH:

        # Generate the first 64-bit hash using the supplied seed.
        h1 = int(
            _xxhash.xxh64(
                data, seed=seed & _MASK64
            ).intdigest()
        )

        # Generate a second 64-bit hash using a different seed.
        # XOR with GOLDEN produces a different seed value.
        h2 = int(
            _xxhash.xxh64(
                data,
                seed=(seed ^ _GOLDEN) & _MASK64
            ).intdigest()
        )

        # Shift h1 into the upper 64 bits.
        # Place h2 in the lower 64 bits using bitwise OR.
        # The result combines both hashes into one 128-bit integer.
        return ((h1 << 64) | h2) & _MASK128

    # Fallback path: use BLAKE2b if xxhash is unavailable.
    # The seed-dependent salt makes the result depend on the seed.
    # digest_size=16 produces 128 bits (16 bytes).
    digest = hashlib.blake2b(
        data,
        digest_size=16,
        salt=_salt(seed)
    ).digest()

    # Convert the 16-byte digest into an integer.
    # "little" means the least significant byte comes first.
    return int.from_bytes(digest, "little")


def hash64(key: Key, seed: int = 0) -> int:
    """Return a deterministic 64-bit hash of the input key."""

    # If xxhash is available, directly calculate a 64-bit hash.
    if _HAVE_XXHASH:
        return int(
            _xxhash.xxh64(
                _to_bytes(key), seed=seed & _MASK64
            ).intdigest()
        )

    # Otherwise, calculate a 128-bit hash and keep only
    # its lowest 64 bits using the 64-bit mask.
    return hash128(key, seed) & _MASK64


def double_hashing(
    key: Key, i: int, width: int, seed: int = 0
) -> int:
    """Calculate a column index for a Count-Min Sketch row."""

    # A sketch must have a positive number of columns.
    if width <= 0:
        raise ValueError("width must be positive")

    # Calculate one 128-bit hash for the input key.
    h = hash128(key, seed)

    # Extract the lower 64 bits as the starting hash value.
    h1 = h & _MASK64

    # Extract the upper 64 bits as the step size.
    # OR with 1 forces the step size to be odd.
    # Masking keeps it within 64 bits.
    h2 = ((h >> 64) | 1) & _MASK64

    # Double-hashing formula:
    # index = (starting value + row number * step size) % width
    #
    # The modulo operation ensures the index is between
    # 0 and width - 1, making it a valid sketch column.
    #
    # This allows multiple row indices to be generated
    # from one 128-bit hash instead of hashing separately per row.
    return (h1 + i * h2) % width


# Specify the main functions available for wildcard imports:
# from module_name import *
__all__ = ["hash128", "hash64", "double_hashing"]
