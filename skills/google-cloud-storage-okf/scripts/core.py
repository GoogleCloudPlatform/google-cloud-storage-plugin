"""Core logic for GCS-OKF navigation and chunk fetching.

OKF Region Concept & Metadata Architecture:
--------------------------------------------
A **Region** is a contiguous, semantically cohesive segment of a GCS object
(defined by inclusive `start_byte` and `end_byte` offsets, a descriptive
`title`, and relevant `keywords`). Regions act as a Table of Contents index
stored in GCS metadata, allowing LLMs to perform targeted byte-range reads
(`expand`) without loading or processing the full file.

1. Self-Contained Region Index Encoding Architecture:
   - The underlying GCS object data remains completely untouched—we are NOT
     splitting or creating separate GCS files. Instead, each region index
     descriptor (a JSON object containing byte boundaries, titles, and keywords)
     is individually serialized to compact JSON, Base64-encoded, and
     saved to a dedicated GCS Object Context key (`okf.r.000`, `okf.r.001`, ...)
     on the original blob itself.
   - **Airtight Semantic Truncation (Pre-Encoding)**: Truncation is never
     applied to raw encoded byte streams (which would corrupt the JSON).
     Instead, if the encoded payload exceeds the 250-byte safe threshold,
     fallback truncation trims the underlying Python dictionary fields
     (keywords, title length, id) *before* re-encoding, strictly guaranteeing
     that the resulting Base64 string is <= 256 bytes while remaining a 100%
     valid JSON object.
   - **Per-Key Encoding & Integrity Validation**: Encoding each region in its
     own context key avoids multi-key JSON splicing, while any missing or
     corrupted `okf.r.*` key invalidates the index across read tools until
     repaired via `write_okf()`.

2. Staleness Detection, GCS Versioning, & Defense-in-Depth Model:
   - **GCS Versioning & Generation Semantics**: In Google Cloud Storage, objects
     and Object Contexts (`custom_contexts`) are strictly bound to an immutable
     64-bit server generation timestamp (`generation`). When an object is
     overwritten, GCS creates a new generation with empty Object Contexts. The
     live object returns `OKF_NOT_FOUND`, prompting a clean re-indexing pass
     rather than borrowing misaligned byte offsets from older versions.
   - **When OKF Indices Become Stale**:
     1. *ETL & Metadata-Copy Pipelines*: Ingestion scripts copying custom
        metadata forward from v1 to v2 without re-indexing byte boundaries.
     2. *Cross-Bucket Copies, Moves, & STS Transfers*: Operations like `gcloud
        storage cp/mv` or Storage Transfer Service (STS) replicate
        metadata/contexts by default, but GCS assigns a brand new generation in
        the target bucket. The copied `okf.src_generation` references the source
        bucket generation, alerting readers to verify/re-stamp provenance.
     3. *Asynchronous Indexing Race Conditions*: An out-of-band indexing worker
        spends 10–30s computing regions for generation G1, but another process
        overwrites the blob to generation G2 before the index is patched.
   - **Defense-in-Depth Protection**:
     - *Write-Time Atomic OCC Guard*: `write_okf` passes
       `if_generation_match=blob.generation` to `blob.patch()`, leveraging GCS
       server-side Optimistic Concurrency Control (OCC) to atomically reject
       writes with `412 Precondition Failed` if the object changed during index
       generation.
     - *Read-Time Provenance Guard*: `peek` and `list_regions` execute
       `_check_staleness_warning` to verify that `blob.generation` matches
       `okf.src_generation`, warning downstream agents before they invoke
       `expand` on potentially shifted byte ranges.

3. Capacity & Key Count Limits:
   - Enforces GCS custom context budget limits: max 50 context keys and 25,600
     total bytes per object.
   - Deducts pre-existing non-OKF object contexts from available key count and
     byte budget before writing.
   - On missing OKF during read (`peek`/`list_regions`) or capacity overflow on
     `write_okf`, returns detailed diagnostic errors providing exact remaining
     storage space, key slots, and existing context text to help LLMs determine
     whether OKF index generation is viable.
"""

import base64
import codecs
from collections.abc import Mapping, Sequence
import datetime
import functools
import gzip
import importlib
import io
import json
import os
from typing import Any
import unicodedata
import zlib

from google.api_core import client_info
from google.api_core import exceptions as gcs_exceptions
from google.auth import exceptions as auth_exceptions
from google.cloud import storage

# GCS Object Contexts Dual-SDK Compatibility:
# On newer SDK releases, ObjectContexts and ObjectCustomContextPayload are
# exported at storage.blob or generated proto types. On older/standard releases
# or external environments, we provide lightweight shims to prevent startup
# import crashes and fall back to REST JSON dictionary manipulation for read
# and write.
ObjectContexts = getattr(storage.blob, "ObjectContexts", None)
ObjectCustomContextPayload = getattr(
    storage.blob, "ObjectCustomContextPayload", None
)
if ObjectContexts is None or ObjectCustomContextPayload is None:
  try:
    storage_v2_types = importlib.import_module(
        "google.cloud._storage_v2.types.storage"
    )
    if ObjectContexts is None:
      ObjectContexts = getattr(storage_v2_types, "ObjectContexts", None)
    if ObjectCustomContextPayload is None:
      ObjectCustomContextPayload = getattr(
          storage_v2_types, "ObjectCustomContextPayload", None
      )
  except Exception:  # pylint: disable=broad-exception-caught
    pass

if ObjectContexts is None:

  class ObjectContexts:
    """Container shim for an object's contexts when SDK types are unavailable."""

    def __init__(self, custom: Any = None):
      self.custom = custom if custom is not None else {}


if ObjectCustomContextPayload is None:

  class ObjectCustomContextPayload:
    """Payload shim for custom context value when SDK types are unavailable."""

    def __init__(self, value: str = ""):
      self.value = value


# pylint: enable=g-import-not-at-top

SKILL_USER_AGENT = "gcs-skills/1.0 (skill:google-cloud-storage-okf)"
CLIENT_INFO = client_info.ClientInfo(user_agent=SKILL_USER_AGENT)

_FIX_CREDENTIALS = "Run: gcloud auth application-default login"
_FIX_PROJECT = (
    "No Google Cloud project is configured. Run: gcloud config set project"
    " <project-id>, or set GOOGLE_CLOUD_PROJECT in this server's environment."
)
_FIX_PERMISSION_READ = (
    "The authenticated principal needs read access to the object"
    " (storage.objects.get)."
)
_FIX_PERMISSION_WRITE = (
    "The authenticated principal needs storage.objects.update on the object"
    " to write OKF metadata."
)
_FIX_QUOTA_PROJECT = (
    "The ADC quota project denies serviceusage.services.use. Run:"
    " gcloud auth application-default login --disable-quota-project, or"
    " gcloud auth application-default set-quota-project <project-id>."
)


GCS_CONTEXT_MAX_VALUE_BYTES = 256
GCS_CONTEXT_SAFE_VALUE_LIMIT = 250
GCS_CONTEXT_MAX_TOTAL_BYTES = 25600
GCS_CONTEXT_MAX_KEYS = 50
OKF_SUMMARY_CHUNK_SIZE = 200
OKF_KEYWORD_TRUNCATION_LIMIT = 3
OKF_TITLE_TRUNCATION_LIMIT = 50
STALE_INDEX_PREFIX = "Stale OKF index:"
BINARY_BASE64_PREFIX = "[Base64-encoded binary data]\n"
# Leading bytes of a gzip stream. Regions indexed before gzip was removed are
# still gzip-compressed, so `list_regions` decompresses them for compatibility.
_GZIP_MAGIC = b"\x1f\x8b"
_MAX_DECODED_REGION_BYTES = 8 * 1024
_FORBIDDEN_CONTEXT_CHARS = frozenset({"'", '"', "\\", "/"})
_GZIP_RAW_READ_CHUNK_BYTES = 256 * 1024
_MAX_GZIP_RAW_SCAN_BYTES = 64 * 1024 * 1024
_MAX_GZIP_DECOMPRESSED_SCAN_BYTES = 256 * 1024 * 1024
_MAX_GZIP_SLICE_BYTES = 16 * 1024 * 1024


def _check_staleness_warning(
    blob: storage.Blob, contexts: Mapping[str, str]
) -> str | None:
  """Checks if stored OKF provenance matches current blob generation.

  Detects index staleness across three critical scenarios:
    1. ETL/Ingestion pipelines copying custom metadata from previous versions.
    2. Bucket moves, cross-bucket copies, or STS transfers where GCS assigns a
       new destination generation while preserving existing metadata.
    3. Concurrent overwrite race conditions where the object was replaced during
       asynchronous OKF index computation.

  Args:
    blob: The target GCS Blob instance.
    contexts: The dictionary of GCS Object Contexts.

  Returns:
    A descriptive staleness warning string if generation mismatch is detected,
    or None if the index is fresh and matching.
  """
  stored_gen = contexts.get("okf.src_generation")
  curr_gen = getattr(blob, "generation", None)
  if stored_gen and curr_gen is not None and str(curr_gen) != str(stored_gen):
    return (
        f"{STALE_INDEX_PREFIX} Index created for object generation"
        f" {stored_gen}, but current generation is {curr_gen}."
    )
  return None


def _parse_gs_uri(gs_uri: str) -> tuple[str, str]:
  """Parses a GCS URI of the form 'gs://bucket/blob_name'.

  Args:
    gs_uri: The string URI to parse.

  Returns:
    A tuple of (bucket_name, blob_name).

  Raises:
    ValueError: If the URI format is invalid.
  """
  if not gs_uri or not isinstance(gs_uri, str):
    raise ValueError(
        f"Invalid URI: Expected a non-empty string, got {type(gs_uri).__name__}"
    )
  if not gs_uri.startswith("gs://"):
    raise ValueError(
        f"Invalid URI: Must start with 'gs://'. Provided: '{gs_uri}'"
    )
  path = gs_uri[5:]
  if not path:
    raise ValueError(f"Invalid URI: Missing bucket name in '{gs_uri}'")
  parts = path.split("/", 1)
  bucket = parts[0]
  if not bucket:
    raise ValueError(f"Invalid URI: Missing bucket name in '{gs_uri}'")
  blob_name = parts[1] if len(parts) > 1 else ""
  if not blob_name:
    raise ValueError(f"Invalid URI: Missing object path in '{gs_uri}'")
  return bucket, blob_name


def _format_gcs_error(
    gs_uri: str, error: Exception, *, writing: bool = False
) -> str:
  """Formats GCS errors into human- and LLM-friendly diagnostic messages.

  Args:
    gs_uri: The URI the failed operation targeted.
    error: The exception raised by the GCS client.
    writing: Whether the operation was writing Object Context, which changes the
      permission the caller needs.

  Returns:
    The message to return from the tool, with a `Fix:` line when the error
    means the caller's environment or permissions are not set up.
  """
  if isinstance(error, ValueError):
    return str(error)
  if isinstance(error, gcs_exceptions.NotFound):
    msg = error.message if hasattr(error, "message") else str(error)
    return f"GCS Resource Not Found: {msg}"
  if isinstance(error, gcs_exceptions.PreconditionFailed):
    msg = error.message if hasattr(error, "message") else str(error)
    return (
        f"Precondition Failed for '{gs_uri}': Object generation condition"
        f" failed (concurrent modification): {msg}"
    )
  if isinstance(error, gcs_exceptions.Unauthorized):
    if hasattr(_get_storage_client, "cache_clear"):
      _get_storage_client.cache_clear()
    return (
        f"Unauthorized accessing '{gs_uri}': {str(error)}\n"
        f"Fix: Credentials were rejected. {_FIX_CREDENTIALS}"
    )
  if isinstance(error, gcs_exceptions.Forbidden):
    if "serviceusage.services.use" in str(error):
      if hasattr(_get_storage_client, "cache_clear"):
        _get_storage_client.cache_clear()
      fix = _FIX_QUOTA_PROJECT
    else:
      fix = _FIX_PERMISSION_WRITE if writing else _FIX_PERMISSION_READ
    return f"Permission Denied accessing '{gs_uri}': {str(error)}\nFix: {fix}"
  fix = _environment_fix(error)
  if fix:
    return f"Environment not set up for '{gs_uri}': {str(error)}\nFix: {fix}"
  operation = "writing Object Context to" if writing else "accessing GCS for"
  return f"Error {operation} '{gs_uri}': {str(error)}"


def _environment_fix(error: Exception) -> str | None:
  """Returns the remedy when `error` means the caller's environment is not set up."""
  if isinstance(error, auth_exceptions.RefreshError):
    if hasattr(_get_storage_client, "cache_clear"):
      _get_storage_client.cache_clear()
    return f"Credentials expired or were revoked. {_FIX_CREDENTIALS}"
  if isinstance(error, auth_exceptions.DefaultCredentialsError):
    return f"Application Default Credentials not found. {_FIX_CREDENTIALS}"
  # google-cloud-core raises a bare EnvironmentError when no project can be
  # resolved from the environment.
  if isinstance(error, OSError) and "project" in str(error).lower():
    return _FIX_PROJECT
  return None


def _is_gcs_alnum_start(text: str) -> bool:
  """Returns whether `text` begins with a GCS-valid Unicode letter or digit."""
  return bool(text) and unicodedata.category(text[0])[0] in ("L", "N")


def _is_interchange_valid_char(ch: str) -> bool:
  """Returns whether `ch` passes GCS `UniLib::IsInterchangeValid` rules."""
  cp = ord(ch)
  if cp <= 0x1F and ch not in ("\t", "\n", "\x0c", "\r"):
    return False
  if 0x7F <= cp <= 0x9F:
    return False
  if 0xFDD0 <= cp <= 0xFDEF or (cp & 0xFFFE) == 0xFFFE:
    return False
  return True


def _validate_gcs_context_value(value: str, field_name: str) -> str | None:
  """Returns an error string if `value` violates GCS context rules, else None."""
  if not isinstance(value, str) or not value:
    return (
        f"Invalid {field_name}: Must be a non-empty string starting with a"
        " letter or digit."
    )
  try:
    value.encode("utf-8")
  except UnicodeEncodeError:
    return f"Invalid {field_name}: Must be a valid UTF-8 string."
  for ch in value:
    if not _is_interchange_valid_char(ch):
      return (
          f"Invalid {field_name}: Contains disallowed control or noncharacter"
          f" code point {ch!r}."
      )
  if not _is_gcs_alnum_start(value):
    return (
        f"Invalid {field_name}: Must start with an alphanumeric character"
        f" (letter or digit), got {value[0]!r}."
    )
  bad_chars = sorted(ch for ch in _FORBIDDEN_CONTEXT_CHARS if ch in value)
  if bad_chars:
    joined = ", ".join(repr(ch) for ch in bad_chars)
    return (
        f"Invalid {field_name}: GCS Object Context values cannot contain"
        " single quotes ('), double quotes (\"), backslashes (\\), or forward"
        f" slashes (/); found {joined}. Reword without slashes or quotes."
    )
  return None


def _chunk_summary_for_gcs(
    summary: str, max_chunk_bytes: int
) -> list[str] | None:
  """Splits `summary` into <= `max_chunk_bytes` chunks starting with L/N.

  Args:
    summary: Valid UTF-8 summary text string to split across context values.
    max_chunk_bytes: Maximum UTF-8 byte length per chunk (must be >= 4).

  Returns:
    Ordered list of summary chunks where every chunk begins with a Unicode
    letter or digit, or `None` if `summary` is non-empty and cannot be split
    such that every chunk starts with a letter or digit.
  """
  if max_chunk_bytes < 4:
    raise ValueError("max_chunk_bytes must be at least 4.")
  if not summary:
    return []
  if not _is_gcs_alnum_start(summary):
    return None
  chunks: list[str] = []
  idx = 0
  total_chars = len(summary)
  while idx < total_chars:
    size = 0
    end = idx
    last_alnum_split = 0
    while end < total_chars:
      ch_bytes = len(summary[end].encode("utf-8"))
      if size + ch_bytes > max_chunk_bytes:
        break
      size += ch_bytes
      end += 1
      if end < total_chars and _is_gcs_alnum_start(summary[end]):
        last_alnum_split = end
    if end < total_chars and not _is_gcs_alnum_start(summary[end]):
      if not last_alnum_split:
        return None
      end = last_alnum_split
    chunks.append(summary[idx:end])
    idx = end
  return chunks


def _parse_nonneg_ascii_int(value: str) -> int | None:
  """Returns non-negative integer if `value` is ASCII digits, else `None`."""
  if value.isascii() and value.isdigit():
    return int(value)
  return None


def extract_summary_or_error(
    contexts: Mapping[str, str],
) -> tuple[str | None, str | None]:
  """Extracts Tier 0 OKF summary or returns an error reason.

  Args:
    contexts: Mapping of GCS custom object context keys to string values.

  Returns:
    `(summary, None)` when a complete, non-empty, conflict-free summary is
    present, or `(None, error_reason)` when missing, incomplete, or conflicting.
  """
  has_direct = "okf.sum" in contexts
  sum_chunks_str = contexts.get("okf.sum_chunks")
  sum_chunk_keys = sorted(k for k in contexts if k.startswith("okf.sum."))

  if has_direct:
    if sum_chunks_str is not None or sum_chunk_keys:
      return (
          None,
          "Incomplete chunked summary: Conflicting okf.sum and chunked keys",
      )
    direct_summary = contexts["okf.sum"]
    if direct_summary:
      return direct_summary, None
    return None, "Manifest summary missing"

  if sum_chunks_str is not None:
    num_chunks = _parse_nonneg_ascii_int(sum_chunks_str)
    if num_chunks is not None and 0 < num_chunks <= len(contexts):
      expected_keys = [f"okf.sum.{i:03d}" for i in range(num_chunks)]
      if sum_chunk_keys == expected_keys and all(
          contexts[k] for k in expected_keys
      ):
        return "".join(contexts[k] for k in expected_keys), None
    return (
        None,
        (
            f"Incomplete chunked summary: Expected {sum_chunks_str} chunks, but"
            " some chunks are missing"
        ),
    )

  if sum_chunk_keys:
    return (
        None,
        (
            "Incomplete chunked summary: Summary chunks present without"
            " okf.sum_chunks"
        ),
    )
  return None, "Manifest summary missing"


def _decode_region_entry(encoded_region: str) -> dict[str, Any]:
  """Decodes and validates a single Base64 (or legacy Base64+Gzip) region."""
  clean_b64 = encoded_region.replace("-", "+").replace("_", "/")
  if padding := len(clean_b64) % 4:
    clean_b64 += "=" * (4 - padding)
  decoded_bytes = base64.b64decode(clean_b64)
  if decoded_bytes.startswith(_GZIP_MAGIC):
    decoded_bytes = gzip.decompress(decoded_bytes)
  if len(decoded_bytes) > _MAX_DECODED_REGION_BYTES:
    raise ValueError("Oversized region payload")
  reg_obj = json.loads(decoded_bytes.decode("utf-8"))
  if not isinstance(reg_obj, dict):
    raise ValueError(
        f"Expected JSON object for region, got {type(reg_obj).__name__}"
    )
  return reg_obj


def validate_regions_or_error(
    contexts: Mapping[str, str],
) -> tuple[list[dict[str, Any]] | None, str | None]:
  """Validates and decodes `okf.r.*` region descriptors from `contexts`.

  Args:
    contexts: Mapping of GCS custom object context keys to string values.

  Returns:
    `(regions, None)` when all region keys and payloads are valid (with
    `regions == []` when `okf.region_count` is `"0"`), or `(None, reason)` when
    the region index is missing (`Detailed navigation index missing`),
    incomplete (`Incomplete region index: ...`), or corrupted
    (`Corrupted region index: ...`).
  """
  region_count_str = contexts.get("okf.region_count")
  region_keys = sorted(k for k in contexts if k.startswith("okf.r."))

  if region_count_str is not None:
    expected_count = _parse_nonneg_ascii_int(region_count_str)
    if expected_count is None:
      return (
          None,
          (
              "Corrupted region index: Invalid okf.region_count"
              f" {region_count_str!r}."
          ),
      )
    expected_keys = (
        [f"okf.r.{i:03d}" for i in range(expected_count)]
        if expected_count <= len(contexts)
        else None
    )
    if expected_keys is None or not all(k in contexts for k in expected_keys):
      return (
          None,
          (
              f"Incomplete region index: Expected {expected_count} regions, but"
              " one or more region keys are missing"
          ),
      )
    if region_keys != expected_keys:
      return (
          None,
          (
              "Corrupted region index: Unexpected okf.r.* keys do not match"
              f" okf.region_count={expected_count}."
          ),
      )
  else:
    if not region_keys:
      return None, "Detailed navigation index missing"
    expected_keys = [f"okf.r.{i:03d}" for i in range(len(region_keys))]
    if region_keys != expected_keys:
      return (
          None,
          "Corrupted region index: Non-contiguous or malformed okf.r.* keys.",
      )

  regions: list[dict[str, Any]] = []
  for k in region_keys:
    try:
      region = _decode_region_entry(contexts[k])
    except (ValueError, OSError, EOFError, zlib.error, RecursionError) as e:
      return (
          None,
          f"Corrupted region index: Failed to decode region '{k}': {e}",
      )
    regions.append(region)
  return regions, None


def _is_valid_utf8_prefix(rem: bytes) -> bool:
  """Returns whether `rem` (0-3 undecoded bytes) is a valid UTF-8 prefix."""
  if len(rem) < 2:
    return True
  b0, b1 = rem[0], rem[1]
  if b0 == 0xE0:
    b1_ok = 0xA0 <= b1 <= 0xBF
  elif b0 == 0xED:
    b1_ok = 0x80 <= b1 <= 0x9F
  elif b0 == 0xF0:
    b1_ok = 0x90 <= b1 <= 0xBF
  elif b0 == 0xF4:
    b1_ok = 0x80 <= b1 <= 0x8F
  else:
    b1_ok = 0x80 <= b1 <= 0xBF
  return b1_ok and (len(rem) < 3 or 0x80 <= rem[2] <= 0xBF)


def decode_slice_bytes(
    content_bytes: bytes, *, trim_leading: bool = False
) -> str:
  """Decodes a byte slice as UTF-8, trimming split boundary codepoints.

  Args:
    content_bytes: Raw slice bytes read from the object.
    trim_leading: Whether to trim up to 3 leading UTF-8 continuation bytes (used
      when `start_byte > 0` may land mid-codepoint).

  Returns:
    Decoded UTF-8 text (trimming incomplete boundary multi-byte sequences), or
    Base64 prefixed with `BINARY_BASE64_PREFIX` for binary slices.
  """
  if not content_bytes:
    return ""
  start = 0
  if trim_leading:
    while (
        start < 3
        and start < len(content_bytes)
        and (content_bytes[start] & 0xC0) == 0x80
    ):
      start += 1
  if start < len(content_bytes):
    try:
      decoder = codecs.getincrementaldecoder("utf-8")(errors="strict")
      decoded = decoder.decode(content_bytes[start:], final=False)
      if decoded and _is_valid_utf8_prefix(decoder.getstate()[0]):
        return decoded
    except UnicodeDecodeError:
      pass
  encoded_b64 = base64.b64encode(content_bytes).decode("ascii")
  return f"{BINARY_BASE64_PREFIX}{encoded_b64}"


def _get_blob_int_size(blob: storage.Blob) -> int | None:
  """Returns `blob.size` when it is an integer, or `None` if unavailable."""
  raw_size = getattr(blob, "size", None)
  if isinstance(raw_size, int) and not isinstance(raw_size, bool):
    return raw_size
  return None


def is_gzip_content_encoded(blob: storage.Blob) -> bool:
  """Returns whether `blob` has HTTP `Content-Encoding: gzip` metadata."""
  encoding = getattr(blob, "content_encoding", None)
  if not isinstance(encoding, str):
    return False
  tokens = [tok.strip().lower() for tok in encoding.split(",") if tok.strip()]
  return tokens in (["gzip"], ["x-gzip"])


class _RawGzipBlobStream(io.RawIOBase):
  """Streams raw compressed chunks from a GCS `Blob` up to scan budget."""

  def __init__(self, blob: storage.Blob, compressed_size: int | None):
    self._blob = blob
    self._size = compressed_size
    self._buf: bytes | None = None
    self._offset = 0

  def readable(self) -> bool:
    return True

  def readinto(self, b: Any) -> int:
    if not b:
      return 0
    if self._size is None:
      try:
        raw_all = self._blob.download_as_bytes(
            start=0, end=_MAX_GZIP_RAW_SCAN_BYTES, raw_download=True
        )
      except gcs_exceptions.RequestRangeNotSatisfiable:
        raw_all = b""
      if len(raw_all) > _MAX_GZIP_RAW_SCAN_BYTES:
        raise ValueError(
            f"Compressed payload exceeds {_MAX_GZIP_RAW_SCAN_BYTES}-byte scan"
            " limit for Content-Encoding: gzip."
        )
      self._buf = raw_all
      self._size = len(raw_all)
    if self._offset >= self._size:
      return 0
    if self._offset >= _MAX_GZIP_RAW_SCAN_BYTES:
      raise ValueError(
          f"Compressed payload exceeds {_MAX_GZIP_RAW_SCAN_BYTES}-byte scan"
          " limit for Content-Encoding: gzip."
      )
    if self._buf is not None:
      chunk = self._buf[self._offset : self._offset + len(b)]
    else:
      end = min(
          self._offset + len(b) - 1,
          self._size - 1,
          _MAX_GZIP_RAW_SCAN_BYTES,
      )
      expected_max = end - self._offset + 1
      chunk = self._blob.download_as_bytes(
          start=self._offset, end=end, raw_download=True
      )
      if self._offset + len(chunk) > _MAX_GZIP_RAW_SCAN_BYTES:
        raise ValueError(
            f"Compressed payload exceeds {_MAX_GZIP_RAW_SCAN_BYTES}-byte scan"
            " limit for Content-Encoding: gzip."
        )
      if len(chunk) > expected_max:
        if self._offset != 0:
          raise ValueError(
              "Unexpected oversized ranged read for Content-Encoding: gzip."
          )
        self._buf = chunk
        self._size = len(chunk)
        chunk = chunk[: len(b)]
    if not chunk:
      self._size = self._offset
      return 0
    n = len(chunk)
    b[:n] = chunk
    self._offset += n
    return n


def _get_gzip_decompressed_size(blob: storage.Blob) -> int:
  """Streams a `Content-Encoding: gzip` blob to compute decompressed size."""
  compressed_size = _get_blob_int_size(blob)
  if compressed_size == 0:
    return 0
  if compressed_size is not None and compressed_size > _MAX_GZIP_RAW_SCAN_BYTES:
    raise ValueError(
        f"Compressed payload exceeds {_MAX_GZIP_RAW_SCAN_BYTES}-byte scan"
        " limit for Content-Encoding: gzip."
    )
  buffered = io.BufferedReader(
      _RawGzipBlobStream(blob, compressed_size),
      buffer_size=_GZIP_RAW_READ_CHUNK_BYTES,
  )
  total = 0
  with gzip.GzipFile(fileobj=buffered, mode="rb") as gz:
    while chunk := gz.read(_GZIP_RAW_READ_CHUNK_BYTES):
      total += len(chunk)
      if total > _MAX_GZIP_DECOMPRESSED_SCAN_BYTES:
        raise ValueError(
            "Decompressed payload exceeds"
            f" {_MAX_GZIP_DECOMPRESSED_SCAN_BYTES}-byte scan limit for"
            " Content-Encoding: gzip."
        )
  return total


def read_blob_slice_bytes(
    blob: storage.Blob, start_byte: int, end_byte: int
) -> bytes:
  """Reads `[start_byte, end_byte]` from `blob`, decompressing gzip if set.

  Args:
    blob: Target GCS blob (`Content-Encoding` already populated if present).
    start_byte: 0-indexed inclusive start offset (decompressed bytes for
      `gzip`/`x-gzip` blobs; stored bytes otherwise). Must be >= 0.
    end_byte: 0-indexed inclusive end offset (>= `start_byte`). If `start_byte`
      is within the payload and `end_byte` exceeds EOF, returns bytes to EOF.

  Returns:
    Slice bytes (up to `end_byte - start_byte + 1` bytes, or `b""` when
    reading from offset 0 of an empty gzip stream).

  Raises:
    ValueError: If a gzip slice or stream exceeds size/scan caps, or
      `start_byte` is past the decompressed gzip payload (> 0 if empty).
  """
  if not is_gzip_content_encoded(blob):
    return blob.download_as_bytes(start=start_byte, end=end_byte)

  want = end_byte - start_byte + 1
  if want > _MAX_GZIP_SLICE_BYTES:
    raise ValueError(
        f"Requested slice ({want} bytes) exceeds {_MAX_GZIP_SLICE_BYTES}-byte"
        " limit for Content-Encoding: gzip."
    )
  if start_byte >= _MAX_GZIP_DECOMPRESSED_SCAN_BYTES:
    raise ValueError(
        f"start_byte ({start_byte}) exceeds"
        f" {_MAX_GZIP_DECOMPRESSED_SCAN_BYTES}-byte scan limit for"
        " Content-Encoding: gzip."
    )
  compressed_size = _get_blob_int_size(blob)
  if compressed_size == 0:
    if start_byte > 0:
      raise ValueError(
          f"start_byte ({start_byte}) exceeds decompressed object size of 0"
          " bytes (valid range: [0, 0])."
      )
    return b""

  buffered = io.BufferedReader(
      _RawGzipBlobStream(blob, compressed_size),
      buffer_size=_GZIP_RAW_READ_CHUNK_BYTES,
  )
  with gzip.GzipFile(fileobj=buffered, mode="rb") as gz:
    pos = gz.seek(start_byte) if start_byte > 0 else 0
    slice_bytes = gz.read(want)
  if not slice_bytes and (start_byte > 0 or pos > 0):
    max_valid = max(0, pos - 1)
    raise ValueError(
        f"start_byte ({start_byte}) exceeds decompressed object size of"
        f" {pos} bytes (valid range: [0, {max_valid}])."
    )
  return slice_bytes


def _get_blob_contexts(blob: storage.Blob) -> dict[str, str]:
  """Extracts existing object contexts dictionary from a GCS blob."""
  contexts = getattr(blob, "contexts", None)
  if contexts is None:
    # Fallback to custom_contexts for backward compatibility or mock objects
    contexts = getattr(blob, "custom_contexts", None)
  if (
      contexts is None
      and hasattr(blob, "_properties")
      and isinstance(blob._properties, dict)  # pylint: disable=protected-access
  ):
    contexts = blob._properties.get("contexts")  # pylint: disable=protected-access

  if contexts is None:
    return {}

  custom = getattr(contexts, "custom", None)
  if custom is None and isinstance(contexts, dict):
    custom = contexts.get("custom", contexts)

  if isinstance(custom, (dict, Mapping)):
    result = {}
    for k, v in custom.items():
      if hasattr(v, "value"):
        result[str(k)] = str(v.value)
      elif isinstance(v, dict) and "value" in v:
        result[str(k)] = str(v["value"])
      elif v is not None:
        result[str(k)] = str(v)
    return result

  if isinstance(contexts, (dict, Mapping)):
    return {str(k): str(v) for k, v in contexts.items() if v is not None}

  return {}


def _format_missing_okf_error(
    gs_uri: str, blob: storage.Blob, missing_item: str
) -> str:
  """Formats diagnostic error detailing context capacity, existing contexts, and metadata."""
  existing_contexts = _get_blob_contexts(blob)
  used_bytes = sum(
      len(k.encode("utf-8")) + len(str(v).encode("utf-8"))
      for k, v in existing_contexts.items()
  )
  remaining_bytes = GCS_CONTEXT_MAX_TOTAL_BYTES - used_bytes
  remaining_keys = max(0, GCS_CONTEXT_MAX_KEYS - len(existing_contexts))
  contexts_str = (
      json.dumps(existing_contexts, indent=2) if existing_contexts else "None"
  )
  metadata = getattr(blob, "metadata", None)
  metadata_dict = dict(metadata) if isinstance(metadata, dict) else {}
  metadata_clean = {
      str(k): str(v)
      for k, v in metadata_dict.items()
      if isinstance(k, str) and isinstance(v, (str, int, float, bool))
  }
  metadata_str = (
      json.dumps(metadata_clean, indent=2) if metadata_clean else "None"
  )

  attributes: dict[str, Any] = {}
  size = getattr(blob, "size", None)
  if isinstance(size, int):
    attributes["size"] = size
  content_type = getattr(blob, "content_type", None)
  if isinstance(content_type, str):
    attributes["content_type"] = content_type
  content_encoding = getattr(blob, "content_encoding", None)
  if isinstance(content_encoding, str) and content_encoding.strip():
    attributes["content_encoding"] = content_encoding.strip()
  generation = getattr(blob, "generation", None)
  if isinstance(generation, (int, str)):
    attributes["generation"] = generation
  updated = getattr(blob, "updated", None)
  if isinstance(updated, str):
    attributes["updated"] = updated
  elif isinstance(updated, datetime.datetime):
    attributes["updated"] = updated.isoformat()

  attributes_str = json.dumps(attributes, indent=2) if attributes else "None"
  return (
      f"OKF_NOT_FOUND: {missing_item} for '{gs_uri}'.\n"
      f"Object Context Capacity: {remaining_bytes} bytes remaining"
      f" ({remaining_keys} of {GCS_CONTEXT_MAX_KEYS} keys remaining,"
      f" {used_bytes} bytes used across {len(existing_contexts)} keys out of"
      f" {GCS_CONTEXT_MAX_TOTAL_BYTES} total bytes).\n"
      f"Existing Object Contexts:\n{contexts_str}\n"
      f"Existing Custom Metadata:\n{metadata_str}\n"
      f"Object Attributes:\n{attributes_str}"
  )


@functools.cache
def _get_storage_client() -> storage.Client:
  """Returns a cached module-level storage.Client instance with telemetry.

  The project is resolved from GOOGLE_CLOUD_PROJECT, then from
  CLOUD_STORAGE_PROJECT (the plugin manifests pass the plugin's Project ID
  setting under that name), and otherwise left to the SDK's defaults (gcloud
  config).
  """
  project = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get(
      "CLOUD_STORAGE_PROJECT"
  )
  return storage.Client(project=project or None, client_info=CLIENT_INFO)


def _fetch_blob(gs_uri: str) -> tuple[storage.Blob | None, str | None]:
  """Helper to parse URI and retrieve GCS blob using client.bucket().get_blob().

  Args:
    gs_uri: Target GCS URI.

  Returns:
    Tuple of (Blob, None) on success or (None, error_message) on failure.
  """
  try:
    bucket_name, blob_name = _parse_gs_uri(gs_uri)
  except Exception as e:  # pylint: disable=broad-exception-caught
    return None, _format_gcs_error(gs_uri, e)

  try:
    client = _get_storage_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.get_blob(blob_name)
  except Exception as e:  # pylint: disable=broad-exception-caught
    return None, _format_gcs_error(gs_uri, e)

  if not blob:
    return None, f"Object '{gs_uri}' not found."

  return blob, None


def peek(gs_uri: str) -> str:
  """Returns the Tier 0 OKF summary from GCS object contexts.

  Args:
    gs_uri: The target Google Cloud Storage URI.

  Returns:
    The summary text string or diagnostic error detailing remaining capacity and
    existing context.
  """
  blob, err = _fetch_blob(gs_uri)
  if err or not blob:
    return err or f"Object '{gs_uri}' not found."

  contexts = _get_blob_contexts(blob)
  stale_warning = _check_staleness_warning(blob, contexts)
  if stale_warning:
    return stale_warning

  summary, err_reason = extract_summary_or_error(contexts)
  if summary is None:
    return _format_missing_okf_error(
        gs_uri, blob, err_reason or "Manifest summary missing"
    )
  _, region_err = validate_regions_or_error(contexts)
  if region_err is not None:
    return _format_missing_okf_error(gs_uri, blob, region_err)
  return summary


def list_regions(gs_uri: str) -> str:
  """Returns the Tier 1 table of contents (TOC) from region object contexts.

  Args:
    gs_uri: The target Google Cloud Storage URI.

  Returns:
    JSON string of region list or diagnostic error detailing remaining capacity
    and existing context.
  """
  blob, err = _fetch_blob(gs_uri)
  if err or not blob:
    return err or f"Object '{gs_uri}' not found."

  contexts = _get_blob_contexts(blob)
  stale_warning = _check_staleness_warning(blob, contexts)
  if stale_warning:
    return json.dumps({"warning": stale_warning, "regions": []})

  if not contexts:
    return _format_missing_okf_error(gs_uri, blob, "No object contexts found")

  has_region_keys = any(k.startswith("okf.r.") for k in contexts)
  if "okf.region_count" not in contexts and not has_region_keys:
    return _format_missing_okf_error(
        gs_uri, blob, "Detailed navigation index missing"
    )

  summary, summary_err = extract_summary_or_error(contexts)
  if summary is None:
    return _format_missing_okf_error(
        gs_uri, blob, summary_err or "Manifest summary missing"
    )

  regions, region_err = validate_regions_or_error(contexts)
  if region_err is not None or regions is None:
    if region_err and region_err.startswith("Corrupted region index:"):
      return json.dumps({"error": region_err, "regions": []})
    return _format_missing_okf_error(
        gs_uri, blob, region_err or "Detailed navigation index missing"
    )
  return json.dumps(regions, indent=2)


def expand(gs_uri: str, start_byte: int, end_byte: int) -> str:
  """Fetches a specific byte-range slice (Tier 2/3) from GCS.

  Both start_byte and end_byte are inclusive 0-indexed byte offsets
  (e.g., start_byte=0, end_byte=0 returns 1 byte; start_byte=0, end_byte=10
  returns 11 bytes).

  Args:
    gs_uri: The target Google Cloud Storage URI.
    start_byte: The 0-indexed starting byte offset (inclusive).
    end_byte: The 0-indexed ending byte offset (inclusive).

  Returns:
    The decoded string content or labeled Base64 fallback for binary data, or a
    staleness warning if the object generation changed since OKF index creation.
  """
  if start_byte < 0:
    return f"Invalid byte range: start_byte ({start_byte}) must be >= 0."
  if end_byte < 0:
    return f"Invalid byte range: end_byte ({end_byte}) must be >= 0."
  if start_byte > end_byte:
    return (
        f"Invalid byte range: start_byte ({start_byte}) cannot be greater than"
        f" end_byte ({end_byte})."
    )

  blob, err = _fetch_blob(gs_uri)
  if err or not blob:
    return err or f"Object '{gs_uri}' not found."

  contexts = _get_blob_contexts(blob)
  stale_warning = _check_staleness_warning(blob, contexts)
  if stale_warning:
    return stale_warning

  try:
    content_bytes = read_blob_slice_bytes(blob, start_byte, end_byte)
  except Exception as e:  # pylint: disable=broad-exception-caught
    return (
        f"Error reading byte range [{start_byte}:{end_byte}] from '{gs_uri}':"
        f" {str(e)}"
    )

  return decode_slice_bytes(content_bytes, trim_leading=start_byte > 0)


def write_okf(
    gs_uri: str,
    summary: str,
    regions: Sequence[Mapping[str, Any]],
    created_at: str = "",
) -> str:
  """Writes the OKF manifest to GCS Object Contexts using encoded keys.

  Encoding & Concurrency Architecture:
    - Encodes each region descriptor as base64(json) to fit within the GCS
      256-byte per-value limit.
    - If an encoded region payload exceeds 250 bytes, semantic truncation is
      applied to the underlying Python dictionary (trimming keywords, title,
      or ID) before re-encoding, guaranteeing valid JSON payloads under 256
      bytes without corrupting byte streams.
    - Uses GCS Optimistic Concurrency Control (OCC) by passing
      `if_generation_match=blob.generation` to `blob.patch()`, atomically
      preventing write race conditions if the object was replaced or modified
      during asynchronous index computation.
    - Enforces GCS Object Context limits: max 50 context keys and 25,600 total
      bytes per object.

  Args:
    gs_uri: The target Google Cloud Storage URI.
    summary: The Tier 0 OKF summary. Must be non-empty, start with a letter or
      digit, and omit single quotes, double quotes, backslashes, and forward
      slashes.
    regions: The list of OKF regions (max ~45-50 regions per object).
    created_at: Optional creation timestamp (defaults to UTC ISO 8601 if empty;
      validated against GCS Object Context rules when provided).

  Returns:
    Status message string confirming write or detailing validation, capacity, or
    precondition errors.
  """

  summary_err = _validate_gcs_context_value(summary, "summary")
  if summary_err:
    return summary_err

  summary_bytes = summary.encode("utf-8")
  summary_chunks: list[str] | None = None
  if len(summary_bytes) > GCS_CONTEXT_SAFE_VALUE_LIMIT:
    summary_chunks = _chunk_summary_for_gcs(summary, OKF_SUMMARY_CHUNK_SIZE)
    if summary_chunks is None:
      return (
          "Invalid summary: Summary contains a >200-byte span without letters"
          " or digits and cannot be split into valid GCS Object Context"
          " chunks."
      )

  if not created_at:
    created_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
  else:
    created_at_err = _validate_gcs_context_value(created_at, "created_at")
    if created_at_err:
      return created_at_err
    if len(created_at.encode("utf-8")) > GCS_CONTEXT_MAX_VALUE_BYTES:
      return (
          "Invalid created_at: Exceeds GCS value limit of"
          f" {GCS_CONTEXT_MAX_VALUE_BYTES} bytes."
      )

  if not isinstance(regions, list):
    return (
        "Invalid regions data: Expected a list of region objects, got"
        f" {type(regions).__name__}."
    )

  for i, reg in enumerate(regions):
    if not isinstance(reg, dict):
      return (
          f"Invalid region entry at index {i}: Each region must be a"
          f" dictionary, got {type(reg).__name__}."
      )

  blob, err = _fetch_blob(gs_uri)
  if err or not blob:
    return err or f"Object '{gs_uri}' not found."

  initial_generation = getattr(blob, "generation", None)
  initial_metageneration = getattr(blob, "metageneration", None)

  object_size = _get_blob_int_size(blob)
  if object_size == 0 and regions:
    return (
        f"Cannot create regions for empty object '{gs_uri}' (size is 0 bytes)."
    )

  prev_start_byte: int | None = None
  prev_end_byte: int | None = None
  prev_reg_id: str | None = None
  for i, reg in enumerate(regions):
    reg_id = str(reg.get("id", f"r{i}"))
    start_byte = reg.get("start_byte")
    end_byte = reg.get("end_byte")

    if not isinstance(start_byte, int) or isinstance(start_byte, bool):
      return (
          f"Invalid region '{reg_id}': 'start_byte' must be an integer, got"
          f" {type(start_byte).__name__}."
      )
    if not isinstance(end_byte, int) or isinstance(end_byte, bool):
      return (
          f"Invalid region '{reg_id}': 'end_byte' must be an integer, got"
          f" {type(end_byte).__name__}."
      )

    if start_byte < 0:
      return (
          f"Invalid region '{reg_id}': start_byte {start_byte} cannot be"
          " negative."
      )
    if end_byte < start_byte:
      return (
          f"Invalid region '{reg_id}': start_byte {start_byte} > end_byte"
          f" {end_byte} (inverted byte range)."
      )

    if i == 0 and is_gzip_content_encoded(blob):
      try:
        object_size = _get_gzip_decompressed_size(blob)
      except Exception as e:  # pylint: disable=broad-exception-caught
        return (
            f"Error inspecting Content-Encoding: gzip payload for '{gs_uri}':"
            f" {e}"
        )
      if object_size == 0:
        return (
            f"Cannot create regions for empty object '{gs_uri}' (size is 0"
            " bytes)."
        )

    if object_size is not None:
      if start_byte >= object_size:
        return (
            f"Invalid region '{reg_id}': start_byte {start_byte} exceeds"
            f" object size of {object_size} bytes (valid range: [0,"
            f" {object_size - 1}]). Byte boundaries must be grounded in actual"
            " file size and within object bounds."
        )
      if end_byte >= object_size:
        return (
            f"Invalid region '{reg_id}': end_byte {end_byte} exceeds object"
            f" size of {object_size} bytes (valid range: [0,"
            f" {object_size - 1}]). Byte boundaries must be grounded in actual"
            " file size and within object bounds."
        )

    if prev_start_byte is not None and start_byte < prev_start_byte:
      return (
          f"Invalid region ordering: Region '{reg_id}' (start_byte"
          f" {start_byte}) appears out of order after Region '{prev_reg_id}'"
          f" (start_byte {prev_start_byte}). Regions must be listed in"
          " ascending order by start_byte."
      )

    if prev_end_byte is not None and start_byte <= prev_end_byte:
      return (
          f"Invalid region overlap: Region '{reg_id}' start_byte {start_byte}"
          f" overlaps previous region '{prev_reg_id}' end_byte {prev_end_byte}."
          " For inclusive byte ranges, next start_byte must be at least"
          f" {prev_end_byte + 1}."
      )

    prev_start_byte = start_byte
    prev_end_byte = end_byte
    prev_reg_id = reg_id
  # Construct OKF Object Context map
  context_map: dict[str, str] = {
      "okf.v": "1.0",
      "okf.created_at": created_at,
  }
  if initial_generation is not None:
    context_map["okf.src_generation"] = str(initial_generation)

  # Helper function to encode a region dictionary to base64(json)
  # Uses urlsafe_b64encode because GCS Object Context validation forbids
  # single quotes ('), double quotes ("), backslashes (\), and forward
  # slashes (/).
  def _encode_region(reg_dict: dict[str, Any]) -> str:
    val_json = json.dumps(reg_dict, separators=(",", ":"))
    return base64.urlsafe_b64encode(val_json.encode("utf-8")).decode("utf-8")

  # Store summary (single key if <= GCS_CONTEXT_SAFE_VALUE_LIMIT bytes,
  # or chunked on alphanumeric boundaries if longer)
  if summary_chunks is None:
    context_map["okf.sum"] = summary
  else:
    context_map["okf.sum_chunks"] = str(len(summary_chunks))
    for i, s_chunk in enumerate(summary_chunks):
      context_map[f"okf.sum.{i:03d}"] = s_chunk

  # Build self-contained encoded region key-value pairs
  # (okf.r.000, okf.r.001, ...)
  context_map["okf.region_count"] = str(len(regions))
  for i, reg in enumerate(regions):
    reg_id = str(reg.get("id", f"r{i}"))
    reg_dict = {
        "id": reg_id,
        "start_byte": reg.get("start_byte", 0),
        "end_byte": reg.get("end_byte", 0),
    }
    if "title" in reg:
      reg_dict["title"] = reg["title"]
    if "keywords" in reg:
      reg_dict["keywords"] = reg["keywords"]

    val_b64 = _encode_region(reg_dict)

    # Guard: GCS_CONTEXT_MAX_VALUE_BYTES (256) is the hard per-value limit.
    # If the payload exceeds GCS_CONTEXT_SAFE_VALUE_LIMIT (250),
    # apply airtight step-by-step fallback truncation:
    # 1. Truncate keywords list to OKF_KEYWORD_TRUNCATION_LIMIT (3) items.
    # 2. Trim title to OKF_TITLE_TRUNCATION_LIMIT (50).
    # 3. Drop keywords completely if still exceeding limit.
    # 4. Progressively trim title down to 20 chars, then drop title if needed.
    # 5. Shorten reg_id if still exceeding limit.
    if len(val_b64) > GCS_CONTEXT_SAFE_VALUE_LIMIT:
      if "keywords" in reg_dict and isinstance(reg_dict["keywords"], list):
        reg_dict["keywords"] = reg_dict["keywords"][
            :OKF_KEYWORD_TRUNCATION_LIMIT
        ]
        val_b64 = _encode_region(reg_dict)
      if len(val_b64) > GCS_CONTEXT_SAFE_VALUE_LIMIT and "title" in reg_dict:
        reg_dict["title"] = str(reg_dict["title"])[:OKF_TITLE_TRUNCATION_LIMIT]
        val_b64 = _encode_region(reg_dict)
      if len(val_b64) > GCS_CONTEXT_SAFE_VALUE_LIMIT and "keywords" in reg_dict:
        del reg_dict["keywords"]
        val_b64 = _encode_region(reg_dict)
      if len(val_b64) > GCS_CONTEXT_SAFE_VALUE_LIMIT and "title" in reg_dict:
        reg_dict["title"] = str(reg_dict["title"])[:20]
        val_b64 = _encode_region(reg_dict)
      if len(val_b64) > GCS_CONTEXT_SAFE_VALUE_LIMIT and "title" in reg_dict:
        del reg_dict["title"]
        val_b64 = _encode_region(reg_dict)
      if len(val_b64) > GCS_CONTEXT_SAFE_VALUE_LIMIT:
        reg_dict["id"] = f"r{i}"
        val_b64 = _encode_region(reg_dict)

    if len(val_b64) > GCS_CONTEXT_MAX_VALUE_BYTES:
      return (
          f"Error: Region {reg_id} exceeds hard GCS value size limit of"
          f" {GCS_CONTEXT_MAX_VALUE_BYTES} bytes even after truncation."
      )

    context_map[f"okf.r.{i:03d}"] = val_b64

  # Fetch existing blob contexts to account for already used space
  existing_contexts = _get_blob_contexts(blob)

  # Calculate space and keys consumed by non-OKF pairs already on the object
  non_okf_keys_count = sum(
      1 for k in existing_contexts if not k.startswith("okf.")
  )
  available_keys = GCS_CONTEXT_MAX_KEYS - non_okf_keys_count
  okf_keys_count = len(context_map)

  if okf_keys_count > available_keys:
    return (
        f"Error: OKF index context key count ({okf_keys_count} keys) exceeds"
        f" remaining GCS object context key capacity ({available_keys} keys"
        f" available out of {GCS_CONTEXT_MAX_KEYS} max total keys,"
        f" {non_okf_keys_count} keys used by non-OKF contexts). Please"
        " consolidate or reduce the number of regions (max 50 contexts per"
        " object)."
    )

  non_okf_used_bytes = sum(
      len(k.encode("utf-8")) + len(str(v).encode("utf-8"))
      for k, v in existing_contexts.items()
      if not k.startswith("okf.")
  )

  available_bytes = GCS_CONTEXT_MAX_TOTAL_BYTES - non_okf_used_bytes
  okf_payload_bytes = sum(
      len(k.encode("utf-8")) + len(v.encode("utf-8"))
      for k, v in context_map.items()
  )

  if okf_payload_bytes > available_bytes:
    return (
        f"Error: OKF index context payload ({okf_payload_bytes} bytes) exceeds"
        f" remaining GCS object context capacity ({available_bytes} bytes"
        f" available out of {GCS_CONTEXT_MAX_TOTAL_BYTES} total,"
        f" {non_okf_used_bytes} bytes used by non-OKF contexts). Please"
        " consolidate or reduce the number of regions."
    )

  # Prepare updated context dictionary retaining existing non-OKF contexts
  updated_contexts: dict[str, str] = {
      k: v for k, v in existing_contexts.items() if not k.startswith("okf.")
  }
  updated_contexts.update(context_map)

  # Under GCS REST API PATCH semantics, omitted keys are preserved by default.
  # Explicitly set any obsolete 'okf.*' keys no longer present in context_map
  # to None so GCS PATCH deletes them from custom_contexts.
  custom_payload_raw: dict[str, Any] = {
      k: {"value": str(v)} for k, v in updated_contexts.items()
  }
  for k in existing_contexts:
    if k.startswith("okf.") and k not in context_map:
      custom_payload_raw[k] = None

  contexts_payload_raw = {"custom": custom_payload_raw}

  try:
    if ObjectContexts is not None:
      if ObjectCustomContextPayload is not None:
        custom_payload: dict[str, Any] = {
            k: ObjectCustomContextPayload(value=str(v))
            for k, v in updated_contexts.items()
        }
        for k in existing_contexts:
          if k.startswith("okf.") and k not in context_map:
            custom_payload[k] = None
      else:
        custom_payload = custom_payload_raw
      try:
        setattr(blob, "contexts", ObjectContexts(custom=custom_payload))
      except Exception:  # pylint: disable=broad-exception-caught
        try:
          setattr(blob, "contexts", ObjectContexts(custom=updated_contexts))
        except Exception:  # pylint: disable=broad-exception-caught
          setattr(blob, "contexts", contexts_payload_raw)
    else:
      setattr(blob, "contexts", contexts_payload_raw)

    # Ensure google.cloud.storage.Blob internal change tracking sends contexts
    # over REST API
    if hasattr(blob, "_patch_property"):
      blob._patch_property("contexts", contexts_payload_raw)  # pylint: disable=protected-access
    elif hasattr(blob, "_properties") and isinstance(blob._properties, dict):  # pylint: disable=protected-access
      blob._properties["contexts"] = contexts_payload_raw  # pylint: disable=protected-access
      if hasattr(blob, "_changes") and isinstance(blob._changes, set):  # pylint: disable=protected-access
        blob._changes.add("contexts")  # pylint: disable=protected-access

    patch_kwargs: dict[str, Any] = {}
    if initial_generation is not None:
      patch_kwargs["if_generation_match"] = initial_generation
    if initial_metageneration is not None:
      patch_kwargs["if_metageneration_match"] = initial_metageneration
    blob.patch(**patch_kwargs)
  except gcs_exceptions.PreconditionFailed as e:
    msg = e.message if hasattr(e, "message") else str(e)
    return (
        f"Precondition Failed: Target object '{gs_uri}' was modified or"
        f" replaced during OKF generation (generation mismatch): {msg}"
    )
  except Exception as e:  # pylint: disable=broad-exception-caught
    return _format_gcs_error(gs_uri, e, writing=True)

  return f"Successfully wrote OKF metadata to {gs_uri}"
