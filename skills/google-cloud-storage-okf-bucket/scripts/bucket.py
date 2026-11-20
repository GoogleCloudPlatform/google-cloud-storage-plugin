"""Bucket-level OKF discovery, URI validation, and per-object card extraction.

Provides bucket- and prefix-level URI validation (`parse_bucket_uri`),
bucket-scoped error diagnostics (`format_bucket_error`), per-object OKF
status/metadata card construction (`inspect_blob_okf_card`), and single-call
bucket discovery (`peek_bucket`) for the standalone
`google-cloud-storage-okf-bucket` skill. Relies on `core.py` for shared GCS
Object Context extraction, client construction, and staleness verification.
"""

from collections.abc import Iterable, Mapping, Sequence
import datetime
import enum
import json
import re
from typing import Any, Final

from google.cloud import storage

# pylint: disable=g-blanket-type-suppression
import core
# pylint: enable=g-blanket-type-suppression

MIN_INDEXABLE_OBJECT_BYTES: Final[int] = 20 * 1024
DEFAULT_BUCKET_CARD_MAX_OBJECTS: Final[int] = 100
MAX_BUCKET_CARD_OBJECTS: Final[int] = 500

_BUCKET_NAME_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]$"
)
_BUCKET_LIST_FIELDS: Final[str] = (
    "items(name,size,generation,updated,contentType,contentEncoding,contexts,"
    "metadata),nextPageToken"
)
_MAX_KEYWORDS_PER_OBJECT: Final[int] = 50
_MAX_METADATA_VALUE_CHARS: Final[int] = 256
_DIRECTORY_PLACEHOLDER_HEADROOM: Final[int] = 16
_MAX_DIRECTORY_PLACEHOLDERS_SCANNED: Final[int] = 256
CORRUPT_INDEX_WARNING: Final[str] = (
    "Corrupt or incomplete OKF index; re-index via write_okf to repair."
)


class ObjectOkfStatus(enum.StrEnum):
  """OKF index status of a GCS object in a bucket-level OKF card."""

  FRESH = "fresh"
  STALE = "stale"
  UNINDEXED = "unindexed"


def format_bucket_error(gs_uri: str, error: Exception) -> str:
  """Formats a GCS bucket or URI error with bucket-level IAM guidance.

  Args:
    gs_uri: Target GCS bucket or prefix URI string.
    error: Exception raised while validating or listing the bucket.

  Returns:
    A formatted human-readable error message with bucket-level IAM permissions.
  """
  formatted = core._format_gcs_error(gs_uri, error)  # pylint: disable=protected-access
  return formatted.replace(
      "read access to the object (storage.objects.get)",
      "list and read access to the bucket"
      " (storage.objects.list, storage.objects.get)",
  )


def parse_bucket_uri(gs_uri: str) -> tuple[str, str]:
  """Parses a bucket or prefix GCS URI ('gs://bucket' or 'gs://bucket/prefix').

  Args:
    gs_uri: Target Google Cloud Storage bucket or prefix URI string.

  Returns:
    A tuple of `(bucket_name, prefix)`, where `prefix` is `""` when `gs_uri`
    refers to the root of the bucket.

  Raises:
    ValueError: If `gs_uri` is not a valid `gs://` bucket or prefix URI.
  """
  if not isinstance(gs_uri, str) or not gs_uri.strip():
    raise ValueError(
        "Invalid URI: Expected a non-empty gs:// bucket or prefix string, got"
        f" {type(gs_uri).__name__}."
    )
  stripped_uri = gs_uri.strip()
  if any(char.isspace() for char in stripped_uri):
    raise ValueError(
        f"Invalid URI: Must not contain whitespace characters: '{gs_uri}'"
    )
  if not stripped_uri.lower().startswith("gs://"):
    raise ValueError(
        f"Invalid URI: Must start with 'gs://'. Provided: '{gs_uri}'"
    )
  path = stripped_uri[5:]
  if not path:
    raise ValueError(f"Invalid URI: Missing bucket name in '{gs_uri}'")

  bucket_name, _, prefix = path.partition("/")
  bucket_name = bucket_name.lower()
  if not _BUCKET_NAME_PATTERN.match(bucket_name) or ".." in bucket_name:
    raise ValueError(
        f"Invalid URI: Invalid GCS bucket name '{bucket_name}' in '{gs_uri}'"
    )
  if ".." in prefix.split("/"):
    raise ValueError(
        f"Invalid URI: Path traversal segment '..' is not allowed in '{gs_uri}'"
    )
  return bucket_name, prefix


def _extract_regions_metadata(
    contexts: Mapping[str, str],
) -> tuple[int, list[str], list[str]] | None:
  """Validates and extracts region metadata from GCS Object Contexts.

  Args:
    contexts: Mapping of GCS custom object context keys to string values.

  Returns:
    A `(region_count, region_titles, keywords)` tuple when all region keys and
    payloads are valid, or `None` if the region index is missing, incomplete,
    or corrupted.
  """
  regions, _ = core.validate_regions_or_error(contexts)
  if regions is None:
    return None

  region_titles: list[str] = []
  keywords: list[str] = []
  seen_keywords: set[str] = set()
  for region_payload in regions:
    title = region_payload.get("title")
    if isinstance(title, str) and title.strip():
      region_titles.append(title.strip())
    raw_keywords = region_payload.get("keywords")
    if isinstance(raw_keywords, list):
      for keyword in raw_keywords:
        if len(keywords) >= _MAX_KEYWORDS_PER_OBJECT:
          break
        if isinstance(keyword, str) and (kw := keyword.strip()):
          if kw not in seen_keywords:
            seen_keywords.add(kw)
            keywords.append(kw)

  return len(regions), region_titles, keywords


def _get_blob_size(blob: storage.Blob) -> int:
  """Returns the non-negative byte size of `blob`, defaulting to 0."""
  raw_size = getattr(blob, "size", None)
  if isinstance(raw_size, int) and not isinstance(raw_size, bool):
    return max(0, raw_size)
  return 0


def _populate_optional_blob_attributes(
    blob: storage.Blob,
    object_card: dict[str, Any],
) -> None:
  """Populates optional GCS blob attributes on `object_card` in place."""
  content_type = getattr(blob, "content_type", None)
  if isinstance(content_type, str) and content_type:
    object_card["content_type"] = content_type
  content_encoding = getattr(blob, "content_encoding", None)
  if isinstance(content_encoding, str) and content_encoding.strip():
    object_card["content_encoding"] = content_encoding.strip()
  generation = getattr(blob, "generation", None)
  if isinstance(generation, (int, str)) and not isinstance(generation, bool):
    object_card["generation"] = str(generation)
  updated = getattr(blob, "updated", None)
  if isinstance(updated, str) and updated:
    object_card["updated"] = updated
  elif isinstance(updated, datetime.datetime):
    object_card["updated"] = updated.isoformat()
  metadata = getattr(blob, "metadata", None)
  if isinstance(metadata, Mapping):
    custom_metadata = {
        str(key): str(value)[:_MAX_METADATA_VALUE_CHARS]
        for key, value in metadata.items()
        if isinstance(key, str) and isinstance(value, (str, int, float, bool))
    }
    if custom_metadata:
      object_card["custom_metadata"] = custom_metadata


def inspect_blob_okf_card(
    blob: storage.Blob,
    bucket_name: str,
) -> dict[str, Any]:
  """Constructs a per-object OKF card entry from a GCS Blob metadata instance.

  Args:
    blob: GCS `storage.Blob` instance containing object metadata and contexts.
    bucket_name: Lowercase name of the enclosing GCS bucket.

  Returns:
    A dictionary representing the per-object OKF status and metadata card.
  """
  blob_name = str(getattr(blob, "name", ""))
  size = _get_blob_size(blob)

  # pylint: disable=protected-access
  contexts = core._get_blob_contexts(blob)
  stale_warning = core._check_staleness_warning(blob, contexts)
  # pylint: enable=protected-access
  summary = None
  corrupt_warning = None
  region_count, region_titles, keywords = 0, [], []
  has_okf_keys = any(key.startswith("okf.") for key in contexts)
  if stale_warning is not None:
    okf_status = ObjectOkfStatus.STALE
  else:
    extracted_summary, _ = core.extract_summary_or_error(contexts)
    regions_meta = (
        _extract_regions_metadata(contexts)
        if extracted_summary is not None
        else None
    )
    if regions_meta is not None:
      okf_status = ObjectOkfStatus.FRESH
      summary = extracted_summary
      region_count, region_titles, keywords = regions_meta
    else:
      okf_status = ObjectOkfStatus.UNINDEXED
      if has_okf_keys:
        corrupt_warning = CORRUPT_INDEX_WARNING

  needs_indexing = (
      okf_status == ObjectOkfStatus.STALE
      or corrupt_warning is not None
      or (
          okf_status == ObjectOkfStatus.UNINDEXED
          and size >= MIN_INDEXABLE_OBJECT_BYTES
      )
  )

  object_card: dict[str, Any] = {
      "gs_uri": f"gs://{bucket_name}/{blob_name}",
      "name": blob_name,
      "size": size,
      "okf_status": str(okf_status),
      "needs_indexing": needs_indexing,
  }
  _populate_optional_blob_attributes(blob, object_card)
  if summary is not None:
    object_card["summary"] = summary
  if region_count > 0:
    object_card["region_count"] = region_count
  if region_titles:
    object_card["region_titles"] = region_titles
  if keywords:
    object_card["keywords"] = keywords
  if stale_warning is not None:
    object_card["stale_warning"] = stale_warning
  if corrupt_warning is not None:
    object_card["corrupt_warning"] = corrupt_warning
  non_okf_contexts = {
      key: value
      for key, value in contexts.items()
      if not key.startswith("okf.")
  }
  if non_okf_contexts:
    object_card["custom_contexts"] = non_okf_contexts
  return object_card


def _is_directory_placeholder(blob: storage.Blob) -> bool:
  """Returns whether `blob` is a zero-byte virtual directory placeholder."""
  blob_name = getattr(blob, "name", "")
  return (
      isinstance(blob_name, str)
      and blob_name.endswith("/")
      and _get_blob_size(blob) == 0
  )


def _collect_object_cards(
    blobs_iterator: Iterable[storage.Blob],
    bucket_name: str,
    max_results: int,
) -> tuple[list[dict[str, Any]], bool]:
  """Collects up to `max_results` object cards and reports truncation."""
  object_cards: list[dict[str, Any]] = []
  truncated = False
  skipped_placeholders = 0

  for blob in blobs_iterator:
    if _is_directory_placeholder(blob):
      skipped_placeholders += 1
      if skipped_placeholders > _MAX_DIRECTORY_PLACEHOLDERS_SCANNED:
        truncated = True
        break
      continue
    if len(object_cards) >= max_results:
      truncated = True
      break
    object_cards.append(inspect_blob_okf_card(blob, bucket_name))

  return object_cards, truncated


def _build_bucket_card(
    normalized_uri: str,
    bucket_name: str,
    prefix: str,
    object_cards: Sequence[Mapping[str, Any]],
    *,
    truncated: bool,
) -> dict[str, Any]:
  """Aggregates per-object OKF cards into a bucket-level OKF card dictionary."""
  counts = {
      str(ObjectOkfStatus.FRESH): 0,
      str(ObjectOkfStatus.STALE): 0,
      str(ObjectOkfStatus.UNINDEXED): 0,
  }
  candidates_to_index: list[str] = []
  unindexed_uris: list[str] = []
  stale_uris: list[str] = []
  total_bytes = 0

  for card in object_cards:
    status = card["okf_status"]
    counts[status] += 1
    total_bytes += card["size"]
    uri = card["gs_uri"]
    if card["needs_indexing"]:
      candidates_to_index.append(uri)
    if status == ObjectOkfStatus.UNINDEXED:
      unindexed_uris.append(uri)
    elif status == ObjectOkfStatus.STALE:
      stale_uris.append(uri)

  return {
      "gs_uri": normalized_uri,
      "bucket": bucket_name,
      "prefix": prefix,
      "total_objects": len(object_cards),
      "total_bytes": total_bytes,
      "truncated": truncated,
      "counts": counts,
      "candidates_to_index": candidates_to_index,
      "unindexed_uris": unindexed_uris,
      "stale_uris": stale_uris,
      "objects": list(object_cards),
  }


def peek_bucket(
    gs_uri: str,
    max_results: int = DEFAULT_BUCKET_CARD_MAX_OBJECTS,
) -> str:
  """Returns a bucket- or prefix-level OKF card aggregated from Object Contexts.

  Scans objects under `gs_uri` (`gs://bucket` or `gs://bucket/prefix`) in a
  single metadata listing without downloading object payloads. Aggregates each
  object's OKF status (`fresh`, `stale`, `unindexed`), Tier 0 summary, region
  titles, keywords, and custom contexts, and identifies candidate `gs://` URIs
  that require OKF index generation or refresh.

  Args:
    gs_uri: Target GCS bucket or prefix URI (e.g. 'gs://my-bucket' or
      'gs://my-bucket/logs/2026-09/').
    max_results: Maximum number of objects to include in the card (between 1 and
      500, default 100).

  Returns:
    JSON string containing the bucket-level OKF card (`counts`,
    `candidates_to_index`, `unindexed_uris`, `stale_uris`, and per-object
    `objects` entries), or a diagnostic error string.
  """
  try:
    bucket_name, prefix = parse_bucket_uri(gs_uri)
  except ValueError as error:
    return format_bucket_error(gs_uri, error)

  if (
      isinstance(max_results, bool)
      or not isinstance(max_results, int)
      or not (1 <= max_results <= MAX_BUCKET_CARD_OBJECTS)
  ):
    return (
        "Invalid max_results: Expected an integer between 1 and"
        f" {MAX_BUCKET_CARD_OBJECTS}, got {max_results!r}."
    )

  normalized_uri = (
      f"gs://{bucket_name}/{prefix}" if prefix else f"gs://{bucket_name}"
  )

  try:
    # Reuse core.py's shared client factory without modifying object-level core.
    client = core._get_storage_client()  # pylint: disable=protected-access
    blobs_iterator = client.list_blobs(
        bucket_name,
        prefix=prefix or None,
        page_size=max_results + 1 + _DIRECTORY_PLACEHOLDER_HEADROOM,
        max_results=max_results + 1 + _MAX_DIRECTORY_PLACEHOLDERS_SCANNED,
        projection="noAcl",
        fields=_BUCKET_LIST_FIELDS,
    )
    object_cards, truncated = _collect_object_cards(
        blobs_iterator, bucket_name, max_results
    )
  # MCP tool boundary: convert any GCS/auth/transport failure into a diagnostic
  # string via format_bucket_error rather than crashing the MCP server.
  except Exception as error:  # pylint: disable=broad-exception-caught
    return format_bucket_error(normalized_uri, error)

  bucket_card = _build_bucket_card(
      normalized_uri,
      bucket_name,
      prefix,
      object_cards,
      truncated=truncated,
  )
  return json.dumps(bucket_card, indent=2)
