# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "mcp",
#   "google-cloud-storage",
# ]
# ///
"""mcp_server.py - GCS-OKF Bucket & Object MCP Server.

This script acts as the Model Context Protocol (MCP) server for the
bucket-level GCS-OKF skill (`google-cloud-storage-okf-bucket`). It exposes the
five OKF verbs (`peek_bucket`, `peek`, `list_regions`, `expand`, `write_okf`)
as MCP tools, delegating bucket discovery to `bucket.py` and object-level OKF
operations to `core.py`.

The comment block above is PEP 723 inline script metadata: the plugin
manifests launch this file with `uv run --script`, which installs the listed
dependencies into an isolated environment on first start. Running it with a
plain `python3` that already has the packages installed works too.
"""

import json

# pylint: disable=g-blanket-type-suppression
# pylint: disable=g-import-not-at-top
try:
  from mcp.server.fastmcp import FastMCP as _McpServer
except ImportError:
  # mcp 2.x renamed FastMCP to MCPServer.
  from mcp.server.mcpserver import MCPServer as _McpServer  # pyrefly: ignore[missing-import]

import bucket
import core
# pylint: enable=g-import-not-at-top
# pylint: enable=g-blanket-type-suppression

app = _McpServer("GCS-OKF-Navigator")


@app.tool()
def peek_bucket(gs_uri: str, max_results: int = 100) -> str:
  """Returns the bucket- or prefix-level OKF card from GCS Object Contexts.

  Args:
    gs_uri: Target GCS bucket or prefix URI (e.g. 'gs://bucket_name' or
      'gs://bucket_name/prefix/').
    max_results: Maximum number of objects to inspect (1-500, default 100).

  Returns:
    JSON string containing object OKF status counts, candidate URIs, and
    per-object OKF card entries, or a diagnostic error string.
  """
  return bucket.peek_bucket(gs_uri, max_results=max_results)


@app.tool()
def peek(gs_uri: str) -> str:
  """Returns the Tier 0 Object Knowledge Finder (OKF) summary for a GCS object.

  Use this tool first to retrieve a high-level summary of a GCS object's
  contents
  and evaluate if the object is relevant before fetching detailed region trees
  or content.

  Args:
    gs_uri: The target GCS object URI (e.g. 'gs://bucket_name/object_name').

  Returns:
    Summary text string or diagnostic error detailing context capacity.
  """
  return core.peek(gs_uri)


@app.tool()
def list_regions(gs_uri: str) -> str:
  """Returns the Tier 1/2 OKF Table of Contents (region index) for a GCS object.

  Use this tool to inspect all semantic regions, inclusive byte ranges, titles,
  summaries, and keywords defined on the object.

  Args:
    gs_uri: The target GCS object URI (e.g. 'gs://bucket_name/object_name').

  Returns:
    JSON string of region descriptors or diagnostic error detailing context
    capacity.
  """
  return core.list_regions(gs_uri)


@app.tool()
def expand(gs_uri: str, start_byte: int, end_byte: int) -> str:
  """Fetches a targeted byte-range slice (Tier 2/3) from a GCS object.

  Use this tool after inspecting regions via list_regions to read specific text
  slices.

  Args:
    gs_uri: The target GCS object URI (e.g. 'gs://bucket_name/object_name').
    start_byte: 0-indexed starting byte offset (inclusive).
    end_byte: 0-indexed ending byte offset (inclusive).

  Returns:
    Decoded UTF-8 text content of the requested slice (trimming any split
    boundary UTF-8 bytes) or Base64 prefixed with
    '[Base64-encoded binary data]' for binary data.
  """
  result = core.expand(gs_uri, start_byte, end_byte)
  if result.startswith(core.STALE_INDEX_PREFIX):
    # Reuse core.py's blob fetcher to inspect live size and slice bytes.
    blob, _ = core._fetch_blob(gs_uri)  # pylint: disable=protected-access
    if blob is not None:
      if core.is_gzip_content_encoded(blob):
        size_line = (
            f"Current object size: {blob.size} compressed bytes"
            " (Content-Encoding: gzip; use decompressed offsets)."
        )
      else:
        size_line = f"Current object size: {blob.size} bytes."
      try:
        raw_bytes = core.read_blob_slice_bytes(blob, start_byte, end_byte)
      # Fall back to staleness warning and object size if byte slice fails.
      except Exception:  # pylint: disable=broad-exception-caught
        return f"{result}\n{size_line}"
      text = core.decode_slice_bytes(raw_bytes, trim_leading=start_byte > 0)
      return f"{result}\n{size_line}\n{text}"
  return result


@app.tool()
def write_okf(
    gs_uri: str,
    summary: str,
    regions_json: str,
    created_at: str = "",
) -> str:
  """Writes a generated OKF index manifest into GCS object custom metadata contexts.

  Args:
    gs_uri: The target GCS object URI (e.g. 'gs://bucket_name/object_name').
    summary: High-level overview text string for the object. Must be non-empty,
      start with a letter or digit, and omit single quotes, double quotes,
      backslashes, and forward slashes.
    regions_json: Valid JSON array string of region objects, e.g.: '[{"id":
      "r1", "start_byte": 0, "end_byte": 100, "title": "Intro", "keywords":
      ["a"]}]'
    created_at: Optional ISO 8601 timestamp string.

  Returns:
    Success status message or error message if validation fails or object
    context capacity is exceeded.
  """
  try:
    regions = json.loads(regions_json)
  except json.JSONDecodeError as e:
    return f"Error parsing regions_json: Invalid JSON string ({e})"

  if not isinstance(regions, list):
    return (
        "Invalid regions_json: Must be a JSON array of region objects, got"
        f" {type(regions).__name__}."
    )

  try:
    return core.write_okf(gs_uri, summary, regions, created_at=created_at)
  # MCP tool boundary: surface unexpected write failures as a tool error string.
  except Exception as e:  # pylint: disable=broad-exception-caught
    return f"Error writing OKF: {e}"


if __name__ == "__main__":
  # Runs the server using standard stdin/stdout for MCP communication
  app.run()
