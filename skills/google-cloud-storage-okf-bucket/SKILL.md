---
name: google-cloud-storage-okf-bucket
description: >-
  Discovers, inventories, triages, and searches across Google Cloud Storage (GCS)
  buckets and prefixes (gs://bucket or gs://bucket/prefix/) using the Open
  Knowledge Format (OKF). Aggregates per-object OKF status (fresh, stale,
  unindexed), Tier 0 summaries, region titles, keywords, and custom contexts in a
  single metadata listing without downloading object payloads. Use when: (1)
  answering open-ended discovery, search, or root-cause triage questions across a
  GCS bucket or prefix; (2) auditing bucket OKF index health and identifying
  candidates_to_index; (3) comparing sections across multiple objects in a bucket;
  or (4) batch-indexing unindexed (20 KB or larger) and stale objects across a
  bucket. Don't use for single-object navigation when the exact
  gs://bucket/object URI is already known and no bucket-level discovery is
  needed (use google-cloud-storage-okf), or for bucket provisioning and IAM
  administration (use google-cloud-storage-basics or
  google-cloud-storage-bucket-architect).
license: Apache-2.0
metadata:
  version: v3
  publisher: google
  tags: "gcs, okf, bucket-search, semantic-search, open-knowledge-format, progressive-disclosure, mcp"
  category: storage
  support_tier: primary
---

# Google Cloud Storage Bucket-Level OKF Skill

Use the `google-cloud-storage-okf-bucket` skill to discover, inventory, triage,
compare, and batch-index unstructured datasets across Google Cloud Storage
(`gs://`) buckets and folder prefixes via 4-tier progressive disclosure, without
polling individual objects or downloading full payloads into the context window.
For single-object navigation when a specific `gs://bucket/object` path is
already known, see the companion `google-cloud-storage-okf` skill.

## Open Knowledge Format (OKF) at the Bucket & Prefix Level

The
[Open Knowledge Format (OKF)](https://cloud.google.com/blog/products/data-analytics/how-the-open-knowledge-format-can-improve-data-sharing)
establishes an open standard for attaching self-describing, AI-ready semantic
knowledge directly to data assets without mutating raw content. While
`google-cloud-storage-okf` navigates inside a single blob (`peek` ->
`list_regions` -> `expand`), **`google-cloud-storage-okf-bucket` extends OKF to
whole buckets and prefixes**:

-   **Single-Call Bucket Discovery (`peek_bucket`)**: Aggregates every object's
    GCS Object Context (`custom_contexts`) in a single HTTP `objects.list`
    metadata call (`projection="noAcl"`) with zero object payload downloads (`0`
    bytes read) and zero per-object `objects.get` round-trips.
-   **4-Tier Progressive Disclosure**: Narrows a bucket of hundreds of files
    down to the exact relevant objects and byte ranges (`peek_bucket` ->
    `list_regions` -> `expand`) before reading a single byte of object content.
-   **Automated Index Health Triage**: Classifies every object in the bucket or
    prefix as `fresh`, `stale`, or `unindexed`, and computes
    `candidates_to_index` using the `20 KB` (`20,480` bytes) minimum indexable
    size threshold.

## Inline GCS Object Context Aggregation vs. Per-Object Polling

In Google Cloud Storage (GCS), **Object Context** (`custom_contexts` /
`contexts.custom`) is returned inline by the GCS JSON API `objects.list`
endpoint when requested via a partial response field mask:

-   **Inline Context Extraction**: `peek_bucket` requests
    `items(name,size,generation,updated,contentType,contentEncoding,contexts,metadata),nextPageToken`
    with `projection="noAcl"`, extracting each object's summary (`okf.sum` or
    `okf.sum.000..N`), decoded region titles and deduplicated keywords
    (`okf.r.000..M`), provenance (`okf.src_generation` vs. `generation`), and
    non-OKF contexts.
-   **Why Per-Object Polling Fails at Bucket Scale**: Calling `peek(gs_uri)` in
    a loop across `N` objects issues `N` sequential `objects.get` RPCs and
    clutters the agent context with `N` separate tool responses. `peek_bucket`
    replaces all `N` calls with a single structured JSON bucket card.

### Listing Projection & SDK Compatibility

-   **Directory Placeholder Filtering**: Zero-byte virtual directory
    placeholders (`name` ending with `/` and `size == 0`) are automatically
    skipped so `total_objects` and `counts` reflect real data objects.
-   **Dual-SDK Context Parsing**: Reads both typed `blob.contexts` attributes on
    newer `google-cloud-storage` SDKs and raw `blob._properties["contexts"]`
    dicts on older releases, capping decoded region entries at `8 KB`.

--------------------------------------------------------------------------------

## Core Architecture & 4-Tier Progressive Disclosure

Bucket-level GCS-OKF adds **Tier -1 (`peek_bucket`)** above the object-level
progressive disclosure hierarchy:

-   **Tier -1 (`peek_bucket`)**: Single-request bucket/prefix OKF card
    aggregating object status counts (`fresh`, `stale`, `unindexed`),
    `candidates_to_index`, and per-object `summary`, `region_titles`,
    `keywords`, and `custom_contexts`.
-   **Tier 0 (`peek`)**: Single-object summary and `OKF_NOT_FOUND` capacity
    diagnostic (used when inspecting an individual unindexed object's context
    budget prior to indexing; skipped when `peek_bucket` already returned a
    `fresh` summary).
-   **Tier 1/2 (`list_regions`)**: Per-object Table of Contents with exact
    inclusive `[start_byte, end_byte]` boundaries for each semantic region.
-   **Tier 2/3 (`expand`)**: Targeted byte-range slice reader fetching only the
    relevant region(s).
-   **Generative Layer (`write_okf`)**: Persists newly generated or refreshed
    OKF manifests into GCS Object Context with `if_generation_match` Optimistic
    Concurrency Control (OCC).

```
1. peek_bucket(gs_uri, max_results=100) -> Bucket card (counts, candidates_to_index, objects)
2. Filter candidate objects from card (match summary, region_titles, keywords; skip peek() on fresh)
3. list_regions(candidate_gs_uri) -> [start_byte, end_byte]
4. expand(candidate_gs_uri, start_byte, end_byte) -> Read targeted byte slice(s)
```

--------------------------------------------------------------------------------

## Setup (Read Before Any OKF Call)

Every OKF verb is served by the `google-cloud-storage-okf` MCP server. If that
server is not registered with your MCP client, the tools do not exist in your
session and no OKF work is possible.

### Tool Availability Gate

**Before the first OKF step of any task, you MUST confirm that the MCP tools
`peek_bucket`, `peek`, `list_regions`, `expand`, and `write_okf` are all
present** (either as direct or server-prefixed tools, or under `<mcp_servers>`
as lazy-loaded MCP tools invoked via `call_mcp_tool` — both count as present).

-   **All five present**: proceed with the bucket-level OKF workflow.
-   **Any one missing**: do NOT start the workflow. Tell the user the
    `google-cloud-storage-okf` MCP server is not registered with this client (or
    points to the 4-tool object server if only `peek_bucket` is missing), relay
    the **One-Time Setup** steps below, and **STOP**. Do not resume OKF work
    until all five tools are bound.

### Never Bypass the MCP Server

**NEVER** import or execute `scripts/bucket.py`, `scripts/core.py`, or
`scripts/mcp_server.py` directly as a substitute for the MCP tools — for example
`python3 -c "import bucket; bucket.peek_bucket(...)"`, or launching
`mcp_server.py` as a module in a subshell and driving it by hand. Do **NOT**
read sibling skills, internal eval files, `scripts/bucket.py`, or
`scripts/core.py` — all tool schemas and workflows are documented below so you
can invoke the MCP tools (`peek_bucket`, `peek`, `list_regions`, `expand`,
`write_okf`) directly. Furthermore, do **NOT** fall back to `gcloud storage ls`,
`gsutil ls`, `gcloud storage cat`, or raw `google.cloud.storage` Python scripts
to list or read bucket objects when the MCP server is missing or returns an
error. Every OKF call MUST go through MCP.

### One-Time Setup

1.  Install the **whole `google-cloud-storage-okf-bucket` skill directory**
    (including `scripts/`) wherever your harness discovers skills. A
    `SKILL.md`-only copy cannot run the server.
2.  Install the server's two Python dependencies, the MCP SDK and the GCS client
    library, using the `python3` that will run the server:

    ```bash
    python3 -m pip install mcp google-cloud-storage
    ```

3.  Authenticate and pick a project. The server reads Application Default
    Credentials and needs a Google Cloud project to bill and quota its requests:

    ```bash
    gcloud auth application-default login
    gcloud config set project <project-id>
    ```

    Instead of a machine-wide project, you can pass one to this server only
    through the `env` block in step 4.

4.  Register the canonical server command with your MCP client, then restart the
    client. Use an absolute path for `<abs google-cloud-storage-okf-bucket dir>`
    and the same `python3` as in step 2; the `env` block is optional:

    ```json
    {
      "mcpServers": {
        "google-cloud-storage-okf": {
          "command": "python3",
          "args": ["<abs google-cloud-storage-okf-bucket dir>/scripts/mcp_server.py"],
          "env": {"GOOGLE_CLOUD_PROJECT": "<project-id>"}
        }
      }
    }
    ```

If the client reports that the server failed to connect, run the command from
step 4 by hand and read the traceback for missing dependencies.

### Credential, Project, and Permission Errors

If any OKF tool returns an error about credentials, a missing project (for
example
`Project was not passed and could not be determined from the environment`), or
permission denied (401 or 403), the environment is not set up, not the request.
Relay the error and its `Fix:` line verbatim and **STOP**:

-   Missing or expired credentials: `gcloud auth application-default login`.
-   Missing project: `gcloud config set project <project-id>`, or the `env`
    block in step 4.
-   Permission denied: follow `Fix:` (`--disable-quota-project` or
    `set-quota-project` for `serviceusage.services.use`, or grant
    `storage.objects.list`, `storage.objects.get`, and `storage.objects.update`
    on the bucket).

Do **NOT** change the user's gcloud configuration or credentials yourself, do
NOT guess a project, and do NOT retry the operation through `gcloud storage`,
`gsutil`, or client libraries as a workaround.

--------------------------------------------------------------------------------

## MCP Tools Reference

### 1. Peek Bucket Tool (`peek_bucket`)

Retrieves a bucket- or prefix-level OKF card aggregated from inline GCS Object
Contexts in a single metadata listing (`0` payload bytes downloaded).

-   **Args**: `gs_uri` (target GCS bucket or prefix URI, e.g.,
    `'gs://my-bucket'` or `'gs://my-bucket/logs/2026-08/'`), `max_results`
    (maximum non-placeholder objects to inspect, `1` to `500`, default `100`).
-   **Returns**: Formatted JSON string with the bucket-level OKF card:
    -   `gs_uri`, `bucket`, `prefix`: Normalized target bucket and prefix.
    -   `total_objects`, `total_bytes`, `truncated`: Inventory totals and
        whether additional objects exist beyond `max_results`. When `truncated`
        is `true`, either raise `max_results` (up to `500`) or narrow `gs_uri`
        to a more specific `gs://bucket/subprefix/`, and note the cap in
        inventory reports.
    -   `counts`: Object count breakdown by `okf_status`
        (`{"fresh": N, "stale": M, "unindexed": K}`).
    -   `candidates_to_index`: List of `gs://` URIs where `needs_indexing` is
        `true` (`stale` objects, objects with `corrupt_warning`, plus clean
        `unindexed` objects with `size >= 20480` bytes).
    -   `unindexed_uris`, `stale_uris`: Lists of `gs://` URIs by status.
    -   `objects`: Per-object card entries containing `gs_uri`, `name`, `size`,
        `okf_status` (`"fresh"`, `"stale"`, `"unindexed"`), `needs_indexing`
        (`bool`), `content_type`, `content_encoding` (when set, e.g. `"gzip"`),
        `generation`, `updated`, and (when present) `summary`, `region_count`,
        `region_titles`, `keywords` (for `fresh` objects), `stale_warning` (for
        `stale` objects), `corrupt_warning` (for corrupt or incomplete OKF
        metadata), `custom_contexts`, and `custom_metadata`.

### 2. Peek Tool (`peek`)

Retrieves the Tier 0 summary or `OKF_NOT_FOUND` capacity diagnostic for a single
GCS object (`gs://bucket/object`).

-   **Args**: `gs_uri` (target GCS object URI, e.g.,
    `'gs://my-bucket/app.log'`).
-   **Returns**: Tier 0 summary string if fresh OKF Object Context exists, a
    `Stale OKF index` warning if `okf.src_generation` mismatches live
    `generation`, or an `OKF_NOT_FOUND` diagnostic detailing remaining
    `Object Context Capacity` (out of `50` keys / `25,600` bytes),
    `Existing Object Contexts`, `Existing Custom Metadata`, and
    `Object Attributes`.
-   **When to Skip `peek` After `peek_bucket`**: If `peek_bucket` already
    returned `okf_status: "fresh"` and `summary` for an object, do **NOT** call
    `peek(gs_uri)` on that object again — proceed directly to
    `list_regions(gs_uri)`.

### 3. List Regions Tool (`list_regions`)

Retrieves the Tier 1/2 Table of Contents (region index) for a candidate object.

-   **Args**: `gs_uri` (target GCS object URI).
-   **Returns**: JSON array of region descriptors (`id`, inclusive `start_byte`
    and `end_byte`, `title`, and `keywords`), a
    `{"warning": "Stale OKF index...", "regions": []}` object, or an
    `OKF_NOT_FOUND` diagnostic.

### 4. Expand Tool (`expand`)

Fetches a targeted inclusive byte-range slice `[start_byte, end_byte]` from a
GCS object payload.

-   **Args**: `gs_uri` (target GCS object URI), `start_byte` (0-indexed
    inclusive start offset), `end_byte` (0-indexed inclusive end offset,
    `0 <= start_byte <= end_byte < size` for uncompressed objects; when
    `content_encoding` is `"gzip"` or `"x-gzip"`, `size` reflects stored
    compressed bytes while `expand` and `write_okf` use decompressed offsets).
-   **Returns**: UTF-8 decoded text slice (trimming any split boundary UTF-8
    bytes, or Base64 prefixed with `[Base64-encoded binary data]` for binary
    data). If the object's OKF index is `stale`, returns the `Stale OKF index`
    warning with `Current object size:` and the requested live byte slice so you
    can inspect the updated payload and refresh the index.

### 5. Write OKF Tool (`write_okf`)

Writes a generated or refreshed OKF manifest into an object's GCS Object Context
(`okf.sum`, `okf.r.000..N`, `okf.src_generation`) with atomic
`if_generation_match` OCC protection.

-   **Args**: `gs_uri` (target GCS object URI), `summary` (concise Tier 0
    overview string, ~200-250 bytes optimal; must be non-empty, start with a
    letter or digit, and omit single quotes, double quotes, backslashes, and
    forward slashes per GCS Object Context rules), `regions_json` (JSON array
    string of region descriptors with `id`, `start_byte`, `end_byte`, `title`,
    `keywords` satisfying `0 <= start_byte <= end_byte < object_size`—using
    decompressed size for `content_encoding` `"gzip"`/`"x-gzip"`—in strictly
    ascending, non-overlapping order `next start_byte >= previous end_byte + 1`,
    e.g., `'[{"id": 0, "start_byte": 0, "end_byte": 8191, "title": "Schema",
    "keywords": ["parquet"]}]'`), and optional `created_at` (ISO 8601 UTC).
-   **Returns**: Success confirmation message or capacity/validation error.

--------------------------------------------------------------------------------

## Bucket-Level Discovery & Open-Ended Triage Workflows

Calling `peek_bucket` on a mixed-status bucket or prefix returns a structured
JSON card summarizing every object's OKF state:

```json
{"gs_uri": "gs://acme-telemetry/etl/", "bucket": "acme-telemetry", "prefix": "etl/",
 "total_objects": 3, "total_bytes": 100352, "truncated": false,
 "counts": {"fresh": 1, "stale": 1, "unindexed": 1},
 "candidates_to_index": ["gs://acme-telemetry/etl/watermark_checkpoint.json", "gs://acme-telemetry/etl/raw_clickstream_batch.ndjson"],
 "unindexed_uris": ["gs://acme-telemetry/etl/raw_clickstream_batch.ndjson"],
 "stale_uris": ["gs://acme-telemetry/etl/watermark_checkpoint.json"],
 "objects": [
   {"gs_uri": "gs://acme-telemetry/etl/spark_driver_2026.log", "name": "etl/spark_driver_2026.log", "size": 65536,
    "okf_status": "fresh", "needs_indexing": false, "region_count": 3,
    "summary": "Spark ETL driver log for analytics-east covering JVM GC initialization, shuffle throughput, and executor OOM traces.",
    "region_titles": ["JVM Heap & GC Initialization", "Shuffle Stage Throughput", "Executor OOM & ShuffleFetchFailedException"],
    "keywords": ["gc", "shuffle", "ShuffleFetchFailedException", "ExecutorRunner.scala"],
    "custom_contexts": {"cluster": "analytics-east", "job": "daily-rollup"}},
   {"gs_uri": "gs://acme-telemetry/etl/watermark_checkpoint.json", "name": "etl/watermark_checkpoint.json", "size": 2048,
    "okf_status": "stale", "needs_indexing": true,
    "stale_warning": "Stale OKF index: Index created for object generation 1710000000000000, but current generation is 1710050000000000."},
   {"gs_uri": "gs://acme-telemetry/etl/raw_clickstream_batch.ndjson", "name": "etl/raw_clickstream_batch.ndjson", "size": 32768,
    "okf_status": "unindexed", "needs_indexing": true}]}
```

### 1. Open-Ended Bucket Search & Root-Cause Triage

When the user asks an open-ended question about a bucket or prefix (for example,
*"Which job in `gs://acme-telemetry/etl/` failed with a shuffle error and what
was the root cause?"*):

1.  **Discover the Bucket Card Once**: Call `peek_bucket(gs_uri)` directly (or
    on large buckets with `> 30` objects when `invoke_subagent` is available,
    delegate `peek_bucket` to an isolated search sub-agent that returns only
    shortlisted matching URIs plus `counts` and `candidates_to_index`).
2.  **Filter Candidate Objects by Metadata**: Match the query against each
    object's `summary`, `region_titles`, `keywords`, `custom_contexts`, and
    `custom_metadata`. Eliminate unrelated distractor objects immediately.
3.  **Drill Down Without Redundant `peek` Calls**: For each matching `fresh`
    object, skip `peek(gs_uri)` and call `list_regions(gs_uri)` to obtain exact
    `[start_byte, end_byte]` offsets for the relevant region(s).
4.  **Read Only Matching Regions**: Call `expand(gs_uri, start_byte, end_byte)`
    only on the matching region(s). Never call `list_regions` or `expand` on
    unrelated distractor objects.

### 2. Bucket OKF Health Inventory & 20 KB Indexing Threshold

When the user asks for an inventory, readiness audit, or OKF status summary of a
bucket or prefix:

1.  **Zero-Byte Metadata Audit**: Call `peek_bucket(gs_uri)` and report the
    inventory directly from the card without calling `list_regions` or `expand`
    on any object.
2.  **Report Status Breakdown & Candidates**:
    -   Cite `total_objects`, `total_bytes`, and `counts` (`fresh`, `stale`,
        `unindexed`), plus each object's `gs_uri`, `size`, `okf_status`,
        `needs_indexing`, and `summary` / `region_titles` (or `stale_warning`).
    -   Explain the **`20 KB` (`20,480` bytes) indexing threshold**:
        `candidates_to_index` includes all `stale` objects, objects carrying
        `corrupt_warning`, and clean `unindexed` objects with `size >= 20480`.
        Clean uncompressed `unindexed` objects with `size < 20480` have
        `needs_indexing: false` because they fit in context via
        `expand(gs_uri, 0, size - 1)` (for `content_encoding` `"gzip"` or
        `"x-gzip"`, `size` is compressed, so decompressed size may exceed
        `20 KB`).

### 3. Cross-Object Comparative Progressive Disclosure

When comparing metrics, configurations, or sections across two or more objects
in a bucket (for example, comparing `analytics-east` vs. `analytics-west`
shuffle throughput):

1.  Call `peek_bucket(gs_uri)` to locate the target objects and verify their
    `region_titles` and `custom_contexts`.
2.  Call `list_regions` on each compared object to find the byte boundaries of
    the specific section being compared (e.g., the throughput region).
3.  Call `expand` only on that matching region in each compared object, omitting
    unrelated sections (such as initialization headers or crash dumps) and
    untouched distractor files.

### 4. Mixed-Bucket Non-Blocking Triage & Confirmation Gate

Real-world buckets frequently contain a mix of `fresh`, `stale`, small
`unindexed` (`< 20 KB`), and large `unindexed` (`>= 20 KB`) objects:

-   **Step 1 — Answer Immediately Without Blocking on Indexing**: Use relevant
    `fresh` objects via `list_regions` -> `expand`. For a relevant `unindexed`
    or `stale` object with `size < 20480` and no `"gzip"`/`"x-gzip"`
    `content_encoding`, read `expand(gs_uri, 0, size - 1)`. When
    `content_encoding` is `"gzip"` or `"x-gzip"` (`size` is compressed), read
    `expand(gs_uri, 0, 20479)` first and probe `start_byte = 20480` if EOF is
    not obvious; if data continues, treat it as `>= 20 KB`. For relevant
    `>= 20 KB` `unindexed` or `stale` objects, read targeted slices via `expand`
    without calling `write_okf`. Deliver the answer before indexing.
-   **Step 2 — Apply the Confirmation Gate for `candidates_to_index`**:
    -   **When Confirmation Is Required (Read/Search Queries)**: If the user has
        **not** explicitly requested or pre-approved generating/refreshing OKF
        indices, do **NOT** call `write_okf` (and do **NOT** `expand` unrelated
        `candidates_to_index` objects). After answering, report
        `candidates_to_index` (list all candidate URIs with `okf_status` and
        `size` when `<= 20` objects are non-fresh, or report exact counts plus
        up to `10` sample candidate URIs when `> 20` are non-fresh, noting why
        `< 20 KB` unindexed objects are excluded) and ask a single yes/no
        question offering to index or refresh them.
    -   **When Consent Is Already Granted (Explicit Indexing Requests)**: When
        the prompt explicitly asks to index, refresh, or generate OKF manifests
        for the bucket's candidates (or confirms `yes`), index **only**
        `candidates_to_index` (`stale`, `corrupt_warning`, and `>= 20 KB`
        `unindexed` objects), skipping `fresh` and clean `< 20 KB` `unindexed`
        objects unless small-file indexing was explicitly requested.
-   **Step 3 — Generate and Commit Grounded OKF Indices**: For each approved
    candidate, check `size` and `content_encoding` from `peek_bucket`. Read
    uncompressed `size < 20480` objects via `expand(gs_uri, 0, size - 1)`; for
    `size >= 20480` or `"gzip"`/`"x-gzip"` objects (reading `0..20479` first and
    probing `20480` if needed), inspect structural offsets via `expand`, build
    ascending non-overlapping regions bounded by decompressed object size
    (`0 <= start_byte <= end_byte < decompressed_size`), and commit via
    `write_okf(gs_uri, summary, regions_json)`.

--------------------------------------------------------------------------------

## Parallel Processing with Sub-Agents

When sub-agent delegation (`invoke_subagent`) is available, use sub-agents to
isolate large bucket discovery cards and parallelize batch indexing:

### Large-Bucket Search Context Isolation

For targeted search or cross-object triage on large buckets (`> 30` objects),
delegate `peek_bucket(gs_uri)` to an isolated search sub-agent via
`invoke_subagent` that returns only shortlisted matching URIs (with `okf_status`
and `region_titles`), `counts`, and `candidates_to_index` (or up to `10` sample
URIs when `> 20` are non-fresh), keeping the full bucket card out of the main
context. Call `peek_bucket` directly for full inventories or smaller prefixes.

### Bucket Discovery to Parallel Sub-Agent Batch Indexing

1.  **Discover Candidates via `peek_bucket`**: Call `peek_bucket(gs_uri)` once
    and read `candidates_to_index` (`stale`, `corrupt_warning`, and `>= 20 KB`
    `unindexed`).
2.  **Confirm Once for the Batch**: Unless the prompt already grants permission
    to index/refresh the candidates, report `candidates_to_index` (listing all
    when `<= 20` non-fresh, or counts + up to `10` sample URIs above `20`) and
    ask a single yes/no confirmation question before dispatching workers.
3.  **Partition & Dispatch (`invoke_subagent`)**:
    -   For `>= 3` candidates when `invoke_subagent` is available, partition
        `candidates_to_index` into balanced batches (5-10 URIs per worker) and
        dispatch parallel sub-agents with confirmation already granted.
    -   For 1-2 candidates (or without `invoke_subagent`), index each candidate
        URI directly (`expand` -> `write_okf`).
4.  **Verify Refresh**: Report the updated `write_okf` results (or re-run
    `peek_bucket(gs_uri)`) confirming the candidates are now `fresh`.

--------------------------------------------------------------------------------

## Telemetry & Attribution

The `google-cloud-storage-okf` MCP server automatically stamps outbound GCS API
calls with `User-Agent: gcs-skills/1.0 (skill:google-cloud-storage-okf)`.

--------------------------------------------------------------------------------

## Guardrails & Best Practices

-   **Always Start Bucket Tasks with `peek_bucket`**: Never poll `peek(gs_uri)`
    across every object in a bucket when `peek_bucket(gs_uri)` can aggregate the
    entire bucket or prefix in a single metadata listing.
-   **Never Call Redundant `peek` on `fresh` Objects**: When `peek_bucket`
    returns `okf_status: "fresh"` and `summary`, go straight to `list_regions`
    on matching objects.
-   **Never Bypass the MCP Server**: Only invoke OKF through the MCP tools
    (`peek_bucket`, `peek`, `list_regions`, `expand`, `write_okf`); never run or
    import `scripts/bucket.py`, `scripts/core.py`, or `scripts/mcp_server.py`
    directly or fall back to `gcloud storage` / `gsutil` (see **Setup**).
-   **Never Reconfigure the User's Environment**: On credential, project, or
    permission errors, relay the fix and stop; never change gcloud configuration
    or credentials yourself (see **Setup**).
-   **Confirm Before Indexing `candidates_to_index`**: Unless the user has
    explicitly requested or approved OKF indexing/refresh, answer read/search
    questions immediately (using `list_regions` -> `expand` on `fresh` objects
    and targeted `expand` slices on relevant `unindexed` objects), report
    `candidates_to_index`, and ask once for confirmation before calling
    `write_okf`.
-   **Never Overwrite Object Payload**: OKF operations only read byte ranges and
    write non-destructive Object Contexts (`custom_contexts`). Never delete or
    overwrite the original GCS objects.
-   **Respect Object Context Capacity Budgets**: Each GCS object supports at
    most `50` Object Context keys (~45-50 OKF regions), `25,600` total bytes,
    and `256` bytes per value.
