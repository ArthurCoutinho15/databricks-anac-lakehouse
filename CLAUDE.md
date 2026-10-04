# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

This is a personal study project (preparing for a mid-level data engineer role) that implements
a full Lakehouse on Databricks for Brazilian civil aviation on-time performance data. The complete
spec — business context, data sources, per-layer requirements, phased checklist, and ADR/decision
log — lives in `DESAFIO.md` at the repo root. Read it before planning any non-trivial change; it is
the source of truth for scope and requirements, not this file.

The actual code lives in `anac_lakehouse/`, a single Databricks Asset Bundle (DAB) project.

## Commands

Run from inside `anac_lakehouse/`:

```bash
uv sync --dev                    # install dependencies
uv run pytest                    # run tests
uv run pytest path/to_test.py::test_name   # run a single test
uv run ruff check .              # lint
uv run ruff format .             # format
uv build --wheel                 # build the wheel manually (rarely needed, see below)

databricks bundle validate -t dev
databricks bundle deploy -t dev        # or -t prod
databricks bundle run vra_ingestion_job -t dev
```

Note: `tests/conftest.py` eagerly creates a `DatabricksSession` (via `databricks-connect`) for every
test run, falling back to serverless compute if no cluster/profile is configured. Running tests
requires a reachable, authenticated Databricks workspace — there is no local/mocked Spark fallback.

## Architecture

### Bundle layout quirk: `src/` is flat, but the installed package is `anac_lakehouse`

Code lives directly under `anac_lakehouse/src/` (`src/clients/`, `src/lakehouse/`, `src/pipelines/`),
with `src/__init__.py` at the top. There is **no** `src/anac_lakehouse/` directory. The wheel build
remaps this at build time in `pyproject.toml`:

```toml
[tool.hatch.build.targets.wheel]
packages = ["src"]

[tool.hatch.build.targets.wheel.sources]
"src" = "anac_lakehouse"
```

This means every internal import must be written as `anac_lakehouse.<subpackage>...` (e.g.
`from anac_lakehouse.clients.anac_vra_client import AnacVraBaseClient`) — never `src.xxx` or
`anac_lakehouse.src.xxx`. Editors/linters will flag these imports as unresolved because the
directory on disk is literally named `src`; that warning is expected and not a bug (it would take
an editable install to silence it). Keep this remap in mind before moving files around — it's the
most common source of `ModuleNotFoundError` in this project.

### Wheel build is automatic

`databricks.yml` declares an `artifacts` block that runs `uv build --wheel` on every
`bundle deploy`. Don't hand-build and commit a wheel into the deploy flow — `uv build --wheel` is
only for local inspection (e.g. `unzip -l dist/*.whl` to check what got packaged).

### Jobs run as serverless `python_wheel_task`s

Each job resource (`resources/jobs/*.job.yml`) uses `environment_key` + an `environments:` block
with `environment_version` (the modern field — `client` is deprecated and some workspaces reject
it outright). The Python version that ships with a given `environment_version` must be compatible
with `requires-python` in `pyproject.toml`, or library installation fails at job runtime.
`environment_version: "2"` currently in use = Python 3.11.

Entry points declared under `[project.scripts]` in `pyproject.toml` are what
`python_wheel_task.entry_point` resolves. Databricks loads and calls the entry point with **zero
arguments** (`entry[0].load()()`), so job `parameters` arrive via `sys.argv` — any argument parsing
must happen inside the function itself, not behind an `if __name__ == "__main__":` guard (which
never runs when the module is imported as an installed entry point rather than executed as a
script).

### Unity Catalog as code

`resources/infra/schemas.yml` and `volumes.yml` declare the catalog's schemas (`landing`, `system`,
`bronze`, `silver`, `gold`) and Volumes, parameterized by `${var.catalog}`. Per
`databricks.yml`, this project deliberately uses **one shared catalog** (`anac`) for both targets;
dev/prod isolation is by schema name only (`${workspace.current_user.short_name}` for dev, `prod`
for prod) — not by separate catalogs as `DESAFIO.md`'s suggested structure shows. This was a
conscious simplification for the Free Edition workspace, not an oversight.

### Ingestion pattern: per-source client + generic bronze pipeline

- **Source-specific clients** (`src/clients/`, e.g. `AnacVraBaseClient`) encapsulate scraping/download
  logic that genuinely differs per source — for ANAC VRA this means recursively crawling an
  Apache-style directory index (year → month subfolders) and partitioning downloaded files
  Hive-style (`ano=YYYY/mes=MM/`) based on a year/month parsed out of the filename.
- **Generic bronze ingestion** (`src/pipelines/landing_to_bronze.py`, `LandingToBronze`) is a single
  reusable Auto Loader–based Structured Streaming pipeline, configured via a small dataclass
  (`src/pipelines/models/l2b.py`, `LandingToBronzeConfig`) rather than subclassed per source — the
  Auto Loader mechanics (`cloudFiles`, schema evolution, `availableNow` trigger, ingestion metadata
  columns `_source_file`/`_ingested_at`/`_row_hash`) are identical across every landing source, only
  the config (paths, format, reader options) changes. Don't create a new pipeline class per source;
  add a new `LandingToBronzeConfig` instead.
- This split is intentional and should not be collapsed into one "abstract ingestion client" — the
  per-source download/listing logic is too heterogeneous across sources (file-index crawling vs.
  REST APIs with watermarks/pagination) to share a common base class profitably, while the
  landing→bronze mechanics are uniform enough to warrant one shared, config-driven class.

### ANAC VRA source specifics

- Real index path (discovered by crawling, not documented anywhere obvious):
  `Voos e operações aéreas/Voo Regular Ativo (VRA)/{ano}/{MM - NomeMês}/VRA_{ano}{mês}.csv`
  (month is **not** zero-padded in the folder name's leading number for single digits in some years —
  always verify against the live index rather than hardcoding a guessed path).
- CSVs have a one-line metadata preamble (`Atualizado em: <date>`) before the real header row —
  needs `.option("skipRows", 1)` (this is a Databricks-specific CSV reader option, not available as
  a named kwarg on `DataFrameReader.csv()`; use `.format("csv").option(...).load(...)` instead).
- Delimiter is `;`, fields are quoted, encoding is UTF-8 with BOM for recent years — but
  `DESAFIO.md` notes older years may be latin-1, so don't assume one encoding across the whole
  history.
- Cancelled flights have the literal string `"null"` (not a real NULL) in the actual-time columns.
