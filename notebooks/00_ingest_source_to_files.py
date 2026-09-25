"""Stage 00 -- Ingest: source system -> OneLake Lakehouse Files (landing).

Lands raw CMS Open Payments extracts in the Lakehouse ``Files`` area untouched,
and writes a manifest describing what arrived. Nothing is parsed or reshaped
here: the landing zone is the immutable record of what the source gave us, which
is what lets CAS re-run any downstream layer against the exact bytes tested.

Orchestrated by: Fabric Data Factory pipeline activity ``Ingest_Source_To_Files``.

Parameters (injected by Data Factory):
    env         -- ``local`` | ``fabric``
    pipeline    -- pipeline config name in config/pipelines/
    run_id      -- Data Factory pipeline run id, for lineage correlation

TODO(stage-00): point ``paths.source`` at the real upstream (ADLS Gen2 shortcut
or SFTP drop) once the source system is confirmed; today it reads the manually
downloaded CMS bulk files.
"""

# ---------------------------------------------------------------- PARAMETERS
env = "local"
pipeline = "ingest_open_payments"
run_id = ""
# ------------------------------------------------------------ END PARAMETERS

import hashlib
import os
import re
import shutil
from pathlib import Path

from cas_audit import config as cfg
from cas_audit.runtime import exit_notebook, get_logger, new_run_id

log = get_logger("stage00.ingest")

# CMS bulk file naming: OP_DTL_<DATASET>_PGYR<year>_P<publish>_<submit>.csv
FILENAME_RE = re.compile(
    r"^OP_DTL_(?P<dataset>GNRL|RSRCH|OWNRSHP)_PGYR(?P<program_year>\d{4})_P(?P<published>\d{8})_(?P<submitted>\d{8})\.csv$"
)


def discover(source_dir: str) -> list[dict]:
    """Catalogue the source files, rejecting anything that does not match the
    CMS naming contract. An unparseable filename means we cannot attribute the
    data to a program year, so it must not silently enter the lake."""
    found, rejected = [], []
    for entry in sorted(Path(source_dir).glob("*")):
        if not entry.is_file():
            continue
        match = FILENAME_RE.match(entry.name)
        if not match:
            rejected.append(entry.name)
            continue
        found.append({
            "file_name": entry.name,
            "abs_path": str(entry),
            "size_bytes": entry.stat().st_size,
            **match.groupdict(),
        })
    if rejected:
        log.warning("Ignored %d non-conforming file(s): %s", len(rejected), rejected)
    return found


def checksum(path: str, limit_bytes: int = 64 * 1024 * 1024) -> str:
    """Hash the leading bytes as a cheap change-detection fingerprint.

    Full-file hashing of a 9 GB extract is not worth the IO on every run; the
    header block plus size is enough to detect a re-publication.
    """
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        h.update(fh.read(limit_bytes))
    return h.hexdigest()


def make_dev_extract(record: dict, files_root: str, rows: int) -> str:
    """Write a head-N extract for local iteration.

    Copies whole physical lines only. Because CMS embeds newlines inside quoted
    fields, a byte-truncated file could end mid-record and would not parse; taking
    complete lines keeps the extract a valid CSV.
    """
    stem = record["file_name"].removesuffix(".csv")
    target = os.path.join(files_root, f"{stem}__dev{rows}.csv")
    if os.path.exists(target):
        return target
    os.makedirs(files_root, exist_ok=True)
    with open(record["abs_path"], "r", encoding="utf-8", errors="replace", newline="") as src, \
         open(target, "w", encoding="utf-8", newline="") as dst:
        for i, line in enumerate(src):
            if i > rows:
                break
            dst.write(line)
    return target


def land(record: dict, files_root: str, mode: str) -> str:
    """Place one source file into the landing zone.

    ``copy``    -- real copy, used in Fabric where source and lake differ.
    ``symlink`` -- local default; avoids duplicating ~20 GB on a laptop while
                   keeping downstream paths identical.
    """
    os.makedirs(files_root, exist_ok=True)
    target = os.path.join(files_root, record["file_name"])
    if mode == "symlink":
        if os.path.lexists(target):
            os.remove(target)
        os.symlink(record["abs_path"], target)
    else:
        if not os.path.exists(target) or os.path.getsize(target) != record["size_bytes"]:
            shutil.copy2(record["abs_path"], target)
    return target


def main() -> dict:
    rid = run_id or new_run_id()
    env_cfg = cfg.EnvConfig.load(env)
    pipe_cfg = cfg.PipelineConfig.load(pipeline)

    source_dir = env_cfg.path("source")
    files_root = env_cfg.path("files")
    land_mode = pipe_cfg.get("land_mode", "symlink" if env_cfg.env == "local" else "copy")

    log.info("run_id=%s env=%s source=%s -> files=%s (mode=%s)",
             rid, env_cfg.env, source_dir, files_root, land_mode)

    records = discover(source_dir)
    if not records:
        raise FileNotFoundError(f"No conforming source files under {source_dir}")

    dev_rows = int(env_cfg.sampling.get("dev_extract_rows", 0) or 0)

    manifest = []
    for record in records:
        target = land(record, files_root, land_mode)
        record.update({
            "landed_path": target,
            "content_fingerprint": checksum(record["abs_path"]),
            "run_id": rid,
        })
        # Local only: downstream stages read the extract, but the manifest keeps
        # the full-file path so the record of what arrived stays truthful.
        if dev_rows and env_cfg.env == "local":
            record["full_path"] = target
            record["landed_path"] = make_dev_extract(record, files_root, dev_rows)
            record["dev_extract_rows"] = dev_rows
        manifest.append(record)
        log.info("landed %-52s %7.2f GB  dataset=%s PY=%s",
                 record["file_name"], record["size_bytes"] / 1e9,
                 record["dataset"], record["program_year"])

    # The manifest is the handoff contract to stage 01 -- it reads this rather
    # than globbing the landing zone, so a partial arrival cannot be processed.
    manifest_path = os.path.join(files_root, "_manifest", f"manifest_{rid}.json")
    os.makedirs(os.path.dirname(manifest_path), exist_ok=True)
    import json
    with open(manifest_path, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2)

    result = {
        "stage": "00_ingest",
        "run_id": rid,
        "files_landed": len(manifest),
        "bytes_landed": sum(m["size_bytes"] for m in manifest),
        "manifest_path": manifest_path,
        "program_years": sorted({m["program_year"] for m in manifest}),
        "datasets": sorted({m["dataset"] for m in manifest}),
    }
    log.info("landed %d file(s), %.2f GB", result["files_landed"], result["bytes_landed"] / 1e9)
    return result


if __name__ == "__main__":
    exit_notebook(main())
