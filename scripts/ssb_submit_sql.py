#!/usr/bin/env python3.11
"""Submit Flink SQL files via Cloudera SQL Stream Builder (SSB) REST API.

CSA clusters often ship without a working flink-sql-client. SSB exposes the same
execution path as the Web UI through POST /sql/execute.

Usage (repo root, after kinit):
  python3.11 scripts/ssb_submit_sql.py flink/ltas_5min.sql
  python3.11 scripts/ssb_submit_sql.py --list-projects
  python3.11 scripts/ssb_submit_sql.py --list-jobs

Environment (.env or shell):
  SSB_API_BASE   e.g. https://host:8082/ssb/api/v1  (from SSB API Explorer)
  SSB_PROJECT_ID optional project scope (numeric id from SSB UI / --list-projects)
  SSB_CA_CERT    TLS CA bundle (jshin Auto-TLS pem)
  SSB_AUTH       kerberos (default) | basic
  SSB_USER/SSB_PASSWORD  for basic auth
  SSB_JOB_NAME   override Flink job name for INSERT statements
  SSB_PARALLELISM default 1
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CATALOG = REPO_ROOT / "flink" / "conf" / "00_catalog_setup_jshin.sql"


def load_dotenv() -> None:
    env_file = REPO_ROOT / ".env"
    if not env_file.is_file():
        return
    for raw in env_file.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key.strip(), value)


def cfg(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def strip_sql_comments(text: str) -> str:
    lines: list[str] = []
    for line in text.splitlines():
        if line.strip().startswith("--"):
            continue
        lines.append(line)
    return "\n".join(lines)


def split_statements(sql: str) -> list[str]:
    sql = strip_sql_comments(sql)
    if not sql.strip():
        return []

    statements: list[str] = []
    buf: list[str] = []
    for line in sql.splitlines():
        buf.append(line)
        if line.rstrip().endswith(";"):
            stmt = "\n".join(buf).strip()
            if stmt:
                statements.append(stmt)
            buf = []

    tail = "\n".join(buf).strip()
    if tail:
        if not tail.rstrip().endswith(";"):
            tail = tail.rstrip() + ";"
        statements.append(tail)
    return statements


def curl_request(
    method: str,
    url: str,
    *,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ca_cert = cfg("SSB_CA_CERT", cfg("IMPALA_SSL_CA_CERT"))
    auth = cfg("SSB_AUTH", "kerberos").lower()

    cmd = ["curl", "-sS", "-X", method, "-H", "Accept: application/json"]
    if ca_cert:
        cmd.extend(["--cacert", ca_cert])
    else:
        cmd.append("-k")

    body_path: str | None = None
    if payload is not None:
        cmd.extend(["-H", "Content-Type: application/json"])
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as fh:
            json.dump(payload, fh)
            body_path = fh.name
        cmd.extend(["-d", f"@{body_path}"])

    if auth == "kerberos":
        cmd.extend(["--negotiate", "-u", ":"])
    elif auth == "basic":
        user = cfg("SSB_USER")
        password = cfg("SSB_PASSWORD")
        if not user:
            sys.exit("ERROR: SSB_AUTH=basic requires SSB_USER and SSB_PASSWORD")
        cmd.extend(["-u", f"{user}:{password}"])
    else:
        sys.exit(f"ERROR: unsupported SSB_AUTH={auth!r} (use kerberos or basic)")

    cmd.append(url)

    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    finally:
        if body_path:
            os.unlink(body_path)

    stdout = proc.stdout.strip()
    stderr = proc.stderr.strip()

    if proc.returncode != 0:
        raise RuntimeError(
            f"curl failed ({proc.returncode}) for {url}\n"
            f"stderr: {stderr}\nstdout: {stdout}"
        )

    if not stdout:
        return {}

    try:
        return json.loads(stdout)
    except json.JSONDecodeError:
        return {"raw": stdout}


def api_base() -> str:
    base = cfg("SSB_API_BASE")
    if not base:
        sys.exit(
            "ERROR: SSB_API_BASE is not set.\n"
            "  Open SSB Web UI → API Explorer and copy the base URL, e.g.\n"
            "  https://<host>:8082/ssb/api/v1\n"
            "  Add to .env: SSB_API_BASE=..."
        )
    return base.rstrip("/") + "/"


def api_url(path: str) -> str:
    return urljoin(api_base(), path.lstrip("/"))


def project_scoped_url(path: str) -> str:
    project_id = cfg("SSB_PROJECT_ID")
    clean = path.lstrip("/")
    if project_id:
        return urljoin(api_base(), f"projects/{project_id}/{clean}")
    return urljoin(api_base(), clean)


def list_projects() -> None:
    base = cfg("SSB_API_BASE").rstrip("/")
    candidates = [api_url("projects")]
    if "/v1" in base:
        candidates.insert(0, base.replace("/v1", "/v2") + "/projects")
    last_err: Exception | None = None
    for url in candidates:
        try:
            data = curl_request("GET", url)
            print(json.dumps(data, indent=2))
            return
        except RuntimeError as exc:
            last_err = exc
    if last_err:
        raise last_err


def list_jobs() -> None:
    data = curl_request("GET", api_url("jobs"))
    print(json.dumps(data, indent=2))


def resolve_sql_files(paths: list[str]) -> list[Path]:
    files: list[Path] = []
    for raw in paths:
        p = Path(raw)
        if not p.is_file():
            p = REPO_ROOT / raw
        if not p.is_file():
            sys.exit(f"ERROR: SQL file not found: {raw}")
        files.append(p.resolve())
    return files


def build_script(files: list[Path]) -> str:
    names = {f.name for f in files}
    parts: list[str] = []
    if files and names.isdisjoint({"00_catalog_setup_jshin.sql", "00_catalog_setup.sql"}):
        if DEFAULT_CATALOG.is_file():
            parts.append(DEFAULT_CATALOG.read_text(encoding="utf-8"))
    for f in files:
        parts.append(f.read_text(encoding="utf-8"))
    return "\n\n".join(parts)


def job_name_from_path(path: Path) -> str:
    override = cfg("SSB_JOB_NAME")
    if override:
        return override
    stem = path.stem.replace("_", "-")
    return f"kdap-{stem}"


def execute_statement(sql: str, *, job_name: str | None) -> dict[str, Any]:
    payload: dict[str, Any] = {"sql": sql}
    if job_name and re.match(r"^\s*INSERT\b", sql, re.IGNORECASE):
        payload["job_config"] = {
            "job_name": job_name,
            "runtime_config": {
                "execution_mode": "SESSION",
                "parallelism": int(cfg("SSB_PARALLELISM", "1")),
                "start_with_savepoint": False,
            },
        }
    return curl_request("POST", project_scoped_url("sql/execute"), payload=payload)


def submit_files(paths: list[str], *, dry_run: bool) -> None:
    files = resolve_sql_files(paths)
    script = build_script(files)
    statements = split_statements(script)
    if not statements:
        sys.exit("ERROR: no SQL statements found in input files")

    primary = files[-1]
    job_name = job_name_from_path(primary)

    print(f"SSB API: {api_base().rstrip('/')}")
    if cfg("SSB_PROJECT_ID"):
        print(f"Project: {cfg('SSB_PROJECT_ID')}")
    print(f"Statements: {len(statements)} (job name for INSERT: {job_name})")

    for idx, stmt in enumerate(statements, start=1):
        preview = re.sub(r"\s+", " ", stmt)[:120]
        print(f"\n[{idx}/{len(statements)}] {preview}...")
        if dry_run:
            continue
        name = job_name if idx == len(statements) else None
        resp = execute_statement(stmt, job_name=name)
        print(json.dumps(resp, indent=2))
        if resp.get("type") == "job" or "job_id" in resp or "ssb_job_id" in resp:
            print("\nJob submitted. Check SSB Web UI → Jobs or Flink Dashboard.")


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Submit Flink SQL via SSB REST API")
    parser.add_argument("sql_files", nargs="*", help="SQL file(s), catalog auto-prepended for job files")
    parser.add_argument("--list-projects", action="store_true", help="GET /projects and exit")
    parser.add_argument("--list-jobs", action="store_true", help="GET /jobs and exit")
    parser.add_argument("--dry-run", action="store_true", help="Parse and print statements only")
    args = parser.parse_args()

    if args.list_projects:
        list_projects()
        return
    if args.list_jobs:
        list_jobs()
        return
    if not args.sql_files:
        parser.error("provide at least one SQL file, or use --list-projects / --list-jobs")

    submit_files(args.sql_files, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
