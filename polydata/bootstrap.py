"""Validate a token and initialize the chosen dataset; never print the token."""
from __future__ import annotations
import json
import os
from pathlib import Path
import uuid
from huggingface_hub import HfApi, CommitOperationAdd
from huggingface_hub.errors import RepositoryNotFoundError
from huggingface_hub.utils import validate_repo_id
from .worker import UPSTREAM, redact


def main() -> None:
    token = Path(os.getenv("HF_TOKEN_FILE", "/run/secrets/hf_token")).read_text().strip()
    if not token.startswith("hf_") or any(c.isspace() for c in token):
        raise ValueError("Invalid HF token format")
    api = HfApi(token=token)
    user = api.whoami()["name"]
    repo = os.getenv("HF_REPO_ID") or f"{user}/polymarket-l2"
    validate_repo_id(repo)
    try:
        api.repo_info(repo, repo_type="dataset")
    except RepositoryNotFoundError:
        try:
            api.create_repo(repo, repo_type="dataset", private=False, exist_ok=True)
        except Exception as exc:
            raise RuntimeError(f"Cannot create/access {repo}. For a repo-scoped token, first create that dataset on the HF website and authorize read/write on it. Set HF_REPO_ID for a different name.") from exc
    api.auth_check(repo, repo_type="dataset")  # v0.36 read check; real commit below checks write.
    operations = []
    info = api.repo_info(repo, repo_type="dataset")
    paths = {s.rfilename for s in info.siblings or []}
    if "README.md" not in paths:
        readme = """---
pretty_name: Polymarket PMXT feed archive
tags:
- finance
- orderbook
- polymarket
---
# Polymarket PMXT feed archive
Captured using https://github.com/zer946/Poly_data_to_everyone .

`raw/` retains all four PMXT feed event types. `payload` contains the exact
Redis message bytes, not the original WebSocket frame. `local_seq` is a
receiver sequence, NOT an exchange sequence. Read `health/` and `manifests/`
before using the data. No assertion of complete history or gap-free service.

Redis Pub/Sub is not replayable. REST snapshots cannot backfill lost L2.
State matching does not prove no intermediate event was lost. Book validation
uses a bounded whole-book cache and explicitly reports unchecked coverage.

Data is provided only subject to source/platform terms. The collector's code
license does not grant additional rights to the underlying market data.
"""
        operations.append(CommitOperationAdd(path_in_repo="README.md", path_or_fileobj=readme.encode()))
    # A real write test; harmless source metadata, never credentials or host/IP.
    name = f"metadata/deployment-{uuid.uuid4().hex}.json"
    operations.append(CommitOperationAdd(path_in_repo=name,
                      path_or_fileobj=json.dumps({"upstream_commit": UPSTREAM, "schema_version": 1}).encode()))
    commit = api.create_commit(repo, repo_type="dataset", operations=operations,
                               commit_message="Initialize collector metadata", num_threads=1)
    if not api.get_paths_info(repo, [name], repo_type="dataset", revision=commit.oid):
        raise RuntimeError("Read-after-write validation failed")
    print(repo)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        import sys
        print(redact(exc), file=sys.stderr)
        raise SystemExit(1)
