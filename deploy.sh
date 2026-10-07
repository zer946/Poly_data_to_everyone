#!/usr/bin/env bash
# Run as root on Debian 12/13 or Ubuntu. Token is read from /dev/tty, not argv.
set -Eeuo pipefail
umask 077
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"
UPSTREAM_SHA=cb0f6631556bf460d03594fe20f9bbd020b47d19
[[ "$EUID" -eq 0 ]] || { echo 'Run: sudo bash deploy.sh' >&2; exit 1; }
[[ "$(uname -m)" == x86_64 ]] || { echo 'This small-VPS profile is tested/configured for x86_64.' >&2; exit 1; }
command -v apt-get >/dev/null || { echo 'Debian/Ubuntu apt is required.' >&2; exit 1; }
apt-get update -qq
apt-get install -y --no-install-recommends ca-certificates curl git python3
if ! command -v docker >/dev/null; then
  # Use Docker's official apt repository, not a downloaded shell script.
  . /etc/os-release
  case "$ID" in debian|ubuntu) ;; *) echo 'Unsupported distribution.' >&2; exit 1;; esac
  install -m 0755 -d /etc/apt/keyrings
  curl --fail --show-error --silent --location "https://download.docker.com/linux/$ID/gpg" -o /etc/apt/keyrings/docker.asc
  chmod a+r /etc/apt/keyrings/docker.asc
  printf 'deb [arch=amd64 signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/%s %s stable\n' "$ID" "$VERSION_CODENAME" > /etc/apt/sources.list.d/docker.list
  apt-get update -qq
  apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
fi
systemctl enable --now docker
# Existing Docker installations are not removed or silently replaced.
docker compose version >/dev/null || { echo 'Install the Docker Compose v2 plugin, then rerun.' >&2; exit 1; }
docker buildx version >/dev/null || { echo 'Install the Docker buildx plugin, then rerun.' >&2; exit 1; }
FREE=$(df --output=avail -B1 "$ROOT" | tail -1 | tr -d ' ')
(( FREE >= 10*1024*1024*1024 )) || { echo 'At least 10 GiB free disk is required for first build and data reserve.' >&2; exit 1; }
MEM=$(awk '/MemTotal/{print $2}' /proc/meminfo)
(( MEM >= 850000 )) || { echo 'Less than approximately 850 MiB RAM: this profile is not supported.' >&2; exit 1; }
# A dedicated, documented build swap file; existing swap is never removed.
SWAP=$(awk '/SwapTotal/{print $2}' /proc/meminfo)
if (( MEM < 2000000 && SWAP < 2000000 )); then
  SWAPFILE="$ROOT/.build-swap"
  if [[ ! -e "$SWAPFILE" ]]; then
    dd if=/dev/zero of="$SWAPFILE" bs=1M count=2048 status=none
    chmod 600 "$SWAPFILE"
    mkswap "$SWAPFILE" >/dev/null
  fi
  swapon --show=NAME --noheadings | grep -Fxq "$SWAPFILE" || swapon "$SWAPFILE"
  grep -Fq "$SWAPFILE none swap" /etc/fstab || printf '%s none swap sw 0 0\n' "$SWAPFILE" >> /etc/fstab
fi
if [[ ! -d .upstream/.git ]]; then
  git clone --filter=blob:none https://github.com/pmxt-dev/polymarket-orderbook-collector.git .upstream
fi
[[ -z "$(git -C .upstream status --porcelain)" ]] || { echo '.upstream has local changes; refusing to overwrite.' >&2; exit 1; }
git -C .upstream fetch --depth 1 origin "$UPSTREAM_SHA"
git -C .upstream checkout --detach "$UPSTREAM_SHA"
[[ "$(git -C .upstream rev-parse HEAD)" == "$UPSTREAM_SHA" ]] || exit 1
mkdir -p .secrets data
chmod 700 .secrets data
if [[ ! -s .secrets/hf_token || "${RESET_HF_TOKEN:-0}" == 1 ]]; then
  IFS= read -r -s -p 'Hugging Face token (hidden): ' TOKEN </dev/tty
  printf '\n' >/dev/tty
  [[ "$TOKEN" =~ ^hf_[A-Za-z0-9]+$ ]] || { echo 'Invalid token format.' >&2; exit 1; }
  printf '%s' "$TOKEN" > .secrets/hf_token
  unset TOKEN
fi
chmod 600 .secrets/hf_token
if [[ ! -f .env ]]; then cp .env.example .env; fi
chmod 600 .env
# An isolated buildx builder prevents clearing unrelated images/build caches.
BUILDER="polydata-setup-$$"
cleanup_builder() { docker buildx rm "$BUILDER" >/dev/null 2>&1 || true; }
trap cleanup_builder EXIT
docker buildx create --name "$BUILDER" --driver docker-container >/dev/null
# Build serially. PMXT source is unchanged; only release build parallelism differs.
docker buildx build --builder "$BUILDER" --load --target worker -t polydata-worker:local .
# Token-only default: <your HF username>/polymarket-l2. A precreated repo-scoped
# token must target that repo; set HF_REPO_ID to select another existing dataset.
REPO=$(docker compose run --rm --no-deps -e "HF_REPO_ID=${HF_REPO_ID:-$(sed -n 's/^HF_REPO_ID=//p' .env)}" worker python -m polydata.bootstrap)
[[ "$REPO" =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ ]] || { echo 'Could not resolve HF dataset.' >&2; exit 1; }
export REPO
python3 - <<'PY'
import os
from pathlib import Path
p=Path('.env'); lines=[x for x in p.read_text().splitlines() if not x.startswith('HF_REPO_ID=')]
p.write_text('HF_REPO_ID='+os.environ['REPO']+'\n'+'\n'.join(lines)+'\n');p.chmod(0o600)
PY
docker buildx build --builder "$BUILDER" --load --target receiver -t polydata-receiver:local .
docker buildx build --builder "$BUILDER" --load --target pmxt -t polydata-pmxt:local .
docker buildx build --builder "$BUILDER" --load -f .upstream/services/polymarket/polymarket-active-markets/Dockerfile -t polydata-discovery:local .upstream
cleanup_builder
trap - EXIT
docker compose config --quiet
# Do not stop existing receivers on rerun: Compose reuses unchanged containers.
docker compose up -d --no-build --wait --wait-timeout 900
printf '\nDeployment services started. Dataset: https://huggingface.co/datasets/%s\n' "$REPO"
printf 'Check actual throughput, swap and gaps: bash manage.sh status\n'
printf '1 GiB is an experimental profile, not a guarantee of gap-free full-market collection.\n'
