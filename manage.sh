#!/usr/bin/env bash
set -Eeuo pipefail
cd "$(dirname "$0")"
case "${1:-status}" in
  status) docker compose ps; docker stats --no-stream; docker compose exec -T worker python -m polydata.worker status ;;
  logs) docker compose logs --tail=100 -f ;;
  verify) docker compose exec -T worker python -m polydata.worker verify-local ;;
  stop) docker compose stop feed; docker compose stop receiver; docker compose stop worker discovery redis ;;
  start) docker compose up -d --no-build ;;
  *) echo 'Usage: bash manage.sh status|logs|verify|stop|start' >&2; exit 2 ;;
esac
