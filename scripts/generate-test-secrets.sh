#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SECRETS_FILE="${SECRETS_FILE:-${REPO_ROOT}/.local/test-secrets.yml}"

mkdir -p "$(dirname "${SECRETS_FILE}")"
chmod 0700 "$(dirname "${SECRETS_FILE}")"

if [[ -f "${SECRETS_FILE}" ]]; then
  echo "Secrets already present at ${SECRETS_FILE} - reusing them (passwords must stay stable across runs)."
  exit 0
fi

gen() { LC_ALL=C tr -dc 'A-Za-z0-9' </dev/urandom | head -c 32; }

umask 077
cat >"${SECRETS_FILE}" <<EOF
---
kafka_scram_users:
  - name: kafka_admin
    password: "$(gen)"
  - name: kafka_broker
    password: "$(gen)"
  - name: clickhouse
    password: "$(gen)"

kafka_tls_keystore_password: "$(gen)"
EOF

chmod 0600 "${SECRETS_FILE}"

echo "Wrote ${SECRETS_FILE} ($(wc -c <"${SECRETS_FILE}") bytes)"
echo "Principals: $(grep -c '  - name:' "${SECRETS_FILE}")"
