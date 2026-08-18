#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
KEY_PATH="${KEY_PATH:-${REPO_ROOT}/.local/kafka-test}"

mkdir -p "$(dirname "${KEY_PATH}")"
chmod 0700 "$(dirname "${KEY_PATH}")"

if [[ -f "${KEY_PATH}" ]]; then
  echo "SSH key already present at ${KEY_PATH} - reusing it."
else
  ssh-keygen -t ed25519 -N '' -C "ansible-role-kafka ephemeral test key" -f "${KEY_PATH}" >/dev/null
  echo "Generated ED25519 keypair at ${KEY_PATH}"
fi

chmod 0600 "${KEY_PATH}"
chmod 0644 "${KEY_PATH}.pub"

ssh-keygen -lf "${KEY_PATH}.pub"
