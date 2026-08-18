#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

banner() { printf '\n\033[1m== %s ==\033[0m\n' "$1"; }
fail() { printf '\n\033[31mFAILED: %s\033[0m\n' "$1" >&2; exit 1; }

banner "Checking local dependencies"
for tool in terraform ansible-playbook ansible-galaxy python3 ssh-keygen curl; do
  command -v "${tool}" >/dev/null 2>&1 || fail "${tool} is not installed"
  printf '  %-18s %s\n' "${tool}" "$(command -v "${tool}")"
done

banner "Generating local material"
./scripts/create-ssh-key.sh
./scripts/generate-test-secrets.sh
./scripts/generate-cluster-identity.sh

banner "Provisioning infrastructure"
terraform -chdir=terraform init -input=false
terraform -chdir=terraform apply -input=false -auto-approve

banner "Rendering the Ansible inventory"
python3 scripts/render-inventory.py

banner "Installing Ansible collections"
ansible-galaxy collection install -r requirements.yml -p .ansible/collections

banner "Waiting for SSH"
for _ in $(seq 1 30); do
  if ansible -i inventory/generated.yml kafka -m ansible.builtin.ping >/dev/null 2>&1; then
    echo "  all nodes reachable"
    break
  fi
  sleep 10
done

banner "Deploying Kafka"
ansible-playbook playbooks/kafka.yml

banner "Verifying the cluster"
ansible-playbook playbooks/verify.yml

banner "Testing idempotency"
IDEM_LOG="$(mktemp)"
ansible-playbook playbooks/kafka.yml | tee "${IDEM_LOG}"

if grep -qE 'changed=[1-9]' "${IDEM_LOG}"; then
  grep -E 'changed=[1-9]' "${IDEM_LOG}" >&2
  rm -f "${IDEM_LOG}"
  fail "second run reported changes; the role is not idempotent"
fi
if grep -qE 'failed=[1-9]' "${IDEM_LOG}"; then
  rm -f "${IDEM_LOG}"
  fail "second run reported failures"
fi
rm -f "${IDEM_LOG}"
echo "  changed=0 and failed=0 on every node"

banner "Verifying Kafka ports are not publicly reachable"
mapfile -t PUBLIC_IPS < <(terraform -chdir=terraform output -json public_ips | python3 -c 'import json,sys;[print(i) for i in json.load(sys.stdin)]')
for ip in "${PUBLIC_IPS[@]}"; do
  timeout 8 bash -c "exec 3<>/dev/tcp/${ip}/22" 2>/dev/null \
    || fail "positive control failed: SSH unreachable on ${ip}, so the port probe proves nothing"
  for port in 9092 9093 9404; do
    if timeout 8 bash -c "exec 3<>/dev/tcp/${ip}/${port}" 2>/dev/null; then
      fail "${ip}:${port} is publicly reachable"
    fi
  done
  echo "  ${ip}: 22 open (control), 9092/9093/9404 closed"
done

banner "End-to-end test passed"
cat <<EOF

  Deployed, verified, and proved idempotent.

  The infrastructure is still running. To tear it down:

      terraform -chdir=terraform destroy
      rm -rf .local

EOF
