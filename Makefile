.DEFAULT_GOAL := help
SHELL := /bin/bash

TF = terraform -chdir=terraform

.PHONY: help ssh-key secrets identity tf-init tf-plan tf-apply inventory deps \
        deploy verify idempotence rolling-restart lint test-e2e destroy clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

ssh-key: ## Generate the ephemeral ED25519 keypair (outside Terraform)
	./scripts/create-ssh-key.sh

secrets: ## Generate SCRAM passwords into .local (once; stable across runs)
	./scripts/generate-test-secrets.sh

identity: ## Generate the cluster ID and controller directory UUIDs (once, immutable)
	./scripts/generate-cluster-identity.sh

tf-init: ## terraform init
	$(TF) init -input=false

tf-plan: ssh-key ## Show the infrastructure plan
	$(TF) plan -input=false

tf-apply: ssh-key ## Create the ephemeral three-node AWS environment
	$(TF) apply -input=false -auto-approve

inventory: ## Render inventory/generated.yml from terraform outputs
	python3 scripts/render-inventory.py

deps: ## Install the required Ansible collections
	ansible-galaxy collection install -r requirements.yml -p .ansible/collections

deploy: ## Deploy Kafka (parallel converge, then serial:1 rolling restart)
	ansible-playbook playbooks/kafka.yml

verify: ## Functional verification: TLS, auth, quorum, topic, produce/consume, metrics
	ansible-playbook playbooks/verify.yml

idempotence: ## Re-run the deploy and assert changed=0 on every node
	@set -o pipefail; \
	log=$$(mktemp); \
	ansible-playbook playbooks/kafka.yml | tee $$log; \
	if grep -qE 'changed=[1-9]|failed=[1-9]' $$log; then \
		echo; echo "NOT IDEMPOTENT:"; grep -E 'changed=[1-9]|failed=[1-9]' $$log; \
		rm -f $$log; exit 1; \
	fi; \
	rm -f $$log; echo; echo "changed=0 and failed=0 on every node"

rolling-restart: ## Restart every broker one at a time, with a health gate between
	ansible-playbook playbooks/rolling_restart.yml

lint: ## Run all linters
	ansible-lint
	yamllint .
	$(TF) fmt -check -recursive
	$(TF) validate

test-e2e: ## Full chain: infra, deploy, verify, idempotency, exposure check
	./scripts/e2e.sh

destroy: ## Destroy the AWS environment
	$(TF) destroy -input=false -auto-approve

clean: destroy ## Destroy the infrastructure and remove all local secret material
	rm -rf .local inventory/generated.yml
	@echo "Removed .local (SSH key, SCRAM passwords, cluster identity, CA) and the generated inventory."
