PACKAGE_NAME := unstructured_ingest
ARCH := $(shell uname -m)
SHELL_FILES := $(shell find . -name '*.sh' -type f | grep -v venv)

###########
# INSTALL #
###########

.PHONY: install-dependencies
install-dependencies:
	@uv sync --all-groups --all-extras

.PHONY: upgrade-dependencies
upgrade-dependencies:
	@uv sync --all-groups --all-extras --upgrade

###########
#  TIDY   #
###########

.PHONY: tidy
tidy: tidy-ruff

.PHONY: tidy-ruff
tidy-ruff:
	uv run ruff format .
	uv run ruff check --fix-only --show-fixes .

.PHONY: tidy-shell
tidy-shell:
	shfmt -i 2 -l -w ${SHELL_FILES}

###########
#  CHECK  #
###########

.PHONY: check
check: check-ruff check-shell

.PHONY: check-ruff
check-ruff:
	uv run ruff check .

.PHONY: check-shell
check-shell:
	shfmt shfmt -i 2 -d ${SHELL_FILES}

.PHONY: check-version
check-version:
    # Fail if syncing version would produce changes
	scripts/version-sync.sh -c \
		-f "unstructured_ingest/__version__.py" semver

###########
#  TEST   #
###########
.PHONY: unit-test
unit-test:
	uv run --frozen pytest -sv --cov unstructured_ingest/ test/unit

.PHONY: unit-test-unstructured
unit-test-unstructured:
	uv run --frozen pytest -sv --cov unstructured_ingest/ test/unit/unstructured

.PHONY: integration-test
integration-test:
	uv run --frozen pytest -sv test/integration

.PHONY: integration-test-partitioners
integration-test-partitioners:
	uv run --frozen pytest -sv test/integration/partitioners --json-report

.PHONY: integration-test-chunkers
integration-test-chunkers:
	uv run --frozen pytest -sv test/integration/chunkers --json-report

.PHONY: integration-test-embedders
integration-test-embedders:
	uv run --frozen pytest -sv test/integration/embedders --json-report

.PHONY: integration-test-connectors-blob-storage
integration-test-connectors-blob-storage:
	uv run --frozen pytest --tags blob_storage -sv test/integration/connectors --json-report

.PHONY: integration-test-connectors-sql
integration-test-connectors-sql:
	uv run --frozen pytest --tags sql -sv test/integration/connectors --json-report

.PHONY: integration-test-connectors-nosql
integration-test-connectors-nosql:
	uv run --frozen pytest --tags nosql -sv test/integration/connectors --json-report

.PHONY: integration-test-connectors-vector-db
integration-test-connectors-vector-db:
	uv run --frozen pytest --tags vector_db -sv test/integration/connectors --json-report

.PHONY: integration-test-connectors-graph-db
integration-test-connectors-graph-db:
	uv run --frozen pytest --tags graph_db -sv test/integration/connectors --json-report

.PHONY: integration-test-connectors-uncategorized
integration-test-connectors-uncategorized:
	uv run --frozen pytest --tags uncategorized -sv test/integration/connectors --json-report

.PHONY: parse-skipped-tests
parse-skipped-tests:
	PYTHONPATH=. python ./scripts/parse_pytest_report.py

.PHONY: check-untagged-tests
check-untagged-tests:
	./scripts/check_untagged_tests.sh

