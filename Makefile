PACKAGE_NAME := unstructured_ingest
ARCH := $(shell uname -m)
SHELL_FILES := $(shell find . -name '*.sh' -type f | grep -v venv)

###########
# INSTALL #
###########

.PHONY: install
install:
	@uv sync --locked --all-groups --all-extras

.PHONY: lock
lock:
	@uv lock --upgrade

.PHONY: install-docker-compose
install-docker-compose:
	ARCH=${ARCH} ./scripts/install-docker-compose.sh

###########
#  TIDY   #
###########

.PHONY: tidy
tidy:
	uv run --locked --no-sync ruff format .
	uv run --locked --no-sync ruff check --fix-only --show-fixes .

.PHONY: tidy-shell
tidy-shell:
	shfmt -i 2 -l -w ${SHELL_FILES}

## version-sync:                update references to version with most recent version from CHANGELOG.md
.PHONY: version-sync
version-sync:
	scripts/version-sync.sh \
		-f ${PACKAGE_NAME}/__version__.py release

###########
#  CHECK  #
###########

.PHONY: check
check: check-ruff

.PHONY: check-ruff
check-ruff:
	uv run --locked --no-sync ruff check .

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

.PHONY: test-unit
test-unit:
	uv run --locked --no-sync pytest -n auto --tb=short --cov unstructured_ingest/ test/unit --ignore test/unit/unstructured

.PHONY: test-unit-unstructured
test-unit-unstructured:
	uv run --locked --no-sync pytest -n auto --tb=short --cov unstructured_ingest/ test/unit/unstructured

.PHONY: test-integration
test-integration:
	uv run --locked --no-sync pytest --tb=short test/integration

.PHONY: test-integration-partitioners
test-integration-partitioners:
	uv run --locked --no-sync pytest --tb=short test/integration/partitioners --json-report

.PHONY: test-integration-chunkers
test-integration-chunkers:
	uv run --locked --no-sync pytest --tb=short test/integration/chunkers --json-report

.PHONY: test-integration-embedders
test-integration-embedders:
	uv run --locked --no-sync pytest --tb=short test/integration/embedders --json-report

.PHONY: test-integration-connectors-blob-storage
test-integration-connectors-blob-storage:
	uv run --locked --no-sync pytest --tb=short --tags blob_storage test/integration/connectors --json-report

.PHONY: test-integration-connectors-sql
test-integration-connectors-sql:
	uv run --locked --no-sync pytest --tb=short --tags sql test/integration/connectors --json-report

.PHONY: test-integration-connectors-nosql
test-integration-connectors-nosql:
	uv run --locked --no-sync pytest --tb=short --tags nosql test/integration/connectors --json-report

.PHONY: test-integration-connectors-vector-db
test-integration-connectors-vector-db:
	uv run --locked --no-sync pytest --tb=short --tags vector_db test/integration/connectors --json-report

.PHONY: test-integration-connectors-graph-db
test-integration-connectors-graph-db:
	uv run --locked --no-sync pytest --tb=short --tags graph_db test/integration/connectors --json-report

.PHONY: test-integration-connectors-uncategorized
test-integration-connectors-uncategorized:
	uv run --locked --no-sync pytest --tb=short --tags uncategorized test/integration/connectors --json-report

.PHONY: parse-skipped-tests
parse-skipped-tests:
	uv run --locked --no-sync python ./scripts/parse_pytest_report.py

.PHONY: check-untagged-tests
check-untagged-tests:
	./scripts/check_untagged_tests.sh

###########
# COMPAT  #
###########
# Deprecated aliases – remove after one release cycle.

.PHONY: install-dependencies
install-dependencies: install

.PHONY: upgrade-dependencies
upgrade-dependencies: lock

.PHONY: unit-test
unit-test: test-unit

.PHONY: unit-test-unstructured
unit-test-unstructured: test-unit-unstructured

.PHONY: integration-test
integration-test: test-integration

.PHONY: integration-test-partitioners
integration-test-partitioners: test-integration-partitioners

.PHONY: integration-test-chunkers
integration-test-chunkers: test-integration-chunkers

.PHONY: integration-test-embedders
integration-test-embedders: test-integration-embedders

.PHONY: integration-test-connectors-blob-storage
integration-test-connectors-blob-storage: test-integration-connectors-blob-storage

.PHONY: integration-test-connectors-sql
integration-test-connectors-sql: test-integration-connectors-sql

.PHONY: integration-test-connectors-nosql
integration-test-connectors-nosql: test-integration-connectors-nosql

.PHONY: integration-test-connectors-vector-db
integration-test-connectors-vector-db: test-integration-connectors-vector-db

.PHONY: integration-test-connectors-graph-db
integration-test-connectors-graph-db: test-integration-connectors-graph-db

.PHONY: integration-test-connectors-uncategorized
integration-test-connectors-uncategorized: test-integration-connectors-uncategorized
