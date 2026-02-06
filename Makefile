PACKAGE_NAME := unstructured_ingest
ARCH := $(shell uname -m)
SHELL_FILES := $(shell find . -name '*.sh' -type f | grep -v venv)

###########
# INSTALL #
###########

.PHONY: install
install:
	@uv sync --frozen --all-groups --all-extras

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
	uv run --frozen --no-sync ruff format .
	uv run --frozen --no-sync ruff check --fix-only --show-fixes .

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
	uv run --frozen --no-sync ruff check .

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
	uv run --frozen --no-sync pytest -n auto -sv --cov unstructured_ingest/ test/unit --ignore test/unit/unstructured

.PHONY: test-unit-unstructured
test-unit-unstructured:
	uv run --frozen --no-sync pytest -n auto -sv --cov unstructured_ingest/ test/unit/unstructured

.PHONY: test-integration
test-integration:
	uv run --frozen --no-sync pytest test/integration

.PHONY: test-integration-partitioners
test-integration-partitioners:
	uv run --frozen --no-sync pytest test/integration/partitioners --json-report

.PHONY: test-integration-chunkers
test-integration-chunkers:
	uv run --frozen --no-sync pytest test/integration/chunkers --json-report

.PHONY: test-integration-embedders
test-integration-embedders:
	uv run --frozen --no-sync pytest test/integration/embedders --json-report

.PHONY: test-integration-connectors-blob-storage
test-integration-connectors-blob-storage:
	uv run --frozen --no-sync pytest --tags blob_storage test/integration/connectors --json-report

.PHONY: test-integration-connectors-sql
test-integration-connectors-sql:
	uv run --frozen --no-sync pytest --tags sql test/integration/connectors --json-report

.PHONY: test-integration-connectors-nosql
test-integration-connectors-nosql:
	uv run --frozen --no-sync pytest --tags nosql test/integration/connectors --json-report

.PHONY: test-integration-connectors-vector-db
test-integration-connectors-vector-db:
	uv run --frozen --no-sync pytest --tags vector_db test/integration/connectors --json-report

.PHONY: test-integration-connectors-graph-db
test-integration-connectors-graph-db:
	uv run --frozen --no-sync pytest --tags graph_db test/integration/connectors --json-report

.PHONY: test-integration-connectors-uncategorized
test-integration-connectors-uncategorized:
	uv run --frozen --no-sync pytest --tags uncategorized test/integration/connectors --json-report

.PHONY: parse-skipped-tests
parse-skipped-tests:
	uv run --frozen --no-sync python ./scripts/parse_pytest_report.py

.PHONY: check-untagged-tests
check-untagged-tests:
	./scripts/check_untagged_tests.sh
