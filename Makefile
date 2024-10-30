PACKAGE_NAME := unstructured_ingest
PIP_VERSION := 23.2.1
ARCH := $(shell uname -m)

###########
# INSTALL #
###########

.PHONY: pip-compile
pip-compile:
	./scripts/pip-compile.sh

.PHONY: install-lint
install-lint:
	pip install -r requirements/lint.txt


.PHONY: install-client
install-client:
	pip install -r requirements/remote/client.txt

.PHONY: install-test
install-test:
	pip install -r requirements/test.txt

.PHONY: install-release
install-release:
	pip install -r requirements/release.txt

.PHONY: install-base
install-base:
	pip install -r requirements/common/base.txt

.PHONY: install-all-connectors
install-all-connectors:
	find requirements/connectors -type f -name "*.txt" -exec pip install -r '{}' ';'

.PHONY: install-all-embedders
install-all-embedders:
	find requirements/embed -type f -name "*.txt" -exec pip install -r '{}' ';'

.PHONY: install-all-deps
install-all-deps:
	find requirements -type f -name "*.txt" ! -name "constraints.txt" -exec pip install -r '{}' ';'

.PHONY: install-docker-compose
install-docker-compose:
	ARCH=${ARCH} ./scripts/install-docker-compose.sh

.PHONY: install-ci
install-ci: install-all-connectors install-all-embedders
	pip install -r requirements/local_partition/pdf.txt
	pip install -r requirements/local_partition/docx.txt
	pip install -r requirements/local_partition/pptx.txt
	pip install -r requirements/local_partition/xlsx.txt
	pip install -r requirements/local_partition/md.txt

###########
#  TIDY   #
###########

.PHONY: tidy
tidy: tidy-black tidy-ruff tidy-autoflake tidy-shell

.PHONY: tidy_shell
tidy-shell:
	shfmt -i 2 -l -w .

.PHONY: tidy-ruff
tidy-ruff:
	ruff check --fix-only --show-fixes .

.PHONY: tidy-black
tidy-black:
	black .

.PHONY: tidy-autoflake
tidy-autoflake:
	autoflake --in-place .

###########
#  CHECK  #
###########

.PHONY: check
check: check-python check-shell

.PHONY: check-python
check-python: check-black check-flake8 check-ruff check-autoflake check-version

.PHONY: check-black
check-black:
	black . --check

.PHONY: check-flake8
check-flake8:
	flake8 .

.PHONY: check-ruff
check-ruff:
	ruff check .

.PHONY: check-autoflake
check-autoflake:
	autoflake --check-diff .

.PHONY: check-shell
check-shell:
	shfmt -i 2 -d .

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
	PYTHONPATH=. pytest test/unit

.PHONY: integration-test
integration-test:
	PYTHONPATH=. pytest -sv test/integration

.PHONY: integration-test-partitioners
integration-test-partitioners:
	PYTHONPATH=. pytest -sv test/integration/partitioners

.PHONY: integration-test-chunkers
integration-test-chunkers:
	PYTHONPATH=. pytest -sv test/integration/chunkers

.PHONY: integration-test-embedders
integration-test-embedders:
	PYTHONPATH=. pytest -sv test/integration/embedders

.PHONY: integration-test-connectors-src
integration-test-connectors-src:
	PYTHONPATH=. pytest --tags source -sv test/integration/connectors


.PHONY: integration-test-connectors-dest
integration-test-connectors-dest:
	PYTHONPATH=. pytest --tags destination -sv test/integration/connectors
