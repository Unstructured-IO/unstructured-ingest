PACKAGE_NAME := unstructured_ingest
PIP_VERSION := 23.2.1

###########
# INSTALL #
###########

.PHONY: pip-compile
pip-compile:
	./scripts/pip-compile.sh

.PHONY: install
install: install-lint install-test install-all-deps

.PHONY: install-lint
install-lint:
	pip install -r requirements/lint.txt

.PHONY: install-test
install-test:
	pip install -r requirements/test.txt

.PHONY: install-all-deps
install-all-deps:
	find requirements -type f -name "*.txt" ! -name "constraints.txt" -exec pip install -r '{}' ';'

###########
#  TIDY   #
###########

.PHONY: tidy
tidy: tidy-black tidy-ruff tidy-autoflake tidy-shell

.PHONY: tidy_shell
tidy-shell:
	shfmt -l -w .

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
check-python: check-black check-flake8 check-ruff check-autoflake

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
	shfmt -d .

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
	PYTHONPATH=. pytest test
