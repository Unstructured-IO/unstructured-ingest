#!/usr/bin/env bash

# python version must match lowest supported (3.9)
major=3
minor=9
if ! python -c "import sys; assert sys.version_info.major == $major and sys.version_info.minor == $minor"; then
  echo "python version not equal to expected $major.$minor: $(python --version)"
  exit 1
fi

pushd ./requirements || exit

find . -type f -name "*.txt" ! -name "constraints.txt" -exec rm '{}' ';'

pushd ./common || exit
find . -type f -name "*.in" -exec pip-compile --upgrade '{}' ';'
popd || exit

pushd ./connectors || exit
find . -type f -name "*.in" -exec pip-compile --upgrade '{}' ';'
popd || exit

find . -type f -name "*.in" -maxdepth 1 -exec pip-compile --upgrade '{}' ';'
popd || exit
