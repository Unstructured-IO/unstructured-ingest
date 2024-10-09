#!/usr/bin/env bash

set -e

# python version must match lowest supported (3.9)
python_version=${UV_PYTHON_VERSION:-"3.9"}

pushd ./requirements || exit

find . -type f -name "*.txt" ! -name "constraints.txt" -exec rm '{}' ';'
find . -type f -name "*.in" -print0 | while read -r -d $'\0' in_file; do
  echo "compiling $in_file"
  txt_file="${in_file//\.in/\.txt}"
  uv pip compile --upgrade "$in_file" --output-file "$txt_file" --no-strip-extras --python-version "$python_version"
done
popd || exit
