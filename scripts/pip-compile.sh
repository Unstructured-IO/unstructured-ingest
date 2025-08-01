#!/usr/bin/env bash

set -e

# python version must match lowest supported (3.10)
python_version=${UV_PYTHON_VERSION:-"3.10"}

# if major and minor python version (x.y) is not equal to current python_version, error out
if [[ $(python --version | cut -d ' ' -f 2 | cut -d '.' -f 1-2) != $(echo "$python_version" | cut -d '.' -f 1-2) ]]; then
  echo "Python version must be $python_version (lowest supported) to be able to pip-compile."
  exit 1
fi

pushd ./requirements || exit

find . -type f -name "*.txt" ! -name "constraints.txt" -exec rm '{}' ';'
find . -type f -name "*.in" -print0 | while read -r -d $'\0' in_file; do
  echo "compiling $in_file"
  # remove .in extension and add .txt extension
  txt_file="${in_file%.in}.txt"
  uv pip compile --upgrade "$in_file" --output-file "$txt_file" --no-strip-extras --python-version "$python_version"
done
popd || exit
