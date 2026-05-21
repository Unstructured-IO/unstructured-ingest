import json
from pathlib import Path

from bs4 import BeautifulSoup
from deepdiff import DeepDiff

from unstructured_ingest.utils import ndjson


def json_equality_check(expected_filepath: Path, current_filepath: Path) -> bool:
    with expected_filepath.open() as f:
        expected_data = json.load(f)
    with current_filepath.open() as f:
        current_data = json.load(f)
    diff = DeepDiff(expected_data, current_data)
    if diff:
        print("diff between expected and current json")
        print(diff.to_json(indent=2))
        return False
    return True


def ndjson_equality_check(expected_filepath: Path, current_filepath: Path) -> bool:
    with expected_filepath.open() as f:
        expected_data = ndjson.load(f)
    with current_filepath.open() as f:
        current_data = ndjson.load(f)
    if len(current_data) != len(expected_data):
        print(
            f"expected data length {len(expected_data)} "
            f"didn't match current results: {len(current_data)}"
        )
    for i in range(len(expected_data)):
        e = expected_data[i]
        r = current_data[i]
        if e != r:
            print(f"{i}th element doesn't match:\nexpected {e}\ncurrent {r}")
            return False
    return True


def html_equality_check(expected_filepath: Path, current_filepath: Path) -> bool:
    with expected_filepath.open() as expected_f:
        expected_soup = BeautifulSoup(expected_f, "html.parser")
    with current_filepath.open() as current_f:
        current_soup = BeautifulSoup(current_f, "html.parser")
    return expected_soup.text == current_soup.text


def unordered_table_html_equality_check(
    expected_filepath: Path, current_filepath: Path
) -> bool:
    # Equality check for HTML files whose first <table> holds rows in an
    # arbitrary order. Header (first <tr>) is compared positionally; remaining
    # data rows are compared as a multiset of their text content. Used for
    # connectors whose upstream API doesn't guarantee stable row ordering
    # (e.g. Notion's database query response).
    with expected_filepath.open() as expected_f:
        expected_soup = BeautifulSoup(expected_f, "html.parser")
    with current_filepath.open() as current_f:
        current_soup = BeautifulSoup(current_f, "html.parser")

    def split_rows(soup: BeautifulSoup) -> tuple[str, list[str]]:
        rows = soup.find_all("tr")
        if not rows:
            return "", []
        header = rows[0].get_text(" ", strip=True)
        data = sorted(r.get_text(" ", strip=True) for r in rows[1:])
        return header, data

    expected_header, expected_data = split_rows(expected_soup)
    current_header, current_data = split_rows(current_soup)

    if expected_header != current_header:
        print("table header differs:")
        print(f"  expected: {expected_header}")
        print(f"  current:  {current_header}")
        return False
    if expected_data != current_data:
        expected_counts = {r: expected_data.count(r) for r in set(expected_data)}
        current_counts = {r: current_data.count(r) for r in set(current_data)}
        only_in_expected = [r for r in expected_counts if expected_counts[r] != current_counts.get(r, 0)]
        only_in_current = [r for r in current_counts if current_counts[r] != expected_counts.get(r, 0)]
        print("table rows differ (order-insensitive):")
        for row in only_in_expected:
            print(f"  only in expected (x{expected_counts[row]}): {row}")
        for row in only_in_current:
            print(f"  only in current  (x{current_counts[row]}): {row}")
        return False
    return True


def txt_equality_check(expected_filepath: Path, current_filepath: Path) -> bool:
    with expected_filepath.open() as expected_f:
        expected_text_lines = expected_f.readlines()
    with current_filepath.open() as current_f:
        current_text_lines = current_f.readlines()
    if len(expected_text_lines) != len(current_text_lines):
        print(
            f"Lines in expected text file ({len(expected_text_lines)}) "
            f"don't match current text file ({len(current_text_lines)})"
        )
        return False
    expected_text = "\n".join(expected_text_lines)
    current_text = "\n".join(current_text_lines)
    if expected_text == current_text:
        return True
    print("txt content don't match:")
    print(f"expected: {expected_text}")
    print(f"current: {current_text}")
    return False


file_type_equality_check = {
    ".json": json_equality_check,
    ".ndjson": ndjson_equality_check,
    ".html": html_equality_check,
    ".txt": txt_equality_check,
}
