import json
import os
from pathlib import Path

root_dir = Path(__file__).parents[1]
report_path = root_dir / ".report.json"

if __name__ == "__main__":
    print(f"parsing report contents from: {report_path}")
    with report_path.open() as f:
        report_data = json.load(f)
    skipped = report_data.get("summary", {}).get("skipped", 0)
    if skipped == 0:
        print("No skipped tests")
        exit()
    tests = report_data.get("tests", [])
    skipped_tests = [test for test in tests if test["outcome"] == "skipped"]
    if not skipped_tests:
        print("No skipped tests")
    print(f"Updating github step summary with {len(skipped_tests)} skipped tests")
    with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as fh:
        print("### Pytest Skipped Test Warning", file=fh)
        for test in skipped_tests:
            node_id = test["nodeid"]
            longrepr: str = test["setup"]["longrepr"]
            longrepr = longrepr.rstrip(")").lstrip("(")
            print(f"* **Node ID:** `{node_id}`", file=fh)
            print(f"  * **Reason:** `{longrepr}`", file=fh)
