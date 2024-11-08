import multiprocessing as mp
import subprocess
from pathlib import Path

from unstructured_ingest.cli.cli import get_cmd


def get_main_file() -> Path:
    script_path = Path(__file__).parents[1]
    main_file = script_path / "unstructured_ingest" / "main.py"
    assert main_file.exists()
    assert main_file.is_file()
    return main_file


def run_command(cmd):
    resp = subprocess.run(
        cmd,
        shell=True,
        capture_output=True,
    )
    if resp.returncode != 0:
        print(f"failed to run: {cmd}")
        print("STDOUT: {}".format(resp.stdout.decode("utf-8")))
        print("STDERR: {}".format(resp.stderr.decode("utf-8")))
        raise subprocess.CalledProcessError(resp.returncode, resp.args, resp.stdout, resp.stderr)


if __name__ == "__main__":
    cmd = get_cmd()
    src_command_dict = cmd.commands
    src_command_labels = list(src_command_dict.keys())

    # Get destination
    first_src_command_label = src_command_labels[0]
    first_dest_command = src_command_dict[first_src_command_label]
    dest_command_dict = first_dest_command.commands
    dest_command_labels = list(dest_command_dict.keys())

    cmds = []
    main_file = get_main_file()
    for src_command_label in src_command_labels:
        cmds.append(f"{main_file} {src_command_label} --help")

    for dest_command_label in dest_command_labels:
        cmds.append(f"{main_file} {first_src_command_label} {dest_command_label} --help")

    with mp.Pool(processes=mp.cpu_count()) as pool:
        pool.map(run_command, cmds)
