import multiprocessing as mp
import subprocess
from pathlib import Path

import click

from unstructured_ingest.cli.cli import get_cmd


def get_project_path() -> Path:
    project_path = Path(__file__).parents[1]
    return project_path.resolve()


def get_main_file() -> Path:
    project_path = get_project_path()
    main_file = project_path / "unstructured_ingest" / "main.py"
    assert main_file.exists()
    assert main_file.is_file()
    return main_file.resolve()


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


@click.command()
@click.option(
    "--local-code",
    is_flag=True,
    default=False,
    help="If set, will use the local main.py file to invoke rather than cli directly",
)
def run_check(local_code: bool = False):
    print("running --help check")
    cmd = get_cmd()
    src_command_dict = cmd.commands
    src_command_labels = list(src_command_dict.keys())

    # Get destination
    first_src_command_label = src_command_labels[0]
    first_dest_command = src_command_dict[first_src_command_label]
    dest_command_dict = first_dest_command.commands
    dest_command_labels = list(dest_command_dict.keys())

    cmds = []
    project_path = get_project_path()
    main_file = get_main_file()
    for src_command_label in src_command_labels:
        if local_code:
            cmds.append(
                f"PYTHONPATH={project_path} {main_file} {first_src_command_label} --help"  # noqa: E501
            )
        else:
            cmds.append(f"unstructured-ingest {first_src_command_label} --help")

    for dest_command_label in dest_command_labels:
        if local_code:
            cmds.append(
                f"PYTHONPATH={project_path} {main_file} {first_src_command_label} {dest_command_label} --help"  # noqa: E501
            )
        else:
            cmds.append(
                f"unstructured-ingest {first_src_command_label} {dest_command_label} --help"
            )

    print("testing the following commands:")
    for cmd in cmds:
        print(cmd)

    with mp.Pool(processes=mp.cpu_count()) as pool:
        pool.map(run_command, cmds)


if __name__ == "__main__":
    run_check()
