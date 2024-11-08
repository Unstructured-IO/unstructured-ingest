from unstructured_ingest.cli.cli import get_cmd

if __name__ == "__main__":
    cmd = get_cmd()
    src_command_dict = cmd.commands
    src_command_labels = list(src_command_dict.keys())

    # Get destination
    first_src_command_label = src_command_labels[0]
    first_dest_command = src_command_dict[first_src_command_label]
    dest_command_dict = first_dest_command.commands
    dest_command_labels = list(dest_command_dict.keys())

    print(src_command_labels)

    print(dest_command_labels)
