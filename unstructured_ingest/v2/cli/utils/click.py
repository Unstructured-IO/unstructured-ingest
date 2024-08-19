import json
import os.path
from gettext import gettext, ngettext
from gettext import gettext as _
from pathlib import Path
from typing import Any, Optional, Type, TypeVar

import click
from pydantic import BaseModel, ConfigDict, Secret


def conform_click_options(options: dict):
    # Click sets all multiple fields as tuple, this needs to be updated to list
    for k, v in options.items():
        if isinstance(v, tuple):
            options[k] = list(v)


class Dict(click.ParamType):
    name = "dict"

    def convert(
        self,
        value: Any,
        param: Optional[click.Parameter] = None,
        ctx: Optional[click.Context] = None,
    ) -> Any:
        try:
            if isinstance(value, dict):
                return value
            if isinstance(value, Path) and value.is_file():
                with value.open() as f:
                    return json.load(f)
            if isinstance(value, str):
                return json.loads(value)
        except json.JSONDecodeError:
            self.fail(
                gettext(
                    "{value} is not a valid json value.",
                ).format(value=value),
                param,
                ctx,
            )


class FileOrJson(click.ParamType):
    name = "file-or-json"

    def __init__(self, allow_raw_str: bool = False):
        self.allow_raw_str = allow_raw_str

    def convert(
        self,
        value: Any,
        param: Optional[click.Parameter] = None,
        ctx: Optional[click.Context] = None,
    ) -> Any:
        # check if valid file
        full_path = os.path.abspath(os.path.expanduser(value))
        if os.path.isfile(full_path):
            return str(Path(full_path).resolve())
        if isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                if self.allow_raw_str:
                    return value
        self.fail(
            gettext(
                "{value} is not a valid json string nor an existing filepath.",
            ).format(value=value),
            param,
            ctx,
        )


class DelimitedString(click.ParamType):
    name = "delimited-string"

    def __init__(self, delimiter: str = ",", choices: Optional[list[str]] = None):
        self.choices = choices if choices else []
        self.delimiter = delimiter

    def convert(
        self,
        value: Any,
        param: Optional[click.Parameter] = None,
        ctx: Optional[click.Context] = None,
    ) -> Any:
        # In case a list is provided as the default, will not break
        if isinstance(value, list):
            split = [str(v).strip() for v in value]
        else:
            split = [v.strip() for v in value.split(self.delimiter)]
        if not self.choices:
            return split
        choices_str = ", ".join(map(repr, self.choices))
        for s in split:
            if s not in self.choices:
                self.fail(
                    ngettext(
                        "{value!r} is not {choice}.",
                        "{value!r} is not one of {choices}.",
                        len(self.choices),
                    ).format(value=s, choice=choices_str, choices=choices_str),
                    param,
                    ctx,
                )
        return split


BaseModelT = TypeVar("BaseModelT", bound=BaseModel)


def extract_config(flat_data: dict, config: Type[BaseModelT]) -> BaseModelT:
    fields = config.model_fields
    config.model_config = ConfigDict(extra="ignore")
    field_names = [v.alias or k for k, v in fields.items()]
    data = {k: v for k, v in flat_data.items() if k in field_names and v is not None}
    if access_config := fields.get("access_config"):
        access_config_type = access_config.annotation
        # Check if raw type is wrapped by a secret
        if (
            hasattr(access_config_type, "__origin__")
            and hasattr(access_config_type, "__args__")
            and access_config_type.__origin__ is Secret
        ):
            ac_subtypes = access_config_type.__args__
            ac_fields = ac_subtypes[0].model_fields
        elif issubclass(access_config_type, BaseModel):
            ac_fields = access_config_type.model_fields
        else:
            raise TypeError(f"Unrecognized access_config type: {access_config_type}")
        ac_field_names = [v.alias or k for k, v in ac_fields.items()]
        data["access_config"] = {
            k: v for k, v in flat_data.items() if k in ac_field_names and v is not None
        }
    return config.model_validate(obj=data)


class Group(click.Group):
    def parse_args(self, ctx, args):
        """
        This allows for subcommands to be called with the --help flag without breaking
        if parent command is missing any of its required parameters
        """
        try:
            return super().parse_args(ctx, args)
        except click.MissingParameter:
            if "--help" not in args:
                raise
            # remove the required params so that help can display
            for param in self.params:
                param.required = False
            return super().parse_args(ctx, args)

    def format_commands(self, ctx: click.Context, formatter: click.HelpFormatter) -> None:
        """
        Copy of the original click.Group format_commands() method but replacing
        'Commands' -> 'Destinations'
        """
        commands = []
        for subcommand in self.list_commands(ctx):
            cmd = self.get_command(ctx, subcommand)
            # What is this, the tool lied about a command.  Ignore it
            if cmd is None:
                continue
            if cmd.hidden:
                continue

            commands.append((subcommand, cmd))

        # allow for 3 times the default spacing
        if len(commands):
            if formatter.width:
                limit = formatter.width - 6 - max(len(cmd[0]) for cmd in commands)
            else:
                limit = -6 - max(len(cmd[0]) for cmd in commands)

            rows = []
            for subcommand, cmd in commands:
                help = cmd.get_short_help_str(limit)
                rows.append((subcommand, help))

            if rows:
                with formatter.section(_("Destinations")):
                    formatter.write_dl(rows)
