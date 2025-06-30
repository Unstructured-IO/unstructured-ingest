from collections import abc
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from time import time
from typing import TYPE_CHECKING, Any, Callable, Generator, List, Optional, Union

from pydantic import BaseModel, Field, Secret

from unstructured_ingest.data_types.file_data import (
    FileData,
    FileDataSourceMetadata,
    SourceIdentifiers,
)
from unstructured_ingest.error import SourceConnectionError
from unstructured_ingest.interfaces import (
    AccessConfig,
    ConnectionConfig,
    Downloader,
    DownloaderConfig,
    DownloadResponse,
    Indexer,
    IndexerConfig,
    download_responses,
)
from unstructured_ingest.logger import logger
from unstructured_ingest.processes.connector_registry import (
    SourceRegistryEntry,
)
from unstructured_ingest.utils.dep_check import requires_dependencies

if TYPE_CHECKING:
    from atlassian import Jira

CONNECTOR_TYPE = "jira"

DEFAULT_C_SEP = " " * 5
DEFAULT_R_SEP = "\n"


class JiraIssueMetadata(BaseModel):
    id: str
    key: str
    fields: Optional[dict] = None  # Add fields to capture attachment data

    def get_project_id(self) -> str:
        return self.key.split("-")[0]

    def get_attachments(self) -> List[dict]:
        """Extract attachment information from fields"""
        if self.fields and "attachment" in self.fields:
            return self.fields["attachment"]
        return []


class FieldGetter(dict):
    def __getitem__(self, key):
        value = super().__getitem__(key) if key in self else None
        if value is None:
            value = FieldGetter({})
        return value


def nested_object_to_field_getter(obj: dict) -> Union[FieldGetter, dict]:
    if isinstance(obj, abc.Mapping):
        new_object = {}
        for k, v in obj.items():
            if isinstance(v, abc.Mapping):
                new_object[k] = FieldGetter(nested_object_to_field_getter(v))
            else:
                new_object[k] = v
        return FieldGetter(new_object)
    else:
        return obj


def api_token_based_generator(
    fn: Callable, key: str = "issues", **kwargs
) -> Generator[dict, None, None]:
    nextPageToken = kwargs.pop("nextPageToken", None)
    while True:
        resp = fn(nextPageToken=nextPageToken, **kwargs)
        issues = resp.get(key, [])
        for issue in issues:
            yield issue
        nextPageToken = resp.get("nextPageToken")
        if not nextPageToken:
            break


def api_page_based_generator(
    fn: Callable, key: str = "issues", **kwargs
) -> Generator[dict, None, None]:
    start = kwargs.pop("start", 0)
    while True:
        resp = fn(start=start, **kwargs)
        issues = resp.get(key, [])
        if not issues:
            break
        for issue in issues:
            yield issue
        start += len(issues)


class JiraAccessConfig(AccessConfig):
    password: Optional[str] = Field(
        description="Jira password or Cloud API token",
        default=None,
    )
    token: Optional[str] = Field(
        description="Jira Personal Access Token",
        default=None,
    )


class JiraConnectionConfig(ConnectionConfig):
    url: str = Field(description="URL of the Jira instance")
    username: Optional[str] = Field(
        description="Username or email for authentication",
        default=None,
    )
    cloud: bool = Field(description="Authenticate to Jira Cloud", default=False)
    access_config: Secret[JiraAccessConfig] = Field(description="Access configuration for Jira")

    def model_post_init(self, __context):
        access_configs = self.access_config.get_secret_value()
        basic_auth = self.username and access_configs.password
        pat_auth = access_configs.token
        if self.cloud and not basic_auth:
            raise ValueError(
                "cloud authentication requires username and API token (--password), "
                "see: https://atlassian-python-api.readthedocs.io/"
            )
        if basic_auth and pat_auth:
            raise ValueError(
                "both password and token provided, only one allowed, "
                "see: https://atlassian-python-api.readthedocs.io/"
            )
        if not (basic_auth or pat_auth):
            raise ValueError(
                "no form of auth provided, see: https://atlassian-python-api.readthedocs.io/"
            )

    @requires_dependencies(["atlassian"], extras="jira")
    @contextmanager
    def get_client(self) -> Generator["Jira", None, None]:
        from atlassian import Jira

        access_configs = self.access_config.get_secret_value()
        with Jira(
            url=self.url,
            username=self.username,
            password=access_configs.password,
            token=access_configs.token,
            cloud=self.cloud,
        ) as client:
            yield client


class JiraIndexerConfig(IndexerConfig):
    projects: Optional[list[str]] = Field(None, description="List of project keys")
    boards: Optional[list[str]] = Field(None, description="List of board IDs")
    issues: Optional[list[str]] = Field(None, description="List of issue keys or IDs")
    status_filters: Optional[list[str]] = Field(
        default=None,
        description="List of status filters, if provided will only return issues that have these statuses",  # noqa: E501
    )

    def model_post_init(self, context: Any, /) -> None:
        if not self.projects and not self.boards and not self.issues:
            raise ValueError("At least one of projects, boards, or issues must be provided.")


@dataclass
class JiraIndexer(Indexer):
    connection_config: JiraConnectionConfig
    index_config: JiraIndexerConfig
    connector_type: str = CONNECTOR_TYPE

    def precheck(self) -> None:
        try:
            with self.connection_config.get_client() as client:
                response = client.get_permissions("BROWSE_PROJECTS")
                permitted = response["permissions"]["BROWSE_PROJECTS"]["havePermission"]
        except Exception as e:
            logger.error(f"Failed to connect to Jira: {e}", exc_info=True)
            raise SourceConnectionError(f"Failed to connect to Jira: {e}")
        if not permitted:
            raise ValueError(
                """The provided user is not permitted to browse projects
                from the given Jira organization URL.
                Try checking username, password, token and the url arguments.""",
            )
        logger.info("Connection to Jira successful.")

    def run_jql(self, jql: str, **kwargs) -> Generator[JiraIssueMetadata, None, None]:
        with self.connection_config.get_client() as client:
            if client.cloud:
                for issue in api_token_based_generator(client.enhanced_jql, jql=jql, **kwargs):
                    yield JiraIssueMetadata.model_validate(issue)
            else:
                for issue in api_page_based_generator(client.jql, jql=jql, **kwargs):
                    yield JiraIssueMetadata.model_validate(issue)

    def _get_issues_within_projects(self) -> Generator[JiraIssueMetadata, None, None]:
        fields = ["key", "id", "status", "attachment"]  # Add attachment field
        jql = "project in ({})".format(", ".join(self.index_config.projects))
        jql = self._update_jql(jql)
        logger.debug(f"running jql: {jql}")
        return self.run_jql(jql=jql, fields=fields)

    def _get_issues_within_single_board(
        self, board_id: str
    ) -> Generator[JiraIssueMetadata, None, None]:
        with self.connection_config.get_client() as client:
            fields = ["key", "id", "attachment"]  # Add attachment field
            if self.index_config.status_filters:
                jql = "status in ({}) ORDER BY id".format(
                    ", ".join([f'"{s}"' for s in self.index_config.status_filters])
                )
            else:
                jql = "ORDER BY id"
            logger.debug(f"running jql for board {board_id}: {jql}")
            for issue in api_page_based_generator(
                fn=client.get_issues_for_board, board_id=board_id, fields=fields, jql=jql
            ):
                yield JiraIssueMetadata.model_validate(issue)

    def _get_issues_within_boards(self) -> Generator[JiraIssueMetadata, None, None]:
        if not self.index_config.boards:
            yield
        for board_id in self.index_config.boards:
            for issue in self._get_issues_within_single_board(board_id=board_id):
                yield issue

    def _update_jql(self, jql: str) -> str:
        if self.index_config.status_filters:
            jql += " and status in ({})".format(
                ", ".join([f'"{s}"' for s in self.index_config.status_filters])
            )
        jql = jql + " ORDER BY id"
        return jql

    def _get_issues_by_keys(self) -> Generator[JiraIssueMetadata, None, None]:
        fields = ["key", "id", "attachment"]  # Add attachment field
        jql = "key in ({})".format(", ".join(self.index_config.issues))
        jql = self._update_jql(jql)
        logger.debug(f"running jql: {jql}")
        return self.run_jql(jql=jql, fields=fields)

    def _create_file_data_from_issue(self, issue: JiraIssueMetadata) -> FileData:
        # Construct relative path and filename first
        filename = f"{issue.key}.txt"
        relative_path = str(Path(issue.get_project_id()) / filename)

        # Build metadata with attachments included in record_locator
        record_locator = {"id": issue.id, "key": issue.key, "full_path": relative_path}

        # Add attachments to record_locator if they exist
        attachments = issue.get_attachments()
        if attachments:
            record_locator["attachments"] = [
                {
                    "id": att["id"],
                    "filename": att["filename"],
                    "created": att.get("created"),
                    "mimeType": att.get("mimeType"),
                }
                for att in attachments
            ]

        metadata = FileDataSourceMetadata(
            date_processed=str(time()),
            record_locator=record_locator,
        )

        source_identifiers = SourceIdentifiers(
            filename=filename,
            fullpath=relative_path,
            rel_path=relative_path,
        )

        file_data = FileData(
            identifier=issue.id,
            connector_type=self.connector_type,
            metadata=metadata,
            additional_metadata=issue.model_dump(),
            source_identifiers=source_identifiers,
            display_name=source_identifiers.fullpath,
        )
        return file_data

    def get_generators(self) -> List[Callable]:
        generators = []
        if self.index_config.boards:
            generators.append(self._get_issues_within_boards)
        if self.index_config.issues:
            generators.append(self._get_issues_by_keys)
        if self.index_config.projects:
            generators.append(self._get_issues_within_projects)
        return generators

    def run(self, **kwargs: Any) -> Generator[FileData, None, None]:
        seen_keys = []
        for gen in self.get_generators():
            for issue in gen():
                if not issue:
                    continue
                if issue.key in seen_keys:
                    continue
                seen_keys.append(issue.key)
                yield self._create_file_data_from_issue(issue=issue)


class JiraDownloaderConfig(DownloaderConfig):
    download_attachments: bool = Field(
        default=False, description="If True, will download any attachments and process as well"
    )


@dataclass
class JiraDownloader(Downloader):
    connection_config: JiraConnectionConfig
    download_config: JiraDownloaderConfig = field(default_factory=JiraDownloaderConfig)
    connector_type: str = CONNECTOR_TYPE

    def _get_id_fields_for_issue(
        self, issue: dict, c_sep: str = DEFAULT_C_SEP, r_sep: str = DEFAULT_R_SEP
    ) -> str:
        issue_id, key = issue["id"], issue["key"]
        return f"IssueID_IssueKey:{issue_id}{c_sep}{key}{r_sep}"

    def _get_project_fields_for_issue(
        self, issue: dict, c_sep: str = DEFAULT_C_SEP, r_sep: str = DEFAULT_R_SEP
    ) -> str:
        if "project" in issue:
            return (
                f"ProjectID_Key:{issue['project']['key']}{c_sep}{issue['project']['name']}{r_sep}"
            )
        else:
            return ""

    def _get_dropdown_fields_for_issue(
        self, issue: dict, c_sep: str = DEFAULT_C_SEP, r_sep: str = DEFAULT_R_SEP
    ) -> str:
        return f"""
        IssueType:{issue["issuetype"]["name"]}
        {r_sep}
        Status:{issue["status"]["name"]}
        {r_sep}
        Priority:{issue["priority"]}
        {r_sep}
        AssigneeID_Name:{issue["assignee"]["accountId"]}{c_sep}{issue["assignee"]["displayName"]}
        {r_sep}
        ReporterAdr_Name:{issue["reporter"]["emailAddress"]}{c_sep}{issue["reporter"]["displayName"]}
        {r_sep}
        Labels:{c_sep.join(issue["labels"])}
        {r_sep}
        Components:{c_sep.join([component["name"] for component in issue["components"]])}
        {r_sep}
        """

    def _get_subtasks_for_issue(self, issue: dict) -> str:
        return ""

    def _get_text_fields_for_issue(
        self, issue: dict, c_sep: str = DEFAULT_C_SEP, r_sep: str = DEFAULT_R_SEP
    ) -> str:
        return f"""
        {issue["summary"]}
        {r_sep}
        {issue["description"]}
        {r_sep}
        {c_sep.join([attachment["self"] for attachment in issue["attachment"]])}
        {r_sep}
        """

    def _get_comments_for_issue(
        self, issue: dict, c_sep: str = DEFAULT_C_SEP, r_sep: str = DEFAULT_R_SEP
    ) -> str:
        return c_sep.join(
            [self._get_fields_for_comment(comment) for comment in issue["comment"]["comments"]],
        )

    def _get_fields_for_comment(
        self, comment, c_sep: str = DEFAULT_C_SEP, r_sep: str = DEFAULT_R_SEP
    ) -> str:
        return f"{comment['author']['displayName']}{c_sep}{comment['body']}{r_sep}"

    def form_templated_string(
        self,
        issue: dict,
        parsed_fields: Union[FieldGetter, dict],
        c_sep: str = "|||",
        r_sep: str = "\n\n\n",
    ) -> str:
        """Forms a template string via parsing the fields from the API response object on the issue
        The template string will be saved to the disk, and then will be processed by partition."""
        return r_sep.join(
            [
                self._get_id_fields_for_issue(issue),
                self._get_project_fields_for_issue(parsed_fields),
                self._get_dropdown_fields_for_issue(parsed_fields),
                self._get_subtasks_for_issue(parsed_fields),
                self._get_comments_for_issue(parsed_fields),
                self._get_text_fields_for_issue(parsed_fields),
            ],
        )

    def update_file_data(self, file_data: FileData, issue: dict) -> None:
        file_data.metadata.date_created = issue["fields"]["created"]
        file_data.metadata.date_modified = issue["fields"]["updated"]
        file_data.display_name = issue["fields"]["project"]["name"]

    def get_issue(self, issue_key: str) -> dict:
        try:
            with self.connection_config.get_client() as client:
                return client.issue(key=issue_key)
        except Exception as e:
            logger.error(f"Failed to fetch issue with key: {issue_key}: {e}", exc_info=True)
            raise SourceConnectionError(f"Failed to fetch issue with key: {issue_key}: {e}")

    def generate_attachment_file_data(
        self, attachment_dict: dict, parent_filedata: FileData
    ) -> FileData:
        new_filedata = parent_filedata.model_copy(deep=True)

        # Create attachment record_locator with parent context
        attachment_record_locator = {
            "id": attachment_dict["id"],
            "filename": attachment_dict["filename"],
            "created": attachment_dict.get("created"),
            "mimeType": attachment_dict.get("mimeType"),
            "parent": {
                "id": parent_filedata.metadata.record_locator["id"],
                "key": parent_filedata.metadata.record_locator["key"],
                "full_path": parent_filedata.source_identifiers.fullpath,
            },
        }

        # Append an identifier for attachment to not conflict with issue ids
        new_filedata.identifier = "{}a".format(attachment_dict["id"])
        filename = f"{attachment_dict['filename']}.{attachment_dict['id']}"
        new_filedata.metadata.filesize_bytes = attachment_dict.get("size")
        new_filedata.metadata.date_created = attachment_dict.get("created")
        new_filedata.metadata.url = attachment_dict.get("self")
        new_filedata.metadata.record_locator = attachment_record_locator
        full_path = (
            Path(parent_filedata.source_identifiers.fullpath).with_suffix("") / Path(filename)
        ).as_posix()
        new_filedata.metadata.record_locator["full_path"] = full_path
        new_filedata.source_identifiers = SourceIdentifiers(
            filename=filename,
            # add issue_parent to the fullpath and rel_path
            # to ensure that the attachment is saved in the same folder as the parent issue
            fullpath=full_path,
            rel_path=full_path,
        )
        return new_filedata

    def process_attachments(
        self, file_data: FileData, attachments: list[dict]
    ) -> list[DownloadResponse]:
        with self.connection_config.get_client() as client:
            download_path = self.get_download_path(file_data)
            attachment_download_dir = download_path.parent / "attachments"
            attachment_download_dir.mkdir(parents=True, exist_ok=True)
            download_responses = []
            for attachment in attachments:
                attachment_filename = Path(attachment["filename"])
                attachment_id = attachment["id"]
                attachment_download_path = attachment_download_dir / Path(
                    attachment_id
                ).with_suffix(attachment_filename.suffix)
                resp = client.get_attachment_content(attachment_id=attachment_id)
                with open(attachment_download_path, "wb") as f:
                    f.write(resp)
                attachment_filedata = self.generate_attachment_file_data(
                    attachment_dict=attachment, parent_filedata=file_data
                )
                download_responses.append(
                    self.generate_download_response(
                        file_data=attachment_filedata, download_path=attachment_download_path
                    )
                )
        return download_responses

    def run(self, file_data: FileData, **kwargs: Any) -> download_responses:
        issue_key = file_data.additional_metadata.get("key")
        if not issue_key:
            raise ValueError("Issue key not found in metadata.")
        issue = self.get_issue(issue_key)
        parsed_fields = nested_object_to_field_getter(issue["fields"])
        issue_str = self.form_templated_string(issue, parsed_fields)

        download_path = self.get_download_path(file_data)
        if download_path is None:
            raise ValueError("File data is missing source identifiers data.")
        download_path.parent.mkdir(parents=True, exist_ok=True)
        with open(download_path, "w") as f:
            f.write(issue_str)
        self.update_file_data(file_data, issue)
        download_response = self.generate_download_response(
            file_data=file_data, download_path=download_path
        )
        if self.download_config.download_attachments and (
            attachments := issue.get("fields", {}).get("attachment")
        ):
            attachment_responses = self.process_attachments(
                file_data=file_data, attachments=attachments
            )
            download_response = [download_response] + attachment_responses
        return download_response


jira_source_entry = SourceRegistryEntry(
    connection_config=JiraConnectionConfig,
    indexer_config=JiraIndexerConfig,
    indexer=JiraIndexer,
    downloader_config=JiraDownloaderConfig,
    downloader=JiraDownloader,
)
