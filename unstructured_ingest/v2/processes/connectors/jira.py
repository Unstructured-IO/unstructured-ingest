import math
from collections import abc
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Generator, List, Optional, Union

from pydantic import Field, Secret

from unstructured_ingest.error import SourceConnectionError
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.interfaces import (
    AccessConfig,
    ConnectionConfig,
    Downloader,
    DownloaderConfig,
    DownloadResponse,
    FileData,
    FileDataSourceMetadata,
    Indexer,
    IndexerConfig,
    SourceIdentifiers,
)
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.processes.connector_registry import (
    SourceRegistryEntry,
)

if TYPE_CHECKING:
    from atlassian import Jira

CONNECTOR_TYPE = "jira"

DEFAULT_C_SEP = " " * 5
DEFAULT_R_SEP = "\n"


@dataclass
class JiraIssueMetadata:
    id: str
    key: str
    board_id: Optional[str] = None

    @property
    def project_id(self) -> str:
        return self.key.split("-")[0]

    def to_dict(self) -> Dict[str, Union[str, None]]:
        return {
            "id": self.id,
            "key": self.key,
            "board_id": self.board_id,
            "project_id": self.project_id,
        }


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


def issues_fetcher_wrapper(func, results_key="results", number_of_issues_to_fetch: int = 100):
    """
    A decorator function that wraps around a function to fetch issues from Jira API in a paginated
    manner. This is required because the Jira API has a limit of 100 issues per request.

    Args:
        func (callable): The function to be wrapped. This function should accept `limit` and `start`
        as keyword arguments.
        results_key (str, optional): The key in the response dictionary that contains the list of
        results. Defaults to "results".
        number_of_issues_to_fetch (int, optional): The total number of issues to fetch. Defaults to
        100.

    Returns:
        list: A list of all fetched issues.

    Raises:
        KeyError: If the response dictionary does not contain the specified `results_key`.
        TypeError: If the response type from the Jira API is neither list nor dict.
    """

    def wrapper(*args, **kwargs) -> list:
        kwargs["limit"] = min(100, number_of_issues_to_fetch)
        kwargs["start"] = kwargs.get("start", 0)

        all_results = []
        num_iterations = math.ceil(number_of_issues_to_fetch / kwargs["limit"])

        for _ in range(num_iterations):
            response = func(*args, **kwargs)
            if isinstance(response, list):
                all_results += response
            elif isinstance(response, dict):
                if results_key not in response:
                    raise KeyError(f'Response object is missing "{results_key}" key.')
                all_results += response[results_key]
            else:
                raise TypeError(
                    f"""Unexpected response type from Jira API.
                    Response type has to be either list or dict, got: {type(response).__name__}."""
                )
            kwargs["start"] += kwargs["limit"]

        return all_results

    return wrapper


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
    projects: Optional[List[str]] = Field(None, description="List of project keys")
    boards: Optional[List[str]] = Field(None, description="List of board IDs")
    issues: Optional[List[str]] = Field(None, description="List of issue keys or IDs")


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

    def _get_issues_within_single_project(self, project_key: str) -> List[JiraIssueMetadata]:
        with self.connection_config.get_client() as client:
            number_of_issues_to_fetch = client.get_project_issues_count(project=project_key)
            if isinstance(number_of_issues_to_fetch, dict):
                if "total" not in number_of_issues_to_fetch:
                    raise KeyError('Response object is missing "total" key.')
                number_of_issues_to_fetch = number_of_issues_to_fetch["total"]
            if not number_of_issues_to_fetch:
                logger.warning(f"No issues found in project: {project_key}. Skipping!")
                return []
            get_project_issues = issues_fetcher_wrapper(
                client.get_all_project_issues,
                results_key="issues",
                number_of_issues_to_fetch=number_of_issues_to_fetch,
            )
            issues = get_project_issues(project=project_key, fields=["key", "id"])
            logger.debug(f"Found {len(issues)} issues in project: {project_key}")
            return [JiraIssueMetadata(id=issue["id"], key=issue["key"]) for issue in issues]

    def _get_issues_within_projects(self) -> List[JiraIssueMetadata]:
        project_keys = self.index_config.projects
        if not project_keys:
            # for when a component list is provided, without any projects
            if self.index_config.boards or self.index_config.issues:
                return []
            # for when no components are provided. all projects will be ingested
            else:
                with self.connection_config.get_client() as client:
                    project_keys = [project["key"] for project in client.projects()]
        return [
            issue
            for project_key in project_keys
            for issue in self._get_issues_within_single_project(project_key)
        ]

    def _get_issues_within_single_board(self, board_id: str) -> List[JiraIssueMetadata]:
        with self.connection_config.get_client() as client:
            get_board_issues = issues_fetcher_wrapper(
                client.get_issues_for_board,
                results_key="issues",
            )
            issues = get_board_issues(board_id=board_id, fields=["key", "id"], jql=None)
            logger.debug(f"Found {len(issues)} issues in board: {board_id}")
            return [
                JiraIssueMetadata(id=issue["id"], key=issue["key"], board_id=board_id)
                for issue in issues
            ]

    def _get_issues_within_boards(self) -> List[JiraIssueMetadata]:
        if not self.index_config.boards:
            return []
        return [
            issue
            for board_id in self.index_config.boards
            for issue in self._get_issues_within_single_board(board_id)
        ]

    def _get_issues(self) -> List[JiraIssueMetadata]:
        with self.connection_config.get_client() as client:
            issues = [
                client.get_issue(issue_id_or_key=issue_key, fields=["key", "id"])
                for issue_key in self.index_config.issues or []
            ]
        return [JiraIssueMetadata(id=issue["id"], key=issue["key"]) for issue in issues]

    def get_issues(self) -> List[JiraIssueMetadata]:
        issues = [
            *self._get_issues_within_boards(),
            *self._get_issues_within_projects(),
            *self._get_issues(),
        ]
        # Select unique issues by issue 'id'.
        # Since boards issues are fetched first,
        # if there are duplicates, the board issues will be kept,
        # in order to keep issue 'board_id' information.
        seen = set()
        unique_issues: List[JiraIssueMetadata] = []
        for issue in issues:
            if issue.id not in seen:
                unique_issues.append(issue)
            seen.add(issue.id)
        return unique_issues

    def run(self, **kwargs: Any) -> Generator[FileData, None, None]:
        from time import time

        issues = self.get_issues()
        for issue in issues:
            # Build metadata
            metadata = FileDataSourceMetadata(
                date_processed=str(time()),
                record_locator=issue.to_dict(),
            )

            # Construct relative path and filename
            filename = f"{issue.id}.txt"
            relative_path = str(Path(issue.project_id) / filename)

            source_identifiers = SourceIdentifiers(
                filename=filename,
                fullpath=relative_path,
                rel_path=relative_path,
            )

            file_data = FileData(
                identifier=issue.id,
                connector_type=self.connector_type,
                metadata=metadata,
                additional_metadata=issue.to_dict(),
                source_identifiers=source_identifiers,
            )
            yield file_data


class JiraDownloaderConfig(DownloaderConfig):
    pass


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

    def run(self, file_data: FileData, **kwargs: Any) -> DownloadResponse:
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
        return self.generate_download_response(file_data=file_data, download_path=download_path)


jira_source_entry = SourceRegistryEntry(
    connection_config=JiraConnectionConfig,
    indexer_config=JiraIndexerConfig,
    indexer=JiraIndexer,
    downloader_config=JiraDownloaderConfig,
    downloader=JiraDownloader,
)
