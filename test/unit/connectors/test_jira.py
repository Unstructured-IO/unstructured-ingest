import pytest
from dateutil import parser

from unstructured_ingest.error import ValueError
from unstructured_ingest.processes.connectors.jira import (
    JiraAccessConfig,
    JiraConnectionConfig,
    JiraDownloader,
    JiraIndexer,
    JiraIndexerConfig,
    JiraIssueMetadata,
)


def test_connection_config_password_auth():
    JiraConnectionConfig(
        access_config=JiraAccessConfig(password="api_token"),
        url="https://example.atlassian.net",
        username="user@example.com",
        cloud=True,
    )


def test_connection_config_pat_auth():
    JiraConnectionConfig(
        access_config=JiraAccessConfig(token="pat"),
        url="https://jira.example.com",
    )


def test_connection_config_oauth_auth_requires_cloud_id():
    with pytest.raises(ValueError):
        JiraConnectionConfig(
            access_config=JiraAccessConfig(oauth_token="oauth_token"),
            url="https://example.atlassian.net",
            cloud=True,
        )


def test_connection_config_oauth_auth():
    config = JiraConnectionConfig(
        access_config=JiraAccessConfig(
            oauth_token="oauth_token",
            refresh_token="refresh_token",
        ),
        cloud_id="cloud-123",
        url="https://example.atlassian.net",
        cloud=True,
    )

    assert config.api_url() == "https://api.atlassian.com/ex/jira/cloud-123"
    assert config.issue_url("ENG-7") == "https://example.atlassian.net/browse/ENG-7"


def test_connection_config_oauth_auth_is_exclusive():
    with pytest.raises(ValueError):
        JiraConnectionConfig(
            access_config=JiraAccessConfig(
                password="api_token",
                oauth_token="oauth_token",
            ),
            cloud_id="cloud-123",
            username="user@example.com",
            url="https://example.atlassian.net",
            cloud=True,
        )


def test_indexer_oauth_file_data_uses_cloud_identity_and_updated_version():
    config = JiraConnectionConfig(
        access_config=JiraAccessConfig(oauth_token="oauth_token"),
        cloud_id="cloud-123",
        url="https://example.atlassian.net",
        cloud=True,
    )
    indexer = JiraIndexer(
        connection_config=config,
        index_config=JiraIndexerConfig(projects=["ENG"]),
    )
    issue = JiraIssueMetadata(
        id="10001",
        key="ENG-7",
        fields={
            "created": "2026-05-21T10:00:00.000+0000",
            "updated": "2026-05-22T10:00:00.000+0000",
            "attachment": [],
        },
    )

    file_data = indexer._create_file_data_from_issue(issue)

    assert file_data.identifier == "10001"
    assert file_data.source_identifiers.fullpath == "cloud-123/ENG/ENG-7.txt"
    assert file_data.metadata.url == "https://example.atlassian.net/browse/ENG-7"
    assert file_data.metadata.version == "2026-05-22T10:00:00.000+0000"
    assert file_data.metadata.date_created == str(
        parser.parse("2026-05-21T10:00:00.000+0000").timestamp()
    )
    assert file_data.metadata.date_modified == str(
        parser.parse("2026-05-22T10:00:00.000+0000").timestamp()
    )
    assert file_data.metadata.record_locator["cloud_id"] == "cloud-123"


def test_indexer_file_data_populates_creation_and_modification_dates():
    indexer = JiraIndexer(
        connection_config=JiraConnectionConfig(
            access_config=JiraAccessConfig(token="pat"),
            url="https://jira.example.com",
        ),
        index_config=JiraIndexerConfig(issues=["ENG-7"]),
    )
    issue = JiraIssueMetadata(
        id="10001",
        key="ENG-7",
        fields={
            "created": "2026-05-21T10:00:00.000+0000",
            "updated": "2026-05-22T10:00:00.000+0000",
        },
    )

    file_data = indexer._create_file_data_from_issue(issue)

    assert file_data.metadata.date_created == str(
        parser.parse("2026-05-21T10:00:00.000+0000").timestamp()
    )
    assert file_data.metadata.date_modified == str(
        parser.parse("2026-05-22T10:00:00.000+0000").timestamp()
    )
    assert file_data.metadata.version == "2026-05-22T10:00:00.000+0000"


def test_indexer_file_data_handles_missing_fields():
    indexer = JiraIndexer(
        connection_config=JiraConnectionConfig(
            access_config=JiraAccessConfig(token="pat"),
            url="https://jira.example.com",
        ),
        index_config=JiraIndexerConfig(issues=["ENG-7"]),
    )
    issue = JiraIssueMetadata(id="10001", key="ENG-7", fields=None)

    file_data = indexer._create_file_data_from_issue(issue)

    assert file_data.metadata.date_created is None
    assert file_data.metadata.date_modified is None
    assert file_data.metadata.version is None


def test_downloader_sets_issue_key_summary_display_and_version():
    file_data = JiraIndexer(
        connection_config=JiraConnectionConfig(
            access_config=JiraAccessConfig(token="pat"),
            url="https://jira.example.com",
        ),
        index_config=JiraIndexerConfig(issues=["ENG-7"]),
    )._create_file_data_from_issue(
        JiraIssueMetadata(
            id="10001",
            key="ENG-7",
            fields={"updated": "2026-05-22T10:00:00.000+0000"},
        )
    )
    issue = {
        "key": "ENG-7",
        "fields": {
            "created": "2026-05-21T10:00:00.000+0000",
            "updated": "2026-05-22T10:00:00.000+0000",
            "summary": "Add Atlassian connector",
        },
    }

    JiraDownloader(
        connection_config=JiraConnectionConfig(
            access_config=JiraAccessConfig(token="pat"),
            url="https://jira.example.com",
        )
    ).update_file_data(file_data, issue)

    assert file_data.metadata.version == "2026-05-22T10:00:00.000+0000"
    assert file_data.display_name == "ENG-7: Add Atlassian connector"
