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


def test_downloader_downloads_attachments_to_expected_paths_with_distinct_display_names(
    tmp_path,
):
    from unittest import mock

    from unstructured_ingest.processes.connectors.jira import JiraDownloaderConfig

    connection_config = JiraConnectionConfig(
        access_config=JiraAccessConfig(token="pat"),
        url="https://jira.example.com",
    )
    parent_file_data = JiraIndexer(
        connection_config=connection_config,
        index_config=JiraIndexerConfig(issues=["FACT-1"]),
    )._create_file_data_from_issue(
        JiraIssueMetadata(
            id="10001",
            key="FACT-1",
            fields={"updated": "2026-05-22T10:00:00.000+0000"},
        )
    )
    parent_file_data.metadata.record_locator = {"id": "10001", "key": "FACT-1"}
    parent_file_data.display_name = "FACT-1: Example task"

    issue = {
        "id": "10001",
        "key": "FACT-1",
        "fields": {
            "created": "2026-05-21T10:00:00.000+0000",
            "updated": "2026-05-22T10:00:00.000+0000",
            "summary": "Example task",
            "description": "Issue body text",
            "attachment": [
                {
                    "id": "10011",
                    "filename": "first.pdf",
                    "mimeType": "application/pdf",
                    "self": "https://jira.example.com/rest/api/2/attachment/10011",
                },
                {
                    "id": "10012",
                    "filename": "second.txt",
                    "mimeType": "text/plain",
                    "self": "https://jira.example.com/rest/api/2/attachment/10012",
                },
            ],
            "issuetype": {"name": "Task"},
            "status": {"name": "Open"},
            "priority": "Medium",
            "assignee": {"accountId": "abc", "displayName": "Assignee"},
            "reporter": {"emailAddress": "user@example.com", "displayName": "Reporter"},
            "labels": [],
            "components": [],
            "comment": {"comments": []},
            "project": {"key": "FACT", "name": "Facts"},
        },
    }
    attachment_contents = {
        "10011": b"%PDF-first",
        "10012": b"second attachment body",
    }
    downloader = JiraDownloader(
        connection_config=connection_config,
        download_config=JiraDownloaderConfig(
            download_dir=tmp_path,
            download_attachments=True,
        ),
    )
    mock_client = mock.MagicMock()
    mock_client.get_attachment_content.side_effect = lambda attachment_id: attachment_contents[
        attachment_id
    ]

    with (
        mock.patch.object(downloader, "get_issue", return_value=issue),
        mock.patch.object(type(connection_config), "get_client", mock.MagicMock()),
    ):
        type(connection_config).get_client.return_value.__enter__.return_value = mock_client
        responses = downloader.run(parent_file_data)

    assert len(responses) == 3
    issue_response, first_attachment, second_attachment = responses
    assert issue_response["file_data"].display_name == "FACT-1: Example task"
    assert first_attachment["file_data"].display_name == "first.pdf"
    assert second_attachment["file_data"].display_name == "second.txt"
    assert first_attachment["path"] == tmp_path / "FACT/FACT-1/first.pdf.10011"
    assert second_attachment["path"] == tmp_path / "FACT/FACT-1/second.txt.10012"
    assert first_attachment["path"].read_bytes() == b"%PDF-first"
    assert second_attachment["path"].read_bytes() == b"second attachment body"
    assert issue_response["path"].read_text() != "second attachment body"


def test_downloader_rejects_attachment_path_traversal(tmp_path):
    from unittest import mock

    from unstructured_ingest.processes.connectors.jira import JiraDownloaderConfig

    connection_config = JiraConnectionConfig(
        access_config=JiraAccessConfig(token="pat"),
        url="https://jira.example.com",
    )
    parent_file_data = JiraIndexer(
        connection_config=connection_config,
        index_config=JiraIndexerConfig(issues=["FACT-1"]),
    )._create_file_data_from_issue(
        JiraIssueMetadata(
            id="10001",
            key="FACT-1",
            fields={"updated": "2026-05-22T10:00:00.000+0000"},
        )
    )
    parent_file_data.metadata.record_locator = {"id": "10001", "key": "FACT-1"}
    parent_file_data.display_name = "FACT-1: Example task"

    issue = {
        "id": "10001",
        "key": "FACT-1",
        "fields": {
            "created": "2026-05-21T10:00:00.000+0000",
            "updated": "2026-05-22T10:00:00.000+0000",
            "summary": "Example task",
            "description": "Issue body text",
            "attachment": [
                {
                    "id": "10011",
                    "filename": "../../../../outside.pdf",
                    "mimeType": "application/pdf",
                    "self": "https://jira.example.com/rest/api/2/attachment/10011",
                },
            ],
            "issuetype": {"name": "Task"},
            "status": {"name": "Open"},
            "priority": "Medium",
            "assignee": {"accountId": "abc", "displayName": "Assignee"},
            "reporter": {"emailAddress": "user@example.com", "displayName": "Reporter"},
            "labels": [],
            "components": [],
            "comment": {"comments": []},
            "project": {"key": "FACT", "name": "Facts"},
        },
    }
    downloader = JiraDownloader(
        connection_config=connection_config,
        download_config=JiraDownloaderConfig(
            download_dir=tmp_path,
            download_attachments=True,
        ),
    )
    mock_client = mock.MagicMock()
    mock_client.get_attachment_content.return_value = b"malicious content"

    with (
        mock.patch.object(downloader, "get_issue", return_value=issue),
        mock.patch.object(type(connection_config), "get_client", mock.MagicMock()),
        pytest.raises(ValueError, match="Security error: attachment download path"),
    ):
        type(connection_config).get_client.return_value.__enter__.return_value = mock_client
        downloader.run(parent_file_data)

    assert not (tmp_path.parent.parent / "outside.pdf.10011").exists()
