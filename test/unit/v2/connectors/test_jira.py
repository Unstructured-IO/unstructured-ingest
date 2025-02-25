from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError
from pytest_mock import MockerFixture

from unstructured_ingest.v2.processes.connectors.jira import (
    FieldGetter,
    JiraAccessConfig,
    JiraConnectionConfig,
    JiraIndexer,
    JiraIndexerConfig,
    JiraIssueMetadata,
    issues_fetcher_wrapper,
    nested_object_to_field_getter,
)


@pytest.fixture
def jira_connection_config():
    access_config = JiraAccessConfig(password="password")
    return JiraConnectionConfig(
        url="http://localhost:1234",
        username="test@example.com",
        access_config=access_config,
    )


@pytest.fixture
def jira_indexer(jira_connection_config: JiraConnectionConfig):
    indexer_config = JiraIndexerConfig(projects=["TEST1"], boards=["2"], issues=["TEST2-1"])
    return JiraIndexer(connection_config=jira_connection_config, index_config=indexer_config)


@pytest.fixture
def mock_jira(mocker: MockerFixture):
    mock_client = mocker.patch.object(JiraConnectionConfig, "get_client", autospec=True)
    mock_jira = mocker.MagicMock()
    mock_client.return_value.__enter__.return_value = mock_jira
    return mock_jira


def test_jira_indexer_precheck_success(
    caplog: pytest.LogCaptureFixture,
    mocker: MockerFixture,
    jira_indexer: JiraIndexer,
    mock_jira: MagicMock,
):
    get_permissions = mocker.MagicMock()
    get_permissions.return_value = {"permissions": {"BROWSE_PROJECTS": {"havePermission": True}}}
    mock_jira.get_permissions = get_permissions

    with caplog.at_level("INFO"):
        jira_indexer.precheck()
        assert "Connection to Jira successful." in caplog.text

    get_permissions.assert_called_once()


def test_jira_indexer_precheck_no_permission(
    mocker: MockerFixture,
    jira_indexer: JiraIndexer,
    mock_jira: MagicMock,
):
    get_permissions = mocker.MagicMock()
    get_permissions.return_value = {"permissions": {"BROWSE_PROJECTS": {"havePermission": False}}}
    mock_jira.get_permissions = get_permissions

    with pytest.raises(ValueError):
        jira_indexer.precheck()

    get_permissions.assert_called_once()


@pytest.mark.parametrize(
    ("project_issues_count", "expected_issues_count"), [(2, 2), ({"total": 2}, 2), (0, 0)]
)
def test_jira_indexer_get_issues_within_single_project(
    jira_indexer: JiraIndexer,
    mock_jira: MagicMock,
    project_issues_count,
    expected_issues_count,
):
    mock_jira.get_project_issues_count.return_value = project_issues_count
    mock_jira.get_all_project_issues.return_value = [
        {"id": "1", "key": "TEST-1"},
        {"id": "2", "key": "TEST-2"},
    ]

    issues = jira_indexer._get_issues_within_single_project("TEST1")
    assert len(issues) == expected_issues_count

    if issues:
        assert issues[0].id == "1"
        assert issues[0].key == "TEST-1"
        assert issues[1].id == "2"
        assert issues[1].key == "TEST-2"


def test_jira_indexer_get_issues_within_single_project_error(
    jira_indexer: JiraIndexer,
    mock_jira: MagicMock,
):
    mock_jira.get_project_issues_count.return_value = {}

    with pytest.raises(KeyError):
        jira_indexer._get_issues_within_single_project("TEST1")


def test_jira_indexer_get_issues_within_projects_with_projects(
    jira_indexer: JiraIndexer,
    mock_jira: MagicMock,
):
    mock_jira.get_project_issues_count.return_value = 2
    mock_jira.get_all_project_issues.return_value = [
        {"id": "1", "key": "TEST-1"},
        {"id": "2", "key": "TEST-2"},
    ]

    issues = jira_indexer._get_issues_within_projects()
    assert len(issues) == 2
    assert issues[0].id == "1"
    assert issues[0].key == "TEST-1"
    assert issues[1].id == "2"
    assert issues[1].key == "TEST-2"


def test_jira_indexer_get_issues_within_projects_no_projects_with_boards_or_issues(
    mocker: MockerFixture,
    jira_indexer: JiraIndexer,
):
    jira_indexer.index_config.projects = None
    jira_indexer.index_config.boards = ["2"]
    mocker.patch.object(JiraConnectionConfig, "get_client", autospec=True)

    issues = jira_indexer._get_issues_within_projects()
    assert issues == []


def test_jira_indexer_get_issues_within_projects_no_projects_no_boards_no_issues(
    jira_indexer: JiraIndexer,
    mock_jira: MagicMock,
):
    jira_indexer.index_config.projects = None
    jira_indexer.index_config.boards = None
    jira_indexer.index_config.issues = None
    mock_jira.projects.return_value = [{"key": "TEST1"}, {"key": "TEST2"}]
    mock_jira.get_project_issues_count.return_value = 2
    mock_jira.get_all_project_issues.return_value = [
        {"id": "1", "key": "TEST-1"},
        {"id": "2", "key": "TEST-2"},
    ]

    issues = jira_indexer._get_issues_within_projects()
    assert len(issues) == 4
    assert issues[0].id == "1"
    assert issues[0].key == "TEST-1"
    assert issues[1].id == "2"
    assert issues[1].key == "TEST-2"
    assert issues[2].id == "1"
    assert issues[2].key == "TEST-1"
    assert issues[3].id == "2"
    assert issues[3].key == "TEST-2"


def test_jira_indexer_get_issues_within_boards(
    jira_indexer: JiraIndexer,
    mock_jira: MagicMock,
):
    mock_jira.get_issues_for_board.return_value = [
        {"id": "1", "key": "TEST-1"},
        {"id": "2", "key": "TEST-2"},
    ]

    issues = jira_indexer._get_issues_within_boards()
    assert len(issues) == 2
    assert issues[0].id == "1"
    assert issues[0].key == "TEST-1"
    assert issues[1].id == "2"
    assert issues[1].key == "TEST-2"


def test_jira_indexer_get_issues_within_single_board(
    jira_indexer: JiraIndexer,
    mock_jira: MagicMock,
):
    mock_jira.get_issues_for_board.return_value = [
        {"id": "1", "key": "TEST-1"},
        {"id": "2", "key": "TEST-2"},
    ]

    issues = jira_indexer._get_issues_within_single_board("1")
    assert len(issues) == 2
    assert issues[0].id == "1"
    assert issues[0].key == "TEST-1"
    assert issues[0].board_id == "1"
    assert issues[1].id == "2"
    assert issues[1].key == "TEST-2"
    assert issues[1].board_id == "1"


def test_jira_indexer_get_issues_within_single_board_no_issues(
    jira_indexer: JiraIndexer,
    mock_jira: MagicMock,
):
    mock_jira.get_issues_for_board.return_value = []

    issues = jira_indexer._get_issues_within_single_board("1")
    assert len(issues) == 0


def test_jira_indexer_get_issues(
    jira_indexer: JiraIndexer,
    mock_jira: MagicMock,
):
    jira_indexer.index_config.issues = ["TEST2-1", "TEST2-2"]
    mock_jira.get_issue.return_value = {
        "id": "ISSUE_ID",
        "key": "ISSUE_KEY",
    }

    issues = jira_indexer._get_issues()
    assert len(issues) == 2
    assert issues[0].id == "ISSUE_ID"
    assert issues[0].key == "ISSUE_KEY"


def test_jira_indexer_get_issues_unique_issues(mocker: MockerFixture, jira_indexer: JiraIndexer):
    mocker.patch.object(
        JiraIndexer,
        "_get_issues_within_boards",
        return_value=[
            JiraIssueMetadata(id="1", key="TEST-1", board_id="1"),
            JiraIssueMetadata(id="2", key="TEST-2", board_id="1"),
        ],
    )
    mocker.patch.object(
        JiraIndexer,
        "_get_issues_within_projects",
        return_value=[
            JiraIssueMetadata(id="1", key="TEST-1"),
            JiraIssueMetadata(id="3", key="TEST-3"),
        ],
    )
    mocker.patch.object(
        JiraIndexer,
        "_get_issues",
        return_value=[
            JiraIssueMetadata(id="4", key="TEST-4"),
            JiraIssueMetadata(id="2", key="TEST-2"),
        ],
    )

    issues = jira_indexer.get_issues()
    assert len(issues) == 4
    assert issues[0].id == "1"
    assert issues[0].key == "TEST-1"
    assert issues[0].board_id == "1"
    assert issues[1].id == "2"
    assert issues[1].key == "TEST-2"
    assert issues[1].board_id == "1"
    assert issues[2].id == "3"
    assert issues[2].key == "TEST-3"
    assert issues[3].id == "4"
    assert issues[3].key == "TEST-4"


def test_jira_indexer_get_issues_no_duplicates(mocker: MockerFixture, jira_indexer: JiraIndexer):
    mocker.patch.object(
        JiraIndexer,
        "_get_issues_within_boards",
        return_value=[
            JiraIssueMetadata(id="1", key="TEST-1", board_id="1"),
        ],
    )
    mocker.patch.object(
        JiraIndexer,
        "_get_issues_within_projects",
        return_value=[
            JiraIssueMetadata(id="2", key="TEST-2"),
        ],
    )
    mocker.patch.object(
        JiraIndexer,
        "_get_issues",
        return_value=[
            JiraIssueMetadata(id="3", key="TEST-3"),
        ],
    )

    issues = jira_indexer.get_issues()
    assert len(issues) == 3
    assert issues[0].id == "1"
    assert issues[0].key == "TEST-1"
    assert issues[0].board_id == "1"
    assert issues[1].id == "2"
    assert issues[1].key == "TEST-2"
    assert issues[2].id == "3"
    assert issues[2].key == "TEST-3"


def test_jira_indexer_get_issues_empty(mocker: MockerFixture, jira_indexer: JiraIndexer):
    mocker.patch.object(JiraIndexer, "_get_issues_within_boards", return_value=[])
    mocker.patch.object(JiraIndexer, "_get_issues_within_projects", return_value=[])
    mocker.patch.object(JiraIndexer, "_get_issues", return_value=[])

    issues = jira_indexer.get_issues()
    assert len(issues) == 0


def test_connection_config_multiple_auth():
    with pytest.raises(ValidationError):
        JiraConnectionConfig(
            access_config=JiraAccessConfig(
                password="api_token",
                token="access_token",
            ),
            username="user_email",
            url="url",
        )


def test_connection_config_no_auth():
    with pytest.raises(ValidationError):
        JiraConnectionConfig(access_config=JiraAccessConfig(), url="url")


def test_connection_config_basic_auth():
    JiraConnectionConfig(
        access_config=JiraAccessConfig(password="api_token"),
        url="url",
        username="user_email",
    )


def test_connection_config_pat_auth():
    JiraConnectionConfig(
        access_config=JiraAccessConfig(token="access_token"),
        url="url",
    )


def test_jira_issue_metadata_object():
    expected = {"id": "10000", "key": "TEST-1", "board_id": "1", "project_id": "TEST"}
    metadata = JiraIssueMetadata(id="10000", key="TEST-1", board_id="1")
    assert expected == metadata.to_dict()


def test_nested_object_to_field_getter():
    obj = {"a": 1, "b": {"c": 2}}
    fg = nested_object_to_field_getter(obj)
    assert isinstance(fg, FieldGetter)
    assert fg["a"] == 1
    assert isinstance(fg["b"], FieldGetter)
    assert fg["b"]["c"] == 2
    assert isinstance(fg["b"]["d"], FieldGetter)
    assert fg["b"]["d"]["e"] == {}


def test_issues_fetcher_wrapper():
    test_issues_to_fetch = 250
    test_issues = [{"id": i} for i in range(0, test_issues_to_fetch)]

    def mock_func(limit, start):
        return {"results": test_issues[start : start + limit]}

    wrapped_func = issues_fetcher_wrapper(mock_func, number_of_issues_to_fetch=test_issues_to_fetch)
    results = wrapped_func()
    assert len(results) == 250
    assert results[0]["id"] == 0
    assert results[-1]["id"] == 249

    test_issues_to_fetch = 150
    test_issues = [{"id": i} for i in range(0, test_issues_to_fetch)]

    def mock_func_list(limit, start):
        return test_issues[start : start + limit]

    wrapped_func_list = issues_fetcher_wrapper(
        mock_func_list, number_of_issues_to_fetch=test_issues_to_fetch
    )
    results_list = wrapped_func_list()
    assert len(results_list) == 150
    assert results_list[0]["id"] == 0
    assert results_list[-1]["id"] == 149

    def mock_func_invalid(limit, start):
        return "invalid"

    wrapped_func_invalid = issues_fetcher_wrapper(mock_func_invalid, number_of_issues_to_fetch=50)
    with pytest.raises(TypeError):
        wrapped_func_invalid()

    def mock_func_key_error(limit, start):
        return {"wrong_key": []}

    wrapped_func_key_error = issues_fetcher_wrapper(
        mock_func_key_error, number_of_issues_to_fetch=50
    )
    with pytest.raises(KeyError):
        wrapped_func_key_error()
