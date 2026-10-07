from contextlib import contextmanager
from unittest.mock import Mock

import pytest

from unstructured_ingest.processes.connectors.gitlab import (
    GitLabConnectionConfig,
    GitLabIndexer,
    GitLabIndexerConfig,
)


@pytest.mark.parametrize("path", [None, "/", ".", "docs", "docs/"])
@pytest.mark.parametrize("branch", [None, "feature/docs"])
def test_indexer_uses_repository_relative_paths(path, branch):
    config = GitLabIndexerConfig(git_branch=branch, **({"path": path} if path else {}))
    original_path = config.path
    project = Mock(default_branch="main")
    project.repository_tree.return_value = [
        {"path": "docs", "type": "tree", "id": "tree-id", "mode": "040000"},
        {"path": "docs/guide.txt", "type": "blob", "id": "blob-id", "mode": "100644"},
    ]

    @contextmanager
    def get_project(self):
        yield project

    connection = GitLabConnectionConfig(url="https://gitlab.com/team/project")
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(GitLabConnectionConfig, "get_project", get_project)
        records = list(GitLabIndexer(connection_config=connection, index_config=config).run())

    root = path in (None, "/", ".")
    project.repository_tree.assert_called_once_with(
        path="" if root else "docs", ref=branch or "main", recursive=True, iterator=True, all=True
    )
    assert len(records) == 1
    record = records[0]
    assert record.identifier == "blob-id"
    assert record.source_identifiers.fullpath == "docs/guide.txt"
    assert record.source_identifiers.rel_path == ("docs/guide.txt" if root else "guide.txt")
    assert record.source_identifiers.filename == "guide.txt"
    assert record.metadata.record_locator == {
        "file_path": "docs/guide.txt",
        "ref": branch or "main",
    }
    assert config.path == original_path
