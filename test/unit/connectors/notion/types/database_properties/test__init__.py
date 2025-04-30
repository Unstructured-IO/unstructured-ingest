import pytest
import pytest_mock

from unstructured_ingest.processes.connectors.notion.interfaces import DBCellBase, DBPropertyBase
from unstructured_ingest.processes.connectors.notion.types.database_properties import (
    db_cell_type_mapping,
    db_prop_type_mapping,
    map_cells,
    map_properties,
    unsupported_db_prop_types,
)


def test_map_properties_success(mocker: pytest_mock.MockerFixture):
    mock_property = mocker.MagicMock(spec=DBPropertyBase)
    mock_property.from_dict = mocker.MagicMock(return_value=mock_property)
    mocker.patch.dict(db_prop_type_mapping, {"mock_type": mock_property})

    props = {
        "property1": {"type": "mock_type", "data": "value1"},
        "property2": {"type": "mock_type", "data": "value2"},
    }
    result = map_properties(props)

    assert len(result) == 2
    assert "property1" in result
    assert "property2" in result
    mock_property.from_dict.assert_any_call({"type": "mock_type", "data": "value1"})
    mock_property.from_dict.assert_any_call({"type": "mock_type", "data": "value2"})


def test_map_properties_unsupported_type(caplog: pytest.LogCaptureFixture):
    props = {
        "property1": {"type": unsupported_db_prop_types[0], "data": "value1"},
    }
    result = map_properties(props)

    assert result == {}
    assert "Unsupported property type 'button' for property 'property1'. Skipping." in caplog.text


def test_map_properties_key_error():
    props = {
        "property1": {"data": "value1"},  # Missing "type" key
    }

    with pytest.raises(KeyError, match="failed to map to associated database property"):
        map_properties(props)


def test_map_properties_invalid_type():
    props = {
        "property1": {"type": "non_existent_type", "data": "value1"},
    }

    with pytest.raises(KeyError, match="failed to map to associated database property"):
        map_properties(props)


def test_map_cells_success(mocker: pytest_mock.MockerFixture):
    mock_property = mocker.MagicMock(spec=DBCellBase)
    mock_property.from_dict = mocker.MagicMock(return_value=mock_property)
    mocker.patch.dict(db_cell_type_mapping, {"mock_type": mock_property})

    props = {
        "property1": {"type": "mock_type", "data": "value1"},
        "property2": {"type": "mock_type", "data": "value2"},
    }

    result = map_cells(props)

    assert len(result) == 2
    assert "property1" in result
    assert "property2" in result
    mock_property.from_dict.assert_any_call({"type": "mock_type", "data": "value1"})
    mock_property.from_dict.assert_any_call({"type": "mock_type", "data": "value2"})


def test_map_cells_unsupported_property():
    props = {
        "property1": {"type": "non_existent_type", "data": "value1"},
    }

    with pytest.raises(KeyError):
        map_cells(props)


def test_map_cells_key_error():
    props = {
        "property1": {"value": "MissingType"},
    }

    with pytest.raises(KeyError, match="failed to map to associated database property"):
        map_cells(props)


def test_map_cells_logs_warning_for_unsupported_type(caplog: pytest.LogCaptureFixture):
    props = {
        "property1": {"type": "button", "value": "Unsupported"},
    }

    result = map_cells(props)

    assert result == {}
    assert "Unsupported property type 'button' for property 'property1'. Skipping." in caplog.text
