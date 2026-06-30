import sys
import pytest
pytestmark = pytest.mark.spark
import logging
from unittest.mock import MagicMock, patch
from collections import namedtuple
import importlib
from aqueduct.executor.spark.udf import _patch_pyspark_cloudpickle

VersionInfo = namedtuple("VersionInfo", ["major", "minor", "micro", "releaselevel", "serial"])

@pytest.fixture(autouse=True)
def clean_logs(caplog):
    caplog.clear()

def test_patch_skipped_on_old_python():
    with patch("aqueduct.executor.spark.udf.sys.version_info", VersionInfo(3, 12, 9, "final", 0)):
        with patch("aqueduct.executor.spark.udf.importlib.import_module") as mock_import:
            _patch_pyspark_cloudpickle()
            mock_import.assert_not_called()

def test_patch_warns_if_system_cp_missing(caplog):
    caplog.set_level(logging.WARNING)
    with patch("aqueduct.executor.spark.udf.sys.version_info", VersionInfo(3, 13, 0, "final", 0)):
        # Make 'import cloudpickle' fail
        with patch.dict("sys.modules", {"cloudpickle": None}):
            # We need to ensure the local import fails. 
            # In Python, if sys.modules['foo'] is None, 'import foo' raises ImportError.
            _patch_pyspark_cloudpickle()
    
    assert "system cloudpickle is not installed" in caplog.text

def test_patch_warns_if_bundled_cp_missing(caplog):
    caplog.set_level(logging.WARNING)
    mock_system_cp = MagicMock()
    mock_system_cp.__version__ = "3.0.0"
    
    with patch("aqueduct.executor.spark.udf.sys.version_info", VersionInfo(3, 13, 0, "final", 0)):
        with patch.dict("sys.modules", {"cloudpickle": mock_system_cp}):
            with patch("aqueduct.executor.spark.udf.importlib.import_module", side_effect=ImportError("No bundled cp")):
                _patch_pyspark_cloudpickle()
    
    assert "is not importable under any known path" in caplog.text

def test_patch_warns_if_attrs_missing(caplog):
    caplog.set_level(logging.WARNING)
    mock_system_cp = MagicMock()
    mock_system_cp.__version__ = "3.0.0"
    
    # Bundled CP missing 'CloudPickler'
    # Use spec to ensure hasattr(mock_bundled_cp, "CloudPickler") is False
    mock_bundled_cp = MagicMock(spec=["dumps", "loads", "__version__"])
    mock_bundled_cp.__version__ = "2.2.1"
    
    with patch("aqueduct.executor.spark.udf.sys.version_info", VersionInfo(3, 13, 0, "final", 0)):
        with patch.dict("sys.modules", {"cloudpickle": mock_system_cp}):
            with patch("aqueduct.executor.spark.udf.importlib.import_module", return_value=mock_bundled_cp):
                _patch_pyspark_cloudpickle()
    
    assert "lacks expected attributes" in caplog.text
    assert "'CloudPickler'" in caplog.text

def test_patch_warns_on_version_parse_failure(caplog):
    caplog.set_level(logging.WARNING)
    mock_system_cp = MagicMock()
    mock_system_cp.__version__ = "garbage"
    
    mock_bundled_cp = MagicMock()
    mock_bundled_cp.__version__ = "2.2.1"
    
    with patch("aqueduct.executor.spark.udf.sys.version_info", VersionInfo(3, 13, 0, "final", 0)):
        with patch.dict("sys.modules", {"cloudpickle": mock_system_cp}):
            with patch("aqueduct.executor.spark.udf.importlib.import_module", return_value=mock_bundled_cp):
                _patch_pyspark_cloudpickle()
    
    assert "could not compare cloudpickle versions" in caplog.text

def test_patch_skipped_if_bundled_is_newer():
    mock_system_cp = MagicMock()
    mock_system_cp.__version__ = "2.0.0"
    
    mock_bundled_cp = MagicMock()
    mock_bundled_cp.__version__ = "2.2.1"
    mock_bundled_cp.dumps = "original"
    
    with patch("aqueduct.executor.spark.udf.sys.version_info", VersionInfo(3, 13, 0, "final", 0)):
        with patch.dict("sys.modules", {"cloudpickle": mock_system_cp}):
            with patch("aqueduct.executor.spark.udf.importlib.import_module", return_value=mock_bundled_cp):
                _patch_pyspark_cloudpickle()
    
    assert mock_bundled_cp.dumps == "original"

def test_patch_success(caplog):
    caplog.set_level(logging.INFO)
    mock_system_cp = MagicMock()
    mock_system_cp.__version__ = "3.0.0"
    mock_system_cp.dumps = "new_dumps"
    mock_system_cp.loads = "new_loads"
    mock_system_cp.CloudPickler = "new_cp"
    
    mock_bundled_cp = MagicMock()
    mock_bundled_cp.__version__ = "2.2.1"
    mock_bundled_cp.dumps = "old_dumps"
    
    with patch("aqueduct.executor.spark.udf.sys.version_info", VersionInfo(3, 13, 0, "final", 0)):
        with patch.dict("sys.modules", {"cloudpickle": mock_system_cp}):
            with patch("aqueduct.executor.spark.udf.importlib.import_module", return_value=mock_bundled_cp):
                _patch_pyspark_cloudpickle()
    
    assert mock_bundled_cp.dumps == "new_dumps"
    assert mock_bundled_cp.loads == "new_loads"
    assert mock_bundled_cp.CloudPickler == "new_cp"
    assert "Patched pyspark.cloudpickle" in caplog.text
