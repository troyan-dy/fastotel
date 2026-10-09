import sys
import sysconfig
from importlib.metadata import version

import fastotel
import pytest
from fastotel import _fastotel


def test_version_is_the_installed_one() -> None:
    assert fastotel.__version__ == version("fastotel")


def test_native_module_is_loaded() -> None:
    assert _fastotel.__name__ == "fastotel._fastotel"


@pytest.mark.skipif(not sysconfig.get_config_var("Py_GIL_DISABLED"), reason="needs a free-threaded build")
def test_import_keeps_the_gil_disabled() -> None:
    # An extension module that does not declare itself thread-safe turns the GIL back on when it is imported
    assert not sys._is_gil_enabled()  # type: ignore[attr-defined,unused-ignore]  # 3.13+
