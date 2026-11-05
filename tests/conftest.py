import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    from lakehouse.spark import get_spark

    session = get_spark("tests", "local[1]")
    yield session
    session.stop()
