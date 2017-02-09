
# content of conftest.py
import pytest


def pytest_addoption(parser):
    parser.addoption("--data-dir", action="store", default="/home/test/data/", help="Data directory")


@pytest.fixture
def data_dir(request):
    return request.config.getoption("--data-dir")
