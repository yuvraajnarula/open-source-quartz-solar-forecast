import pytest


def pytest_addoption(parser):
    """Add custom command line option for folder name"""
    parser.addoption(
        "--foldername",
        action="store",
        default="30_minutely",
        help="Specify the folder name: 30_minutely or 5_minutely"
    )


@pytest.fixture
def folder_name(request):
    """Fixture to get the folder name from command line"""
    return request.config.getoption("--foldername") 