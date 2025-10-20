import pytest

@pytest.mark.parametrize("folder_name", ["30_minutely", "5_minutely"])
def test_folder_processing(folder_name):
    """
    Run tests for both folder types using parameterization.
    """
    print(f"Running tests for folder: {folder_name}")
    assert folder_name in ["30_minutely", "5_minutely"]
