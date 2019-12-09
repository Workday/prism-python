import os
import pytest

@pytest.fixture
def rootdir():
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')

@pytest.fixture
def schema_file(rootdir):
    """Path to example JSON schema"""
    return os.path.join(rootdir, 'prism', 'data', 'schema.json')
