from prism.prism import Prism, load_schema, create_table, upload_file

from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions

__all__ = ["load_schema", "Prism", "create_table", "upload_file"]
