from prism.prism import (
    Prism,
    set_logging,
    schema_compact,
    upload_file,
    load_schema,
    truncate_table,
)

from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions

__all__ = [
    "Prism",
    "set_logging",
    "schema_compact",
    "upload_file",
    "load_schema",
    "truncate_table",
]
