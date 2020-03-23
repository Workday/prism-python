from prism.prism import Prism, load_schema

from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions

__all__ = ["Prism", "load_schema"]
