from prism.prism import Prism, set_logging

from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions

__all__ = ["Prism","set_logging"]
