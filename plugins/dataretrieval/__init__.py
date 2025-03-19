from importlib.metadata import PackageNotFoundError, version

from dataretrieval.nwis import *
from dataretrieval.utils import *

try:
    __version__ = version("dataretrieval")
except PackageNotFoundError:
    __version__ = "version-unknown"
