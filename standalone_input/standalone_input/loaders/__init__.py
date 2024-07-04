from .direct_loader import DirectLoader
from .misp_loader import MISPLoader
from .source_loader import SourceLoader

TLoader = DirectLoader | MISPLoader | SourceLoader

__all__ = [
    "DirectLoader",
    "MISPLoader",
    "SourceLoader",
    "TLoader"
]
