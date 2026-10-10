"""
A Rust-backed drop-in for parts of the OpenTelemetry Python SDK.
"""

from importlib.metadata import version

from fastotel import _fastotel as _fastotel
from fastotel._processor import OTLPSpanProcessor

__version__ = version("fastotel")
__all__ = ["OTLPSpanProcessor", "__version__"]
