"""
The exporter's settings, read from the arguments and the OTEL_EXPORTER_OTLP_* variables as the reference
`OTLPSpanExporter` of opentelemetry-exporter-otlp-proto-http 1.45 reads them, with the same precedence, parsing,
fallbacks and warnings (on the `fastotel` logger).
"""

import logging
import os
import re
from collections.abc import Mapping
from importlib.metadata import version
from urllib.parse import unquote

OTEL_EXPORTER_OTLP_ENDPOINT = "OTEL_EXPORTER_OTLP_ENDPOINT"
OTEL_EXPORTER_OTLP_TRACES_ENDPOINT = "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"
OTEL_EXPORTER_OTLP_HEADERS = "OTEL_EXPORTER_OTLP_HEADERS"
OTEL_EXPORTER_OTLP_TRACES_HEADERS = "OTEL_EXPORTER_OTLP_TRACES_HEADERS"
OTEL_EXPORTER_OTLP_TIMEOUT = "OTEL_EXPORTER_OTLP_TIMEOUT"
OTEL_EXPORTER_OTLP_TRACES_TIMEOUT = "OTEL_EXPORTER_OTLP_TRACES_TIMEOUT"
OTEL_EXPORTER_OTLP_COMPRESSION = "OTEL_EXPORTER_OTLP_COMPRESSION"
OTEL_EXPORTER_OTLP_TRACES_COMPRESSION = "OTEL_EXPORTER_OTLP_TRACES_COMPRESSION"
OTEL_EXPORTER_OTLP_CERTIFICATE = "OTEL_EXPORTER_OTLP_CERTIFICATE"
OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE = "OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE"
OTEL_EXPORTER_OTLP_CLIENT_KEY = "OTEL_EXPORTER_OTLP_CLIENT_KEY"
OTEL_EXPORTER_OTLP_TRACES_CLIENT_KEY = "OTEL_EXPORTER_OTLP_TRACES_CLIENT_KEY"
OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE = "OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE"
OTEL_EXPORTER_OTLP_TRACES_CLIENT_CERTIFICATE = "OTEL_EXPORTER_OTLP_TRACES_CLIENT_CERTIFICATE"

_DEFAULT_ENDPOINT = "http://localhost:4318"
_TRACES_PATH = "v1/traces"
_DEFAULT_TIMEOUT = 10
_COMPRESSIONS = ("none", "deflate", "gzip")
# Where the reference names itself, fastotel does; the rest are the reference's defaults
_DEFAULT_HEADERS = {"content-type": "application/x-protobuf", "user-agent": f"fastotel/{version('fastotel')}"}

logger = logging.getLogger("fastotel")


def endpoint(argument: str | None) -> str:
    if argument:
        return argument
    if traces := os.environ.get(OTEL_EXPORTER_OTLP_TRACES_ENDPOINT):
        return traces
    base = os.environ.get(OTEL_EXPORTER_OTLP_ENDPOINT) or _DEFAULT_ENDPOINT
    return f"{base.removesuffix('/')}/{_TRACES_PATH}"


def headers(argument: Mapping[str, str] | None) -> dict[str, str]:
    # The defaults, then the variable, then the argument, merged name by name
    resolved = dict(_DEFAULT_HEADERS)
    resolved.update(
        parse_env_headers(
            os.environ.get(OTEL_EXPORTER_OTLP_TRACES_HEADERS) or os.environ.get(OTEL_EXPORTER_OTLP_HEADERS, "")
        )
    )
    if argument:
        resolved.update({name.lower(): value for name, value in argument.items()})
    return resolved


def timeout(argument: float | None) -> float:
    if argument is not None:
        return argument
    raw = (
        os.environ.get(OTEL_EXPORTER_OTLP_TRACES_TIMEOUT)
        or os.environ.get(OTEL_EXPORTER_OTLP_TIMEOUT)
        or _DEFAULT_TIMEOUT
    )
    try:
        return float(raw)
    except ValueError:
        logger.warning("Invalid timeout value %r, using default of %s seconds", raw, _DEFAULT_TIMEOUT)
        return float(_DEFAULT_TIMEOUT)


def compression(argument: object) -> str:
    """
    "none", "deflate" or "gzip": the argument is one of the reference's `Compression` enums or a string.
    """
    if argument:
        value = getattr(argument, "value", argument)
        if not isinstance(value, str):
            raise TypeError(f"compression must be a str or a Compression, not {type(argument).__name__}")
        if (name := value.strip().lower()) not in _COMPRESSIONS:
            raise ValueError(f"Invalid compression type: {value!r}. Expected one of: 'none', 'deflate', 'gzip'.")
        return name
    raw = os.environ.get(OTEL_EXPORTER_OTLP_TRACES_COMPRESSION) or os.environ.get(OTEL_EXPORTER_OTLP_COMPRESSION)
    name = (raw or "none").lower().strip()
    if name not in _COMPRESSIONS:
        logger.warning("Unsupported compression type: %s", name)
        return "none"
    return name


def tls_file(argument: str | None, traces_variable: str, variable: str) -> str | None:
    return argument or os.environ.get(traces_variable) or os.environ.get(variable) or None


# The header format of the reference, from opentelemetry.util.re (Apache-2.0), which older versions of
# opentelemetry-api parse strictly: W3C baggage, values URL-encoded, and in "liberal" mode unencoded ones too
_OWS = r"[ \t]*"
_KEY_FORMAT = r"[\x21\x23-\x27\x2a\x2b\x2d\x2e\x30-\x39\x41-\x5a\x5e-\x7a\x7c\x7e]+"
_VALUE_FORMAT = r"[\x21\x23-\x2b\x2d-\x3a\x3c-\x5b\x5d-\x7e]*"
_LIBERAL_VALUE_FORMAT = r"[\x20\x21\x23-\x2b\x2d-\x3a\x3c-\x5b\x5d-\x7e]*"
_HEADER_PATTERN = re.compile(rf"{_OWS}{_KEY_FORMAT}{_OWS}={_OWS}{_VALUE_FORMAT}{_OWS}")
_LIBERAL_HEADER_PATTERN = re.compile(rf"{_OWS}{_KEY_FORMAT}{_OWS}={_OWS}{_LIBERAL_VALUE_FORMAT}{_OWS}")
_DELIMITER_PATTERN = re.compile(r"[ \t]*,[ \t]*")
_INVALID_HEADER = (
    "Header format invalid! Header values in environment variables must be "
    "URL encoded per the OpenTelemetry Protocol Exporter specification or "
    "a comma separated list of name=value occurrences: %s"
)


def parse_env_headers(raw: str) -> dict[str, str]:
    """
    `parse_env_headers(raw, liberal=True)` of opentelemetry.util.re.
    """
    parsed: dict[str, str] = {}
    for header in _DELIMITER_PATTERN.split(raw):
        if not header:
            continue
        if _HEADER_PATTERN.fullmatch(header.strip()):
            name, value = header.strip().split("=", 1)
            parsed[unquote(name).strip().lower()] = unquote(value).strip()
        elif _LIBERAL_HEADER_PATTERN.fullmatch(header.strip()):
            # Not URL-encoded, taken as it is, as other languages' SDKs take it
            name, value = header.strip().split("=", 1)
            parsed[name.strip().lower()] = value.strip()
        else:
            logger.warning(_INVALID_HEADER, header)
    return parsed
