"""Domain-specific exceptions for s3-exchange."""

from __future__ import annotations


class S3ExchangeError(Exception):
    """Base exception for s3-exchange errors."""
    pass


class MissingPlaceholderError(S3ExchangeError):
    """Raised when a required placeholder is missing in a template."""
    def __init__(self, placeholder: str, template: str) -> None:
        self.placeholder = placeholder
        self.template = template
        super().__init__(f"Missing placeholder '{placeholder}' in template: {template}")


class InvalidManifestError(S3ExchangeError):
    """Raised when manifest parsing fails or entry is invalid."""
    def __init__(self, message: str, line_number: int | None = None) -> None:
        self.line_number = line_number
        if line_number is not None:
            message = f"Line {line_number}: {message}"
        super().__init__(message)


class ShardReadError(S3ExchangeError):
    """Raised when reading a shard archive fails."""
    pass


class ObjectNotFoundError(S3ExchangeError):
    """Raised when an S3 object is not found."""
    def __init__(self, key: str) -> None:
        self.key = key
        super().__init__(f"S3 object not found: {key}")


class ManifestNotFoundError(S3ExchangeError):
    """Raised when a manifest file is not found."""
    def __init__(self, key: str) -> None:
        self.key = key
        super().__init__(f"Manifest not found: {key}")
