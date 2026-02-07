"""Utility functions for key normalization, placeholder resolution, and batching."""

from __future__ import annotations

import re
from typing import Any

from .exceptions import MissingPlaceholderError


def normalize_key(key: str) -> str:
    """Normalize S3 key by removing double slashes and leading/trailing slashes.

    Parameters
    ----------
    key : str
        Raw key string.

    Returns
    -------
    str
        Normalized key string.
    """
    # Remove double slashes
    key = re.sub(r'/+', '/', key)
    # Remove leading slash
    key = key.lstrip('/')
    # Remove trailing slash (unless it's the root)
    if key and key != '/':
        key = key.rstrip('/')
    return key


def resolve_template(template: str, vars: dict[str, Any], default_vars: dict[str, Any] | None = None) -> str:
    """Resolve placeholders in a template string.

    Parameters
    ----------
    template : str
        Template string with placeholders like {job_id}.
    vars : dict[str, Any]
        Variables to use for resolution.
    default_vars : dict[str, Any] | None, optional
        Optional default variables.

    Returns
    -------
    str
        Resolved string.

    Raises
    ------
    MissingPlaceholderError
        If a required placeholder is missing.
    """
    # Merge default vars with provided vars (provided vars take precedence)
    all_vars: dict[str, Any] = {}
    if default_vars:
        all_vars.update(default_vars)
    all_vars.update(vars)
    
    # Convert all values to strings
    str_vars = {k: str(v) for k, v in all_vars.items()}
    
    # Find all placeholders in the template
    placeholders = re.findall(r'\{(\w+)\}', template)
    
    # Check for missing placeholders
    missing = [p for p in placeholders if p not in str_vars]
    if missing:
        raise MissingPlaceholderError(missing[0], template)
    
    # Resolve template
    try:
        resolved = template.format_map(str_vars)
    except KeyError as e:
        raise MissingPlaceholderError(str(e), template) from e
    
    return normalize_key(resolved)


def infer_id_from_key(key: str, remove_extension: bool = True) -> str:
    """Infer an ID from an S3 key basename.

    Parameters
    ----------
    key : str
        S3 key path.
    remove_extension : bool, optional
        If True, remove file extension from basename. Defaults to True.

    Returns
    -------
    str
        Inferred ID string.
    """
    basename = key.split('/')[-1]
    if remove_extension:
        # Remove extension if present
        if '.' in basename:
            basename = '.'.join(basename.split('.')[:-1])
    return basename
