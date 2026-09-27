"""Lakehouse table locations."""

from pathlib import Path


def table_path(root: Path | str, layer: str, name: str) -> str:
    return str(Path(root) / layer / name)
