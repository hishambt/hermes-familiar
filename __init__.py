"""Registers the delivery-only ``familiar`` platform with Hermes.

Everything that matters is in ``adapter.py``; this file is the entry point and nothing else.
"""

from .adapter import register

__all__ = ["register"]
