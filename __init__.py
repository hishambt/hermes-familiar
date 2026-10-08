"""Registers the delivery-only ``familiar`` platform with Hermes.

The connection is ``adapter.py`` and the reads a client asks for are ``answers.py``; this file is the entry point
and nothing else.
"""

from .adapter import register

__all__ = ["register"]
