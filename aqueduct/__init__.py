"""Aqueduct — Intelligent Spark Pipeline Engine."""

__version__ = "1.0.0a0"

from aqueduct.parser.parser import parse, ParseError

__all__ = ["__version__", "parse", "ParseError"]
