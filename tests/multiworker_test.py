"""Compatibility entry point for the real, isolated OS-process test."""

import sys
from pathlib import Path

import pytest

if __name__ == "__main__":
    sys.exit(pytest.main([str(Path(__file__).with_name("test_integration.py")) + "::test_delivery_presence_restart_and_revocation_across_processes", "-q"]))
