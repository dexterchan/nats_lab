#!/usr/bin/env python

"""Tests for `nats_lab` package."""

import pytest


@pytest.fixture
def get_connection_details()->dict[str,str]:
    return {
        "hostname":"localhost",
        "port": 4222
    }

@pytest.fixture
def get_test_subject() -> str:
    return "test_subject"

@pytest.fixture
def get_test_stream() -> str:
    return "test_stream2"