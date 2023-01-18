#!/usr/bin/env python

"""Tests for `nats_lab` package."""

import pytest
from async_work_stream.model import Seq_Workload_Envelope
import uuid
import os

@pytest.fixture
def get_connection_details()->dict[str,str]:
    test_host_name = os.environ.get("NATS_HOSTNAME","localhost")
    test_port = int( os.environ.get("NATS_PORT", "4222") )
    return {
        "hostname":test_host_name,
        "port": test_port
    }

@pytest.fixture
def get_test_subject() -> str:
    return "test_subject"

@pytest.fixture
def get_test_stream() -> str:
    return "test_stream2"




@pytest.fixture
def get_test_stream_controller_normal_response() -> str:
    return "test_stream_controller_normal_response"

@pytest.fixture
def get_test_subject_controller_normal_response() -> str:
    return "test_subject_controller_normal_response"

@pytest.fixture
def get_test_stream_controller_no_response() -> str:
    return "test_stream_controller_no_response"

@pytest.fixture
def get_test_subject_controller_no_response()->str:
    return "test_subject_controller_no_response"

@pytest.fixture

def get_first_job() -> Seq_Workload_Envelope:
    def _get_first_job(test_total:int=10) -> Seq_Workload_Envelope:
        interval = 10
        return Seq_Workload_Envelope(
            job_id=uuid.uuid4().hex,
            id=1,
            total=test_total,
            payload={
                "start":1,
                "end":interval,
                "interval":interval
            }
        )
    return _get_first_job

@pytest.fixture
def get_model_path() -> str:
    return "./resources/optimize_batch_job_controller.json"