from async_work_stream.controller import Seq_Controller
from async_work_stream.worker import Worker
from async_work_stream.model import Seq_Workload_Envelope
import pytest
import uuid
from collections import defaultdict

from utility.logging import get_test_logger

logger = get_test_logger(__name__)
test_total=2
exection_limit_seconds = 10





@pytest.fixture
def get_test_subject_Seq_Controller_check_idempotent() -> str:
    return "test_Seq_Controller_subject_check_idempotent"

@pytest.fixture
def get_test_stream_Seq_Controller_check_idempotent()->str:
    return "test_Seq_Controller_stream_check_idempotent"

@pytest.fixture
def get_job_id_defined() -> str:
    return "NO_RESPONSE"

@pytest.fixture
def get_my_first_job(get_first_job, get_job_id_defined) -> Seq_Workload_Envelope:
    first_job:Seq_Workload_Envelope = get_first_job(test_total)
    first_job.job_id = get_job_id_defined
    return first_job



test_message_retention_period=10  

@pytest.mark.asyncio
async def test_controller_without_response(
    get_connection_details,
    get_test_subject_Seq_Controller_check_idempotent,
     get_test_stream_Seq_Controller_check_idempotent,
     get_my_first_job) -> None:
    
    conn_details:dict = get_connection_details
    logger.info(f"Start controller {conn_details}")


    _controller = Seq_Controller(
        hostname=conn_details.get("hostname"), 
        port=conn_details.get("port"),
        subject=get_test_subject_Seq_Controller_check_idempotent,
        persistance_stream_name=get_test_stream_Seq_Controller_check_idempotent,
        execution_limit_seconds= exection_limit_seconds,
        msg_retention_minutes=test_message_retention_period)

    process_counter_dict:dict = defaultdict(int)
    def _iterate_message(msg:Seq_Workload_Envelope) -> tuple[Seq_Workload_Envelope, bool]:
        """_summary_

        Args:
            msg (Seq_Workload_Envelope): _description_

        Returns:
            tuple[Seq_Workload_Envelope, bool]: Next Message, Continue_Next?
        """
        #process_counter += 1
        logger.debug(f"controller processing: {msg}")
        process_counter_dict["n"] += 1
        return msg, True

    await _controller.submit_seq_job(
        first_job=get_my_first_job,
        iterate_job_func=_iterate_message
    )
    assert process_counter_dict["n"] == 1
    pass

from jetstreams.client import Async_EventBus_Nats
import nats
from typing import Any
from async_work_stream.model import WorkStatus

@pytest.mark.asyncio
async def test_worker_with_response (get_connection_details, 
                            get_test_subject_Seq_Controller_check_idempotent, 
                            get_test_stream_Seq_Controller_check_idempotent) -> None:
    job_submit_subject = f"{get_test_subject_Seq_Controller_check_idempotent}_seq_job_submit"
    job_feedback_subject = f"{get_test_subject_Seq_Controller_check_idempotent}_seq_job_feedback"
    test_stream = get_test_stream_Seq_Controller_check_idempotent

    
    conn_details:dict[str, Any] = get_connection_details
    p = Async_EventBus_Nats(
        server=conn_details.get("hostname"),
        port = conn_details.get("port"),
        msg_retention_minutes=test_message_retention_period
    )
    await p.connect()

    
    await p.register_subject_to_stream(stream_name=test_stream, subject=[job_submit_subject, job_feedback_subject])

    pubsub = await p.pull_subscribe(subject=job_submit_subject, durable_name="idempotent_worker1")

    continue_read:bool = True
    message_received:int = 0
    try:
        while continue_read:
            async with  p.pull_subscribe_fetch_message_helper(
                    pull_subscription=pubsub,
                    number_msgs=3,
                    timeout_seconds=5
                ) as messages:
                for m in messages:
                    logger.info(m)
                    received_msg = Seq_Workload_Envelope(**m)
                    logger.info(f"Acknowledge Parsed message {received_msg}")
                    message_received += 1
                    received_msg.last_status = WorkStatus.SUCCESS
                    logger.info(f"Worker received {received_msg}")
                    await p.publish(subject=job_feedback_subject, payloads=[received_msg.__dict__])
                    await p.publish(subject=job_feedback_subject, payloads=[received_msg.__dict__])
                    logger.info(f"Published to {job_feedback_subject}")
                continue_read = (len(messages) > 0)

    except nats.errors.TimeoutError:
        logger.info("Time out reading")
    finally:
        await p.close()
    logger.info(f"Worker processed {message_received} messages")
    
    pass

