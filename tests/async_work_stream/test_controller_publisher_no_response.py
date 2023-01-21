from async_work_stream.controller import Seq_Controller
from async_work_stream.worker import Worker
from async_work_stream.model import Seq_Workload_Envelope
import pytest
import uuid
from collections import defaultdict

from utility.logging import get_test_logger

logger = get_test_logger(__name__)
test_total=10
exection_limit_seconds = 10





@pytest.fixture
def get_test_subject_Seq_Controller_no_response() -> str:
    return "test_Seq_Controller_subject_no_response"

@pytest.fixture
def get_test_stream_Seq_Controller_no_response()->str:
    return "test_Seq_Controller_stream_no_response"

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
    get_test_subject_Seq_Controller_no_response,
     get_test_stream_Seq_Controller_no_response,
     get_my_first_job) -> None:
    
    conn_details:dict = get_connection_details
    logger.info(f"Start controller {conn_details}")


    _controller = Seq_Controller(
        hostname=conn_details.get("hostname"), 
        port=conn_details.get("port"),
        subject=get_test_subject_Seq_Controller_no_response,
        persistance_stream_name=get_test_stream_Seq_Controller_no_response,
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
        return None, False

    await _controller.submit_seq_job(
        first_job=get_my_first_job,
        iterate_job_func=_iterate_message
    )
    assert process_counter_dict["n"] == 1
    pass

@pytest.mark.asyncio
async def test_worker_without_response(
    get_connection_details,
    get_test_subject_Seq_Controller_no_response,
     get_test_stream_Seq_Controller_no_response,
     get_job_id_defined
)->None:
    conn_details:dict = get_connection_details
    logger.info(f"Start worker {conn_details}")
    process_counter_dict:dict = defaultdict(int)
    def _dummy_workload(work:Seq_Workload_Envelope)->bool:
        logger.info(f"Dummy Worker Working on {work}")
        process_counter_dict["n"]+=1
        return True
    logger.info(f"Worker taking jobid: {get_job_id_defined}")
    conn_details:dict = get_connection_details

    worker = Worker(
        hostname=conn_details.get("hostname"), 
        port=conn_details.get("port"), 
        subject=get_test_subject_Seq_Controller_no_response,
        persistance_stream_name=get_test_stream_Seq_Controller_no_response,
        execution_limit_seconds=exection_limit_seconds*10,
        msg_retention_minutes=test_message_retention_period)

    await worker.listen_job_order(
        work_func=_dummy_workload,
        current_job_id=get_job_id_defined
    )
    assert process_counter_dict["n"]==1
    pass