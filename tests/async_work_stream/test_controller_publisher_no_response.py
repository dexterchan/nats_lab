from async_work_stream.controller import Seq_Controller
from async_work_stream.worker import Worker
from async_work_stream.model import Seq_Workload_Envelope
import pytest
import uuid
from collections import defaultdict

from utility.logging import get_test_logger

logger = get_test_logger(__name__)
test_total=10
exection_limit_seconds = 5
@pytest.fixture
def get_first_job() -> Seq_Workload_Envelope:
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
        
@pytest.fixture
def get_test_subject_Seq_Controller_no_response() -> str:
    return "test_Seq_Controller_subject_no_response"

@pytest.fixture
def get_test_stream_Seq_Controller_no_response()->str:
    return "test_Seq_Controller_stream_no_response"
    

@pytest.mark.asyncio
async def test_controller_without_response(
    get_connection_details,
    get_test_subject_Seq_Controller_no_response,
     get_test_stream_Seq_Controller_no_response,
     get_first_job) -> None:
    
    conn_details:dict = get_connection_details

    _controller = Seq_Controller(
        hostname=conn_details.get("hostname"), 
        port=conn_details.get("port"),
        subject=get_test_subject_Seq_Controller_no_response,
        persistance_stream_name=get_test_stream_Seq_Controller_no_response,
        execution_limit_seconds= exection_limit_seconds)

    process_counter_dict:dict = {"n":0}
    def _iterate_message(msg:Seq_Workload_Envelope) -> tuple[Seq_Workload_Envelope, bool]:
        """_summary_

        Args:
            msg (Seq_Workload_Envelope): _description_

        Returns:
            tuple[Seq_Workload_Envelope, bool]: Next Message, Continue_Next?
        """
        #process_counter += 1
        logger.debug(f"received: {msg}")
        process_counter_dict["n"] += 1
        return None, False

    await _controller.submit_seq_job(
        first_job=get_first_job,
        iterate_job_func=_iterate_message
    )
    assert process_counter_dict["n"] == 1
    pass

@pytest.mark.asyncio
async def test_worker_without_response(
    get_connection_details,
    get_test_subject_Seq_Controller_no_response,
     get_test_stream_Seq_Controller_no_response,
)->None:
    conn_details:dict = get_connection_details
    process_counter_dict:dict = {"n":0}
    def _dummy_workload(work:Seq_Workload_Envelope)->bool:
        logger.info("Dummy Worker Working on {work}")
        process_counter_dict["n"]+=1
        return True

    conn_details:dict = get_connection_details

    worker = Worker(
        hostname=conn_details.get("hostname"), 
        port=conn_details.get("port"), 
        subject=get_test_subject_Seq_Controller_no_response,
        persistance_stream_name=get_test_stream_Seq_Controller_no_response,
        execution_limit_seconds=exection_limit_seconds)

    await worker.listen_job_order(
        work_func=_dummy_workload
    )
    assert process_counter_dict["n"]==1
    pass