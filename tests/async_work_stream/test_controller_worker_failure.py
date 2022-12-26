from async_work_stream.controller import Seq_Controller
from async_work_stream.worker import Worker
from async_work_stream.model import Seq_Workload_Envelope
import pytest
import uuid
from collections import defaultdict
from async_work_stream.model import WorkStatus_SUCCESS
from utility.logging import get_test_logger

logger = get_test_logger(__name__)

test_total=1
exection_limit_seconds = test_total * 30

@pytest.fixture
def get_test_subject_Seq_Controller_worker_failure() -> str:
    return "test_Seq_Controller_subject_worker_failure"

@pytest.fixture
def get_test_stream_Seq_Controller_worker_failure()->str:
    return "test_Seq_Controller_stream_worker_failure"

@pytest.mark.asyncio
async def test_controller_failure(
    get_connection_details,
    get_test_subject_Seq_Controller_worker_failure,
    get_test_stream_Seq_Controller_worker_failure,
    get_first_job
) -> None:
    conn_details:dict = get_connection_details

    _controller = Seq_Controller(
        hostname=conn_details.get("hostname"), 
        port=conn_details.get("port"),
        subject=get_test_subject_Seq_Controller_worker_failure,
        persistance_stream_name=get_test_stream_Seq_Controller_worker_failure,
        execution_limit_seconds= exection_limit_seconds)

    process_counter_dict:dict = defaultdict(int)
    def _iterate_message(msg:Seq_Workload_Envelope) -> tuple[Seq_Workload_Envelope, bool]:
        """ expect no iteration

        Args:
            msg (Seq_Workload_Envelope): _description_

        Returns:
            tuple[Seq_Workload_Envelope, bool]: Next Message, Continue_Next?
        """
        process_counter_dict["n"] += 1
        if msg.id >= test_total:
            return None, False
        logger.debug(f"controller received: {msg}")
        return Seq_Workload_Envelope(
            job_id=msg.job_id,
            id=msg.id+1,
            total=msg.total,
            payload=msg.payload,
            trial=msg.trial,
            last_status=WorkStatus_SUCCESS
        ), True
    
    await _controller.submit_seq_job(
        first_job=get_first_job(test_total),
        iterate_job_func=_iterate_message
    )

    assert process_counter_dict["n"] == 0
    pass

@pytest.mark.asyncio
async def test_worker_failure(
    get_connection_details,
    get_test_subject_Seq_Controller_worker_failure,
    get_test_stream_Seq_Controller_worker_failure,
)->None:
    conn_details:dict = get_connection_details
    process_counter_dict:dict = defaultdict(int)
    collected_msg:dict = defaultdict(int)

    def _dummy_work_failure(work:Seq_Workload_Envelope)->bool:
        process_counter_dict["n"] += 1
        logger.info(f"Worker executes {process_counter_dict['n']}:Dummy failure worker Working on {work}")
        collected_msg[f"{work.job_id}-{work.id}"]+=1
        return False

    conn_details:dict = get_connection_details

    worker = Worker(
        hostname=conn_details.get("hostname"), 
        port=conn_details.get("port"), 
        subject=get_test_subject_Seq_Controller_worker_failure,
        persistance_stream_name=get_test_stream_Seq_Controller_worker_failure,
        execution_limit_seconds=exection_limit_seconds)

    await worker.listen_job_order(
        work_func=_dummy_work_failure
    )
    assert len(collected_msg) == 1
    for key in collected_msg:
        assert collected_msg[key] == Seq_Controller.MAX_RETRY
    pass