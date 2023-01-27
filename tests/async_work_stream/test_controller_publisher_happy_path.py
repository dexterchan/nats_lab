from async_work_stream.controller import Seq_Controller
from async_work_stream.worker import Worker
from async_work_stream.model import Seq_Workload_Envelope
import pytest
import uuid
from collections import defaultdict
from async_work_stream.model import WorkStatus
from utility.logging import get_test_logger

logger = get_test_logger(__name__)
test_total:int = 10
exection_limit_seconds = test_total * 9

@pytest.fixture
def get_test_subject_Seq_Controller_happy_path() -> str:
    return "test_Seq_Controller_subject_happy_path"

@pytest.fixture
def get_test_stream_Seq_Controller_happy_path()->str:
    return "test_Seq_Controller_stream_happy_path"

@pytest.fixture
def get_test_message_retention_period()->int:
    return 10

@pytest.fixture
def get_job_id_defined() -> str:
    return "HAPPY_PATH"

@pytest.fixture
def get_my_first_job(get_first_job, get_job_id_defined) -> Seq_Workload_Envelope:
    first_job:Seq_Workload_Envelope = get_first_job(test_total)
    first_job.job_id = get_job_id_defined
    return first_job

@pytest.mark.asyncio
async def test_controller_happy_path(
    get_connection_details,
    get_test_subject_Seq_Controller_happy_path,
    get_test_stream_Seq_Controller_happy_path,
    get_my_first_job,
    get_test_message_retention_period) -> None:
    
    conn_details:dict = get_connection_details

    _controller = Seq_Controller(
        hostname=conn_details.get("hostname"), 
        port=conn_details.get("port"),
        subject=get_test_subject_Seq_Controller_happy_path,
        persistance_stream_name=get_test_stream_Seq_Controller_happy_path,
        execution_limit_seconds= exection_limit_seconds,
        msg_retention_minutes=get_test_message_retention_period)

    process_counter_dict:dict = defaultdict(int)
    def _iterate_message(msg:Seq_Workload_Envelope) -> tuple[Seq_Workload_Envelope, bool]:
        """ iterate each message in happy path until id matching total

        Args:
            msg (Seq_Workload_Envelope): _description_

        Returns:
            tuple[Seq_Workload_Envelope, bool]: Next Message, Continue_Next?
        """
        process_counter_dict["n"] += 1
        if msg.id >= test_total:
            return None, False
        logger.debug(f"controller received: {msg}")
        assert msg.trial == 0
        
        new_workload:Seq_Workload_Envelope = msg.copy()
        assert msg.txn_code != new_workload.txn_code, "txn code should be different"
        new_workload.id += 1
        new_workload.last_status = WorkStatus.SUCCESS
        return new_workload, True
        

    await _controller.submit_seq_job(
        first_job=get_my_first_job,
        iterate_job_func=_iterate_message
    )
    assert process_counter_dict["n"] == test_total
    pass


@pytest.mark.asyncio
async def test_worker_happy_path(
    get_connection_details,
    get_test_subject_Seq_Controller_happy_path,
     get_test_stream_Seq_Controller_happy_path,
     get_test_message_retention_period,
     get_job_id_defined
)->None:
    conn_details:dict = get_connection_details
    process_counter_dict:dict = defaultdict(int)
    def _dummy_happy_workload(work:Seq_Workload_Envelope)->bool:
        logger.info(f"Dummy Happy Path worker Working on {work}")
        check_id = (process_counter_dict['n']+1 == work.id)
        process_counter_dict["n"]+=1
        return check_id

    conn_details:dict = get_connection_details

    worker = Worker(
        hostname=conn_details.get("hostname"), 
        port=conn_details.get("port"), 
        subject=get_test_subject_Seq_Controller_happy_path,
        persistance_stream_name=get_test_stream_Seq_Controller_happy_path,
        execution_limit_seconds=exection_limit_seconds,
        msg_retention_minutes=get_test_message_retention_period)

    await worker.listen_job_order(
        work_func=_dummy_happy_workload,
        current_job_id=get_job_id_defined
    )
    assert process_counter_dict["n"]==test_total
    pass