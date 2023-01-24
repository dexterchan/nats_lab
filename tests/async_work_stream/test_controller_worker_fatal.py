from async_work_stream.controller import Seq_Controller
from async_work_stream.worker import Worker
from async_work_stream.model import Seq_Workload_Envelope
import pytest
import uuid
from collections import defaultdict
from async_work_stream.model import WorkStatus
from utility.logging import get_test_logger
from async_work_stream.exception import WorkerThrowException

logger = get_test_logger(__name__)

test_total=1
exection_limit_seconds = test_total * 30

@pytest.fixture
def get_test_subject_Seq_Controller_worker_fatal() -> str:
    return "test_Seq_Controller_subject_worker_fatal"

@pytest.fixture
def get_test_stream_Seq_Controller_worker_fatal()->str:
    return "test_Seq_Controller_stream_worker_fatal"

@pytest.fixture
def get_test_message_retention_period()->int:
    return 10

@pytest.fixture
def get_job_id_defined() -> str:
    return "WORK_FAILURE"

@pytest.fixture
def get_my_first_job(get_first_job, get_job_id_defined) -> Seq_Workload_Envelope:
    first_job:Seq_Workload_Envelope = get_first_job(test_total)
    first_job.job_id = get_job_id_defined
    return first_job

@pytest.mark.asyncio
async def test_controller_failure(
    get_connection_details,
    get_test_subject_Seq_Controller_worker_fatal,
    get_test_stream_Seq_Controller_worker_fatal,
    get_test_message_retention_period,
    get_my_first_job
) -> None:
    conn_details:dict = get_connection_details

    _controller = Seq_Controller(
        hostname=conn_details.get("hostname"), 
        port=conn_details.get("port"),
        subject=get_test_subject_Seq_Controller_worker_fatal,
        persistance_stream_name=get_test_stream_Seq_Controller_worker_fatal,
        execution_limit_seconds= exection_limit_seconds,
        msg_retention_minutes=get_test_message_retention_period)

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
        new_workload:Seq_Workload_Envelope = msg.copy()
        new_workload.id += 1
        new_workload.last_status = WorkStatus.SUCCESS
        return new_workload, True
        
    
    ret_env, ex = await _controller.submit_seq_job(
        first_job=get_my_first_job,
        iterate_job_func=_iterate_message
    )

    assert process_counter_dict["n"] == 0
    
    assert ret_env.trial == 2
    assert type(ex) == WorkerThrowException
    logger.critical((ex))
    
    
    
@pytest.mark.asyncio
async def test_worker_fatal(
    get_connection_details,
    get_test_subject_Seq_Controller_worker_fatal,
    get_test_stream_Seq_Controller_worker_fatal,
    get_test_message_retention_period,
    get_job_id_defined
)->None:
    conn_details:dict = get_connection_details
    process_counter_dict:dict = defaultdict(int)
    collected_msg:dict = defaultdict(int)

    def _dummy_work_failure(work:Seq_Workload_Envelope)->bool:
        process_counter_dict["n"] += 1
        logger.info(f"Worker executes {process_counter_dict['n']}:Dummy failure worker Working on {work}")
        collected_msg[f"{work.job_id}-{work.id}"]+=1
        if process_counter_dict["n"] < Seq_Controller.MAX_RETRY:
            return False
        else:
            logger.critical(f"Worker throw exception at {process_counter_dict['n'] }")
            raise ValueError("Exception with purpose")

    conn_details:dict = get_connection_details

    worker = Worker(
        hostname=conn_details.get("hostname"), 
        port=conn_details.get("port"), 
        subject=get_test_subject_Seq_Controller_worker_fatal,
        persistance_stream_name=get_test_stream_Seq_Controller_worker_fatal,
        execution_limit_seconds=exection_limit_seconds,
        msg_retention_minutes=get_test_message_retention_period)

    await worker.listen_job_order(
        work_func=_dummy_work_failure,
        current_job_id=get_job_id_defined
    )
    assert len(collected_msg) == 1
    for key in collected_msg:
        assert collected_msg[key] == Seq_Controller.MAX_RETRY
    pass