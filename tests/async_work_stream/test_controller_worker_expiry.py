from async_work_stream.controller import Seq_Controller
from async_work_stream.worker import Worker
from async_work_stream.model import Seq_Workload_Envelope
import pytest
import time
from collections import defaultdict
from async_work_stream.model import WorkStatus
from utility.logging import get_test_logger
from datetime import datetime, timedelta

logger = get_test_logger(__name__)

test_total:int = 1
dummy_job_wait_seconds:int = 5


exection_limit_seconds:int =  dummy_job_wait_seconds+10 * test_total
blocking_job_id:str = "blocking_job"
success_job_id:str = "run_smooth_job"



@pytest.fixture
def get_test_subject_Seq_Controller_worker_expiry() -> str:
    return "test_Seq_Controller_subject_worker_expiry"

@pytest.fixture
def get_test_stream_Seq_Controller_worker_expiry()->str:
    return "test_Seq_Controller_stream_worker_expiry"

@pytest.fixture
def get_test_message_retention_period()->int:
    return 10

@pytest.mark.asyncio
async def test_controller_expiry(
    get_connection_details,
    get_test_subject_Seq_Controller_worker_expiry,
    get_test_stream_Seq_Controller_worker_expiry,
    get_first_job,
    get_test_message_retention_period
)->None:
    conn_details:dict = get_connection_details
    
    _controller = Seq_Controller(
        hostname=conn_details.get("hostname"), 
        port=conn_details.get("port"),
        subject=get_test_subject_Seq_Controller_worker_expiry,
        persistance_stream_name=get_test_stream_Seq_Controller_worker_expiry,
        execution_limit_seconds= exection_limit_seconds,
        msg_retention_minutes=get_test_message_retention_period)

    controller_iterate_counter_dict:dict = defaultdict(int)
    def _iterate_message(msg:Seq_Workload_Envelope) -> tuple[Seq_Workload_Envelope, bool]:
        """ expect no iteration

        Args:
            msg (Seq_Workload_Envelope): _description_

        Returns:
            tuple[Seq_Workload_Envelope, bool]: Next Message, Continue_Next?
        """
        controller_iterate_counter_dict[msg.job_id] += 1
        if msg.id >= test_total:
            return None, False
        logger.debug(f"controller received: {msg}")
        new_workload:Seq_Workload_Envelope = msg.copy()
        new_workload.id += 1
        new_workload.last_status = WorkStatus.SUCCESS
        return new_workload, True
        
    
    block_job:Seq_Workload_Envelope = get_first_job(test_total)
    block_job.job_id = blocking_job_id
    block_job.expiry_date = Seq_Workload_Envelope.calculate_expiry_date_timestamp(seconds=dummy_job_wait_seconds)
    await _controller.submit_seq_job(
        first_job=block_job,
        iterate_job_func=_iterate_message
    )
    assert controller_iterate_counter_dict[blocking_job_id] == 0

    run_job:Seq_Workload_Envelope = get_first_job(test_total)
    run_job.job_id = success_job_id
    logger.info(f"Controller submits job {success_job_id}")
    await _controller.submit_seq_job(
        first_job=run_job,
        iterate_job_func=_iterate_message
    )
    assert controller_iterate_counter_dict[success_job_id] == test_total
    assert controller_iterate_counter_dict[blocking_job_id] == 0
    pass


@pytest.mark.asyncio
async def test_worker_expiry(
    get_connection_details,
    get_test_subject_Seq_Controller_worker_expiry,
    get_test_stream_Seq_Controller_worker_expiry,
    get_test_message_retention_period
)->None:
    conn_details:dict = get_connection_details
    process_counter_dict:dict = defaultdict(int)
    collected_msg:dict = defaultdict(int)

    def _dummy_work_expiry(work:Seq_Workload_Envelope)->bool:
        process_counter_dict["n"] += 1
        collected_msg[f"{work.job_id}-{work.id}"]+=1

        if work.job_id != blocking_job_id:
            logger.info(f"Worker: job id{work.job_id} continue")
            return True

        logger.info(f"Worker executes {work}:Dummy job wait {dummy_job_wait_seconds}")
        time.sleep(dummy_job_wait_seconds)
        logger.info("worker sleep finish... resume work")
        return False

    conn_details:dict = get_connection_details

    worker = Worker(
        hostname=conn_details.get("hostname"), 
        port=conn_details.get("port"), 
        subject=get_test_subject_Seq_Controller_worker_expiry,
        persistance_stream_name=get_test_stream_Seq_Controller_worker_expiry,
        execution_limit_seconds=exection_limit_seconds*2,
        msg_retention_minutes=get_test_message_retention_period)

    await worker.listen_job_order(
        work_func=_dummy_work_expiry
    )
    logger.debug(f"Worker processed {len(collected_msg)} msgs: {collected_msg}")
    assert len(collected_msg) ==  test_total + 1
    
    pass