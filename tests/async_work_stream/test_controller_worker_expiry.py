from async_work_stream.controller import Seq_Controller
from async_work_stream.worker import Worker
from async_work_stream.model import Seq_Workload_Envelope
import pytest
import time
from collections import defaultdict
from async_work_stream.model import WorkStatus_SUCCESS
from utility.logging import get_test_logger

logger = get_test_logger(__name__)

test_total:int = 1
dummy_job_wait_seconds:int = 30


exection_limit_seconds:int =  10
blocking_job_id:str = "blocking_job"
success_job_id:str = "run_smooth_job"



@pytest.fixture
def get_test_subject_Seq_Controller_worker_expiry() -> str:
    return "test_Seq_Controller_subject_worker_expiry"

@pytest.fixture
def get_test_stream_Seq_Controller_worker_expiry()->str:
    return "test_Seq_Controller_stream_worker_expiry"

@pytest.mark.asyncio
async def test_controller_expiry(
    get_connection_details,
    get_test_subject_Seq_Controller_worker_expiry,
    get_test_stream_Seq_Controller_worker_expiry,
    get_first_job
)->None:
    conn_details:dict = get_connection_details
    
    _controller = Seq_Controller(
        hostname=conn_details.get("hostname"), 
        port=conn_details.get("port"),
        subject=get_test_subject_Seq_Controller_worker_expiry,
        persistance_stream_name=get_test_stream_Seq_Controller_worker_expiry,
        execution_limit_seconds= exection_limit_seconds)

    process_counter_dict:dict = defaultdict(int)
    def _iterate_message(msg:Seq_Workload_Envelope) -> tuple[Seq_Workload_Envelope, bool]:
        """ expect no iteration

        Args:
            msg (Seq_Workload_Envelope): _description_

        Returns:
            tuple[Seq_Workload_Envelope, bool]: Next Message, Continue_Next?
        """
        process_counter_dict[msg.job_id] += 1
        if msg.id >= test_total:
            return None, False
        logger.debug(f"controller received: {msg}")
        new_workload:Seq_Workload_Envelope = msg.copy()
        new_workload.id += 1
        new_workload.last_status = WorkStatus_SUCCESS
        return new_workload, True
        # return Seq_Workload_Envelope(
        #     job_id=msg.job_id,
        #     id=msg.id+1,
        #     total=msg.total,
        #     payload=msg.payload,
        #     trial=msg.trial,
        #     last_status=WorkStatus_SUCCESS
        # ), True
    
    block_job:Seq_Workload_Envelope = get_first_job(test_total)
    block_job.job_id = blocking_job_id
    await _controller.submit_seq_job(
        first_job=block_job,
        iterate_job_func=_iterate_message
    )
    assert process_counter_dict[block_job] == 0
    

    pass


@pytest.mark.asyncio
async def test_worker_expiry(
    get_connection_details,
    get_test_subject_Seq_Controller_worker_expiry,
    get_test_stream_Seq_Controller_worker_expiry,
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
        return False

    conn_details:dict = get_connection_details

    worker = Worker(
        hostname=conn_details.get("hostname"), 
        port=conn_details.get("port"), 
        subject=get_test_subject_Seq_Controller_worker_expiry,
        persistance_stream_name=get_test_stream_Seq_Controller_worker_expiry,
        execution_limit_seconds=exection_limit_seconds)

    await worker.listen_job_order(
        work_func=_dummy_work_expiry
    )
    assert len(collected_msg) == 2
    
    pass