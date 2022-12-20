from jetstreams.client import Async_EventBus_Nats
import asyncio
from dataclasses import dataclass
import uuid
import pytest
from typing import Any, Optional
import nats
from datetime import datetime, timedelta

from utility.logging import get_test_logger

logger = get_test_logger(__name__)

test_cases = [
    "test_controller",
    "test_worker"
    ]

@dataclass
class Envelope:
    job_id:str
    id: int
    start:int
    end:int
    total: int
    interval: int
    trial:Optional[int]=0

test_interval:int = 10
test_total:int = 5

execution_limit:int = 5 * test_total

def _create_msg(job_id:str, id:int, total:int, interval:int) -> Envelope:
    return Envelope(
        job_id=job_id,
        id=id,
        start=id,
        end=id+id*(interval),
        total=total,
        interval=interval
    )

async def _controller(hostname:str, 
                    port:int, 
                    test_subject:str, 
                    test_stream:str, 
                    seed_payload:Envelope,
                    process_msg)->None:
    p = Async_EventBus_Nats(
            server=hostname,
            port=port
        )
    logger.info(f"Process controller: {test_subject}")
    await p.connect()
    
    job_submit_subject = f"{test_subject}_job_submit"
    job_feedback_subject = f"{test_subject}_job_feedback"
    await p.register_subject_to_stream(stream_name=test_stream, subject=[job_submit_subject, job_feedback_subject])
    

    payloads = [seed_payload.__dict__]
    # Publish the first message to start the async work flow into job_submit_subject
    await p.publish(subject=job_submit_subject, payloads=payloads)
    logger.info("Publish done")
    # Listen to the job_feedback_subject
    
    pubsub = await p.pull_subscribe(subject=job_feedback_subject, durable_name="controller")

    expiry_ex_datetime:datetime = datetime.now() + timedelta(seconds=execution_limit)

    work_done:bool = False
    while (datetime.now().timestamp() < expiry_ex_datetime.timestamp()) and (not work_done):
        try:
            logger.debug(f"controller subscribing to {job_feedback_subject}")
            async with p.pull_subscribe_fetch_message_helper(
                    pull_subscription=pubsub, number_msgs=3, timeout_seconds=2
                ) as messages:
                for m in messages:
                    logger.info(f"controller received {m}")
                    received_msg = Envelope(**m)
                    next_job, continue_next = process_msg(received_msg)
                    logger.info(f"controller will run next job: {continue_next} with msg: {next_job}")
                    if continue_next:
                        await p.publish(subject=job_submit_subject, payloads=[next_job.__dict__])
        except nats.errors.TimeoutError:
            logger.info(f"Time out reading from subject:{job_feedback_subject}, wait again")
    
    if datetime.now().timestamp() < expiry_ex_datetime.timestamp():
        logger.info(f"Time out for the job {test_subject}")
    
    await p.close()


@pytest.mark.skipif("test_controller" not in test_cases, reason="no need")
def test_controller_with_response(get_connection_details, get_test_subject_controller_normal_response, get_test_stream_controller_normal_response) -> None:
    
    start_env = _create_msg(job_id=uuid.uuid4().hex, id=1, total=test_total,interval=test_interval)
    conn_details:dict[str, Any] = get_connection_details
    process_counter_dict = {"n":0}

    def _process_feedback_message(msg:Envelope) -> tuple[Envelope, bool]:
        """ process feedback in the controller

        Args:
            msg (Envelope): message received

        Returns:
            tuple[Envelope, bool]: Next Message, Continue_Next?
        """
        process_counter_dict["n"] += 1
        
        if msg.id == msg.total:
            return None, False
        #Create a new message
        next_env = _create_msg(
            job_id=msg.job_id,
            id=msg.id+1,
            total=msg.total,
            interval=msg.interval
        )
        return next_env, True
    
    async def _process():
        await _controller(
            hostname=conn_details.get("hostname"),
            port=conn_details.get("port"),
            test_subject=f"{get_test_subject_controller_normal_response}",
            test_stream=get_test_stream_controller_normal_response,
            seed_payload=start_env,
            process_msg=_process_feedback_message
        )
        pass
    
    loop = asyncio.get_event_loop()
    loop.run_until_complete(_process())
    loop.close()
    assert process_counter_dict["n"] >= test_total, f'Controller processed {process_counter_dict["n"]} only, expecting {test_total}'
    logger.info(f"Controller processed {process_counter_dict['n']} messages")
    pass

@pytest.mark.skipif("test_worker" not in test_cases, reason="no need")
def test_worker_with_response (get_connection_details, get_test_subject_controller_normal_response, get_test_stream_controller_normal_response) -> None:
    job_submit_subject = f"{get_test_subject_controller_normal_response}_job_submit"
    job_feedback_subject = f"{get_test_subject_controller_normal_response}_job_feedback"
    test_stream = get_test_stream_controller_normal_response

    async def _process():
        conn_details:dict[str, Any] = get_connection_details
        p = Async_EventBus_Nats(
            server=conn_details.get("hostname"),
            port = conn_details.get("port")
        )
        await p.connect()

        
        await p.register_subject_to_stream(stream_name=test_stream, subject=[job_submit_subject, job_feedback_subject])

        pubsub = await p.pull_subscribe(subject=job_submit_subject, durable_name="test_worker1")

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
                        received_msg = Envelope(**m)
                        message_received += 1
                        logger.info(f"Worker received {received_msg}")
                        await p.publish(subject=job_feedback_subject, payloads=[received_msg.__dict__])
                        logger.info(f"Published to {job_feedback_subject}")
                    continue_read = (len(messages) > 0)

        except nats.errors.TimeoutError:
            logger.info("Time out reading")
        finally:
            await p.close()
        logger.info(f"Worker processed {message_received} messages")
    
    loop = asyncio.get_event_loop()
    loop.run_until_complete(_process())
    loop.close()
    pass

