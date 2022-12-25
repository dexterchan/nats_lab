from __future__ import annotations
from jetstreams.client import Async_EventBus_Nats
from utility.logging import get_logger
from typing import Tuple
import nats
from .model import Seq_Workload_Envelope
from contextlib import asynccontextmanager
from .model import WorkStatus_FAIL, WorkStatus_RUNNING, WorkStatus_SUCCESS
from datetime import datetime, timedelta

logger = get_logger(__name__)

class Worker:
    def __init__(self,hostname:str, port:int, subject:str, persistance_stream_name:str, execution_limit_seconds:int=0) -> None:
        self.job_subject = subject
        self.job_submit_subject = f"{subject}_seq_job_submit"
        self.job_feedback_subject = f"{subject}_seq_job_feedback"
        self.my_stream = persistance_stream_name
        
        self.hostname = hostname
        self.port = port
        self.durable_name = f"Worker"
        self._number_of_message_per_pull = 5
        self._timeout_read_seconds = 5
        self.execution_limit_seconds = execution_limit_seconds
        pass

    @asynccontextmanager
    async def _init_work(self)->None:
        """ Connect to async event bus and create required subject and stream
        """
        self.p = Async_EventBus_Nats(
            server=self.hostname,
            port=self.port
        )
        await self.p.connect()
        await self.p.register_subject_to_stream(stream_name=self.my_stream, \
                subject=[self.job_submit_subject, self.job_feedback_subject])

        #pubsub = await p.pull_subscribe(subject=job_submit_subject, durable_name="test_worker1")
        yield
        await self.p.close()

    async def listen_job_order(self, work_func)->None:
        """listen job order from event bus

        Args:
            work_func (func(Seq_Workload_Envelope)->bool): work function to inject actual work
        """
        
        continue_read:bool = True
        message_received:int = 0

        expiry_ex_datetime:datetime = datetime.now() + timedelta(seconds=self.execution_limit_seconds)

        async with self._init_work():
            pubsub = await self.p.pull_subscribe(subject=self.job_submit_subject, durable_name=self.durable_name)
            while continue_read:
                try:
                    seq_workload_enveloper_lst:list[Seq_Workload_Envelope] = []
                    async with  self.p.pull_subscribe_fetch_message_helper(
                            pull_subscription=pubsub,
                            number_msgs=self._number_of_message_per_pull,
                            timeout_seconds=self._timeout_read_seconds
                        ) as messages:
                        logger.info(f"Worker Received:{len(messages)} messages")
                        for m in messages:
                            logger.info(f"Worker Read Received:{m}")
                            message_received += 1
                            received_msg = Seq_Workload_Envelope(**m)
                            logger.info(f"Acknowledge Parsed message {received_msg}")
                            seq_workload_enveloper_lst.append(received_msg)
                    #Processed each message after acknowledge receive loop
                    for workload in seq_workload_enveloper_lst:
                        feedback_msg:Seq_Workload_Envelope = workload
                        feedback_msg.last_status = WorkStatus_RUNNING
                        try:
                            if work_func(workload):
                                feedback_msg.last_status = WorkStatus_SUCCESS
                            else:
                                feedback_msg.last_status = WorkStatus_FAIL
                        except Exception as ex:
                            feedback_msg.last_status = WorkStatus_FAIL

                        logger.info(f"Published to {self.job_feedback_subject}:{feedback_msg}")
                        await self.p.publish(subject=self.job_feedback_subject
                                , payloads=[feedback_msg.__dict__])
                except nats.errors.TimeoutError:
                    logger.info("Time out reading, try again")
                finally:
                    continue_read = True if self.execution_limit_seconds == 0 else (datetime.now().timestamp() < expiry_ex_datetime.timestamp())
        pass