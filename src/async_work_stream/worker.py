from __future__ import annotations
from jetstreams.client import Async_EventBus_Nats
from utility.logging import get_logger
from typing import Tuple
import nats
from .model import Seq_Workload_Envelope, BatchStatus
from contextlib import asynccontextmanager
from .model import WorkStatus
from datetime import datetime, timedelta
from typing import Optional

logger = get_logger(__name__)

class Worker:
    def __init__(self,hostname:str, port:int, subject:str, persistance_stream_name:str, execution_limit_seconds:int=0, msg_retention_minutes:int=12*60) -> None:
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
        self.msg_retention_minutes = msg_retention_minutes
        pass

    @asynccontextmanager
    async def _init_work(self)->None:
        """ Connect to async event bus and create required subject and stream
        """
        self.p = Async_EventBus_Nats(
            server=self.hostname,
            port=self.port,
            msg_retention_minutes=self.msg_retention_minutes
        )
        await self.p.connect()
        await self.p.register_subject_to_stream(stream_name=self.my_stream, \
                subject=[self.job_submit_subject, self.job_feedback_subject])

        #pubsub = await p.pull_subscribe(subject=job_submit_subject, durable_name="test_worker1")
        yield
        await self.p.close()
    
    def _filter_in_job_message(self, current_job_id:str, msg:Seq_Workload_Envelope) -> bool:
        # if msg.batch_status == BatchStatus.TERMINATE:
        #     logger.info(f"Worker filter out msg since batch job is terminated {msg}")
        #     return False

        if current_job_id is None:
            return True

        if msg.job_id != current_job_id:
            logger.info(f"Worker filter out msg since current job id {current_job_id} not matching {msg}")
            return False
        
        return True
    
    
    def _determine_worker_still_alive(self,  current_job_id:str, last_msg:Seq_Workload_Envelope) -> bool:
        if current_job_id is  None:
            return True
        if current_job_id == last_msg.job_id and last_msg.batch_status == BatchStatus.TERMINATE:
            logger.info(f"Worker find batch {last_msg.job_id} terminated.... Worker stop here {last_msg}")
            return False
        
        return True
    
    def _check_worker_job_expiry(self, expiry_ex_datetime:datetime) -> bool:
        return True if self.execution_limit_seconds == 0 else (datetime.now().timestamp() < expiry_ex_datetime.timestamp())

    async def listen_job_order(self,  work_func, current_job_id:Optional[str]=None)->None:
        """listen job order from event bus

        Args:
            work_func (func(Seq_Workload_Envelope)->bool): work function to inject actual work
            current_job_id (Optional[str], optional): _description_. Defaults to None.
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
                        if not self._filter_in_job_message( current_job_id=current_job_id , msg=feedback_msg):
                            continue
                        if feedback_msg.batch_status == BatchStatus.LIVE:
                            feedback_msg.last_status = WorkStatus.RUNNING
                            try:
                                if work_func(workload):
                                    feedback_msg.last_status = WorkStatus.SUCCESS
                                else:
                                    feedback_msg.last_status = WorkStatus.FAIL
                            except Exception as ex:
                                feedback_msg.last_status = WorkStatus.FAIL

                            logger.info(f"Worker Published to {self.job_feedback_subject}:{feedback_msg}")
                            await self.p.publish(subject=self.job_feedback_subject
                                    , payloads=[feedback_msg.__dict__])

                        continue_read = continue_read and self._determine_worker_still_alive(
                                            current_job_id=current_job_id,
                                            last_msg=feedback_msg
                                        )
                except nats.errors.TimeoutError:
                    logger.info("Time out reading, try again")
                finally:
                    continue_read = continue_read and self._check_worker_job_expiry(expiry_ex_datetime=expiry_ex_datetime)
                
            logger.info(f"Worker quite execution: {datetime.now()} ; expiry {expiry_ex_datetime}")
        pass