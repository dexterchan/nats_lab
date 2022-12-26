from jetstreams.client import Async_EventBus_Nats
import asyncio
from .model import Seq_Workload_Envelope, WorkStatus_RUNNING, WorkStatus_SUCCESS, WorkStatus_FAIL
from typing import Tuple
from datetime import datetime, timedelta
from utility.logging import get_logger
import nats
from contextlib import asynccontextmanager
from .exception import RetryException, TimeOutException

logger = get_logger(__name__)


class Seq_Controller:
    MAX_RETRY:int = 3

    def __init__(self, hostname:str, port:int, subject:str, persistance_stream_name:str, execution_limit_seconds:int) -> None:
        self.job_subject = subject
        self.job_submit_subject = f"{subject}_seq_job_submit"
        self.job_feedback_subject = f"{subject}_seq_job_feedback"
        self.my_stream = persistance_stream_name
        self.execution_limit_seconds = execution_limit_seconds
        self.hostname = hostname
        self.port = port
        self.durable_name = f"Seq_Controller"
        self._process_counter:int = 0
        
        pass
    
    @asynccontextmanager
    async def _init_controller(self)->None:
        """ Connect to async event bus and create required subject and stream
        """
        self.p = Async_EventBus_Nats(
            server=self.hostname,
            port=self.port
        )
        await self.p.connect()
        await self.p.register_subject_to_stream(stream_name=self.my_stream, \
                subject=[self.job_submit_subject, self.job_feedback_subject])
        yield
        await self.p.close()

    
    async def submit_seq_job(self, first_job:Seq_Workload_Envelope, iterate_job_func) -> Tuple[Seq_Workload_Envelope, Exception]:
        """ Submit sequential job

        Args:
            first_job (Seq_Workload_Envelope): the first job def_ion
            iterate_job_func (func(Seq_Workload_Envelope)): iterate job function with argument of Seq_Workload_Envelope

        Returns:
            Tuple[Seq_Workload_Envelope, Exception]: last processed job, exception if exists
        """
        last_job:Seq_Workload_Envelope = first_job
        pubsub = None
        async with self._init_controller():
            try:
                await self.p.publish(subject=self.job_submit_subject, payloads=[first_job.__dict__])
                logger.info("Publish done")
                # Listen to the job_feedback_subject
                pubsub = await self.p.pull_subscribe(subject=self.job_feedback_subject, durable_name=self.durable_name)
                
            except Exception as ex:
                return first_job, ex

            expiry_ex_datetime:datetime = datetime.now() + timedelta(seconds=self.execution_limit_seconds)

            work_done:bool = False
            try:
                while (datetime.now().timestamp() < expiry_ex_datetime.timestamp()) and (not work_done):
                    try:
                        logger.debug(f"controller subscribing to {self.job_feedback_subject}")
                        async with self.p.pull_subscribe_fetch_message_helper(
                                            pull_subscription=pubsub, number_msgs=3, timeout_seconds=2
                                        ) as messages:
                            for m in messages:
                                logger.info(f"controller received {m}")
                                received_msg = Seq_Workload_Envelope(**m)
                                
                                next_job, continue_next = self._process_feedback_message(msg=received_msg, iterate_job_func=iterate_job_func)
                                logger.info(f"controller will run next job: {continue_next} with msg: {next_job}")
                                if not continue_next:
                                    continue
                                last_job = next_job
                                await self.p.publish(subject=self.job_submit_subject, payloads=[next_job.__dict__])
                    except nats.errors.TimeoutError:
                        logger.info(f"Time out reading from subject:{self.job_feedback_subject}, wait again")
            except RetryException as retry_ex:
                return last_job, retry_ex
            except Exception as ex:
                return last_job, ex
            if datetime.now().timestamp() < expiry_ex_datetime.timestamp():
                logger.info(f"Controller Quit: Time out for the job {self.job_subject}")
                return last_job, TimeOutException(f"Controller Quit: Time out for the job {self.job_subject}")
        return last_job, None
    
    def _process_feedback_message(self, msg:Seq_Workload_Envelope, iterate_job_func) -> tuple[Seq_Workload_Envelope, bool]:
        """ process feedback in the controller

        Args:
            msg (Envelope): message received
            iterate_job_func (func(Seq_Workload_Envelope)->tuple[Seq_Workload_Envelope, bool]): iterate function; Return Next Msg, Continue Next?

        Raises:
            RetryException: _description_

        Returns:
            tuple[Envelope, bool]: Next Message, Continue_Next?
        """
        self._process_counter += 1
        
        if msg.id > msg.total:
            logger.info(f"Controller finish job {msg}")
            return None, False
        
        if msg.last_status == WorkStatus_SUCCESS:
            logger.info("Job was successful, iterate next job")
            next_msg, continue_next = iterate_job_func(msg)
            
            return Seq_Workload_Envelope(
                job_id=msg.job_id,
                id=msg.id+1,
                payload=next_msg.payload,
                total=msg.total
            ), continue_next
        if msg.trial+1 >= self.MAX_RETRY:
            logger.info(f"Controller quit retry job {msg}")
            raise RetryException(f"Max retry exceed at {msg}")
        
        logger.info(f"Controller retry job {msg}")
        return Seq_Workload_Envelope(
            job_id=msg.job_id,
            id=msg.id,
            payload=(msg.payload),
            total=msg.total,
            trial=msg.trial+1
        ), True