from jetstreams.client import Async_EventBus_Nats
import asyncio
from .model import Seq_Workload_Envelope, WorkStatus, BatchStatus
from typing import Tuple
from datetime import datetime, timedelta
from utility.logging import get_logger
import nats
from contextlib import asynccontextmanager
from .exception import RetryException, TimeOutException, WorkerThrowException

logger = get_logger(__name__)


class Seq_Controller:
    MAX_RETRY:int = 3

    def __init__(self, hostname:str, port:int, subject:str, persistance_stream_name:str, execution_limit_seconds:int, msg_retention_minutes:int=12*60) -> None:
        self.job_subject = subject
        self.job_submit_subject = f"{subject}_seq_job_submit"
        self.job_feedback_subject = f"{subject}_seq_job_feedback"
        self.my_stream = persistance_stream_name
        self.execution_limit_seconds = execution_limit_seconds
        self.hostname = hostname
        self.port = port
        self.durable_name = f"Seq_Controller"
        self._process_counter:int = 0
        self.msg_retention_minutes = msg_retention_minutes
        
        pass
    
    @asynccontextmanager
    async def _init_controller(self)->None:
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
        yield
        await self.p.close()

    def _check_message_eligible_to_process(self, job_id:str, first_job_timestamp:int, message:Seq_Workload_Envelope) -> bool:
        """ Check if we process the message based on criteria:
        - expiry?
        - job id? (not yet considered)

        Args:
            job_id (str): Job Id
            first_job_timestamp (int): first job timestamp
            message (Seq_Workload_Envelope): Message received

        Returns:
            bool: accept (True) or reject (False)
        """
        current_time = Seq_Workload_Envelope.current_time_stamp()
        #Check if job if match
        if job_id is not None and job_id != message.job_id:
            logger.info(f"Controller found {job_id} not matching {message}")
            return False
        
        if first_job_timestamp > message.timestamp:
            logger.info(f"Controller filter out timestamp {message} since first job timestamp {first_job_timestamp}")
            return False
            
        if message.expiry_date < current_time:
            logger.info(f"Controller job expired {message.expiry_date} current time:{current_time}")
            
            return False
        return True

    
    async def _finalize_batch(self, job_id:str, msg:Seq_Workload_Envelope) -> None:
        """ do the last job to finish the batch
        """
        termination_msg:Seq_Workload_Envelope = msg.copy()
        termination_msg.batch_status = BatchStatus.TERMINATE
        await self.p.publish(subject=self.job_submit_subject, payloads=[termination_msg.__dict__])
        logger.info(f"Controller finalized batch with job_id: {job_id} subject id:{self.job_submit_subject}")
        pass

    def _job_expired(self, expiry_ex_datetime:datetime) -> bool:
        if self.execution_limit_seconds == 0:
            return False
        
        return datetime.now().timestamp() > expiry_ex_datetime.timestamp()

    async def submit_seq_job(self, first_job:Seq_Workload_Envelope, iterate_job_func) -> Tuple[Seq_Workload_Envelope, Exception]:
        """ Submit sequential job

        Args:
            first_job (Seq_Workload_Envelope): the first job def_ion
            iterate_job_func (func(Seq_Workload_Envelope)): iterate job function with argument of Seq_Workload_Envelope

        Returns:
            Tuple[Seq_Workload_Envelope, Exception]: last processed job, exception if exists
        """
        last_job:Seq_Workload_Envelope = first_job
        current_job_id:str = first_job.job_id
        job_first_timestamp:int = first_job.timestamp
        
        
        pubsub = None
        async with self._init_controller():
            try:
                await self.p.publish(subject=self.job_submit_subject, payloads=[first_job.__dict__])
                logger.info(f"Publish {first_job} done at {datetime.now().timestamp()*1000}")
                # Listen to the job_feedback_subject
                pubsub = await self.p.pull_subscribe(subject=self.job_feedback_subject, durable_name=self.durable_name)
                
            except Exception as ex:
                return first_job, ex

            expiry_ex_datetime:datetime = datetime.now() + timedelta(seconds=self.execution_limit_seconds)

            work_done:bool = False
            try:
                while (not self._job_expired(expiry_ex_datetime=expiry_ex_datetime)) and (not work_done):
                    try:
                        logger.debug(f"controller subscribing to {self.job_feedback_subject}")
                        async with self.p.pull_subscribe_fetch_message_helper(
                                            pull_subscription=pubsub, number_msgs=3, timeout_seconds=2
                                        ) as messages:
                            for m in messages:
                                logger.info(f"controller received {m}")
                                received_msg = Seq_Workload_Envelope(**m)
                                if not self._check_message_eligible_to_process(
                                                job_id=current_job_id,
                                                first_job_timestamp=job_first_timestamp,
                                                message=received_msg):
                                    logger.info("Controller skipped this message which does not fit eligible criteria")
                                    continue
                                
                                next_job, continue_next = self._process_feedback_message(msg=received_msg, iterate_job_func=iterate_job_func)
                                if not next_job.job_id == current_job_id:
                                    logger.info(f"Controller {current_job_id} skips job: {next_job}")
                                    continue
                                if not continue_next:
                                    logger.info(f"Controller finished job: {current_job_id} last job status:{received_msg}")
                                    work_done = work_done or (not continue_next)
                                    break
                                logger.info(f"controller will run next job: {continue_next} with msg: {next_job}")

                                last_job = next_job
                                await self.p.publish(subject=self.job_submit_subject, payloads=[next_job.__dict__])
                    except nats.errors.TimeoutError:
                        logger.debug(f"Time out reading from subject:{self.job_feedback_subject}, wait again")
            
                if self._job_expired(expiry_ex_datetime=expiry_ex_datetime):
                    logger.info(f"Controller Quit: Time out for the job {self.job_subject}")
                    return last_job, TimeOutException(f"Controller Quit: Time out for the job {self.job_subject}")
            except RetryException as retry_ex:
                return last_job, retry_ex
            except BaseException as ex:
                return last_job, ex
            finally:
                await self._finalize_batch(job_id=current_job_id, msg=last_job)
                
            
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
        
        if msg.last_status == WorkStatus.SUCCESS:
            logger.info(f"Controller:last successful {msg}, iterate next job")
            next_msg, continue_next = iterate_job_func(msg)
            new_workload: Seq_Workload_Envelope = next_msg
            new_workload.trial = 0
            new_workload.last_status = WorkStatus.RUNNING
            logger.info(f"Controller prepares next job {new_workload}")
            return new_workload, continue_next
        elif msg.last_status == WorkStatus.FATAL:
            logger.error(f"Controller received FATAL from worker{msg}, stop the process")
            raise WorkerThrowException(f"Controller received FATAL from worker{msg}, stop the process")
            
        if msg.trial+1 >= self.MAX_RETRY:
            logger.info(f"Controller quit retry job {msg}")
            raise RetryException(f"Max retry exceed at {msg}")
        
        logger.info(f"Controller retry job {msg}")
        new_workload:Seq_Workload_Envelope = msg.copy()
        new_workload.trial += 1
        new_workload.last_status = WorkStatus.RUNNING
        logger.info(f"Controller prepares next job {new_workload}")
        return new_workload, True
        