from typing import Dict, List
import json
from typing import Dict, List, Set, Any
import nats
from nats.errors import TimeoutError
import asyncio
from nats.js.api import StreamConfig, RetentionPolicy
from typing import Union
from .exception import SubjectNotFoundException
from contextlib import asynccontextmanager
from nats.js.client import JetStreamContext
from utility.logging import get_logger
import backoff
logger = get_logger(__name__)
import logging
logging.getLogger('backoff').addHandler(logging.StreamHandler())

class Async_EventBus_Nats:
    def __init__(self, server: str, port: int, msg_retention_minutes:int = 12*60) -> None:
        self.url = f"nats://{server}:{port}"
        self.reconnect_seconds = 10
        self.stream_subject_set:Set = set()
        self.message_retention_seconds = msg_retention_minutes * 60
        pass
    
    async def connect(self):
        self.nc = await nats.connect(
            servers=[self.url],
            reconnect_time_wait=self.reconnect_seconds,
            verbose=True
        )
        logger.info(f"Connection to {self.url} done")
        self.js = self.nc.jetstream()
    
    #Back off required for Persistant NATS Stream when the first stream get created
    @backoff.on_exception(backoff.expo,
                      asyncio.TimeoutError,
                      max_tries=10,
                      jitter=backoff.random_jitter)
    async def register_subject_to_stream(self, stream_name:str, subject:Union[ str, list[str]]):
        """_summary_

        Args:
            stream_name (str): _description_
            subject (Union[ str, list[str]]): _description_

        Raises:
            Exception: _description_
        """
        if self.js is None:
            raise Exception("Connection has not been done; Cannot register subject to strean")
        subjects = subject if type(subject) == list else [subject]

        
        await self.js.add_stream(
            # name=stream_name, 
            # subjects=subjects, 
            config=StreamConfig(
                name=stream_name,
                subjects=subjects,
                retention=RetentionPolicy.LIMITS,
                max_age = self.message_retention_seconds
        ))
        logger.info(f"registered subjects:{subjects} to {stream_name}")
        self.stream_subject_set|=set(subjects)
        logger.info(f"collected subjects:{self.stream_subject_set}")

    #Back off required for Persistant NATS Stream when the first stream get created
    @backoff.on_exception(backoff.expo,
                      asyncio.TimeoutError,
                      max_tries=10,
                      jitter=backoff.random_jitter)
    async def publish(self, subject: str, payloads: list[dict]) -> None:
        """ publish a list of Dict Payload

        Args:
            subject (str): subject to publish (consumer get message from the same subject)
            payloads (List[Dict]): List of payload of Dict. Each Dict convert to json string

        Raises:
            SubjectNotFoundException: _description_
        """
        if subject not in self.stream_subject_set:
            raise SubjectNotFoundException(f"Subject {subject} not registed to any stream")
        for p in payloads:
            ack = await self.js.publish(subject=subject, payload=json.dumps(p).encode())
            logger.info(f'Ack: stream={ack.stream}, sequence={ack.seq}')

    async def pull_subscribe(self, subject:str, durable_name:str) -> JetStreamContext.PullSubscription:
        """ Pull Subscriber creation

        Args:
            subject (str): subject of the event bus to subscribe
            durable_name (str): Durable name of the consumer in case of resume

        Returns:
            JetStreamContext.PullSubscription: _description_
        """
        psub = await self.js.pull_subscribe(subject=subject, durable=durable_name)
        logger.info(f"Pull subscribe subject {subject} with durable name {durable_name}")
        return psub

    @asynccontextmanager
    async def pull_subscribe_fetch_message_helper(self, pull_subscription: JetStreamContext.PullSubscription, number_msgs:int, timeout_seconds:float) -> Any:
        msgs = await pull_subscription.fetch(batch=number_msgs, timeout=timeout_seconds)

        try:
            logger.info(f"Pull {len(msgs)} messages")
            json_msgs = [ json.loads(m.data) for m in msgs]
            yield json_msgs
        finally:
            for m in msgs:
                logger.info(f"acknowledge messages{m}")
                await m.ack()
        
        
    async def close(self) -> None:
        if self.nc is not None:
            logger.info("Close connection")
            await self.nc.close()
    
