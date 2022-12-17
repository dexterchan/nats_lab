import pytest
import asyncio
from streams.client import Async_EventBus_Nats
from streams.exception import SubjectNotFoundException
from typing import Any
from utility.logging import get_test_logger

logger = get_test_logger(__name__)

@pytest.mark.asyncio
async def test_connect(get_connection_details):
    conn_details:dict = get_connection_details
    p = Async_EventBus_Nats(
        server=conn_details["hostname"],
        port=conn_details["port"]
    )
    
    await p.connect()
    await asyncio.sleep(1)
    await p.close()
    print("connect ok")

@pytest.mark.asyncio
async def test_no_subject_when_publish_throw_exception(get_connection_details):
    conn_details:dict = get_connection_details

    p = Async_EventBus_Nats(
        server = conn_details["hostname"],
        port = conn_details["port"]
    )
    try:
        await p.connect()
        await asyncio.sleep(1)
        await p.publish(subject="invalid", payloads=[
            {
                "context": "invalid"
            }
        ])
        
        assert False
    except SubjectNotFoundException as subject_ex:
        assert True
    finally:
        await p.close()

@pytest.mark.asyncio
async def test_subject_registration(get_connection_details, get_test_stream, get_test_subject):
    conn_details:dict = get_connection_details

    p = Async_EventBus_Nats(
        server=conn_details["hostname"],
        port=conn_details["port"]
    )
    try:
        await p.connect()
        await p.register_subject_to_stream(
            stream_name=get_test_stream,
            subject=[get_test_subject]
        )
        await p.register_subject_to_stream(
            stream_name=get_test_stream,
            subject=get_test_subject
        )
    
    except SubjectNotFoundException as subject_ex:
        print(subject_ex)
    finally:
        await p.close()
    pass

@pytest.mark.asyncio
async def test_publish_subscribe(get_connection_details, get_test_stream, get_test_subject):
    conn_details:dict[str, Any] = get_connection_details
    p = Async_EventBus_Nats(
        server=conn_details.get("hostname"),
        port = conn_details.get("port")
    )
    num_of_messages = 3
    try:
        await p.connect()
        await p.register_subject_to_stream(
            stream_name=get_test_stream, subject=get_test_subject
        )
        assert get_test_subject in p.stream_subject_set, "subject not in the subject set"
        pull_subcription = await p.pull_subscribe(subject=get_test_subject, durable_name="test_publish_subscribe")

        payloads = [ {"id":i+1, "message":f"message{i}"} for i in range(num_of_messages)]
        await p.publish(subject=get_test_subject, payloads=payloads)

        async with p.pull_subscribe_fetch_message_helper(
                        pull_subscription=pull_subcription,
                        number_msgs=num_of_messages*2,
                        timeout_seconds=3
                    ) as messages:
            logger.info(f"returned {messages}")
            for m in messages:
                print(f"received: {m}")
                data = m.data
                print(data)
            assert len(messages) == num_of_messages
    except SubjectNotFoundException as subject_ex:
        print(subject_ex)
        raise subject_ex
    finally:
        await p.close()
    pass