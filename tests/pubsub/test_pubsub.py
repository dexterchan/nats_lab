import pytest
from pubsub import nats_lab
import asyncio
import json
test_server = "localhost"
test_port: int = 4222
test_subject = "test_py"

@pytest.mark.asyncio
async def test_connect():
    p = nats_lab.Async_Publisher_Nats(server=test_server, port=test_port)

    
    await p.connect()
    await p.close()
    print("connect ok")

@pytest.mark.asyncio
async def test_publish():
    p = nats_lab.Async_Publisher_Nats(server=test_server, port=test_port)

    await p.connect()
    await p.publish(test_subject, payloads=[
        {"id":1,"start":1, "end":2, "total":10}
        ])

    await p.close()
    print("ok")
    
@pytest.mark.asyncio
async def test_publish_subscribe(event_loop):
    p = nats_lab.Async_Publisher_Nats(server=test_server, port=test_port)

    await p.connect()

    async def _handler(msg)->None:
        data = json.loads(msg.data.decode())
        print(data)
    await p.subscribe(
        subject=test_subject, message_handler=_handler
    )

    await p.publish(test_subject, payloads=[
        {"id":1,"start":1, "end":2, "total":10}
        ])
    #await asyncio.sleep(1, result=3)
    await p.close()
    print("ok")

@pytest.mark.asyncio
async def test_publish_subscribe_loop(event_loop):
    
    returned_message = []
    total_messages = 10
    

    p = nats_lab.Async_Publisher_Nats(server=test_server, port=test_port)
    await p.connect()
    finish_run = asyncio.Future()

    async def _handler(msg)->None:
        data = json.loads(msg.data.decode())
        id = data["id"]
        total = data["total"]
        print(data)
        returned_message.append(data)
        if id==total:
            finish_run.set_result(f"finish {total}")
        else:
            print(f"publish new message:{id+1}")
            await p.publish(
                    subject=test_subject,
                    payloads=[{
                        "id":id+1,
                        "start":data["start"]+1,
                        "end": data["end"]+1,
                        "total":total
                    }])
            
        
    await p.subscribe(
        subject=test_subject, message_handler=_handler
    )

    await p.publish(test_subject, payloads=[
        {"id":1,"start":1, "end":2, "total":10}
        ])
    #await asyncio.sleep(10, result=3)
    msg = await asyncio.wait_for(finish_run, timeout=10)

    await p.close()
    print(msg)
    assert len(returned_message) == total_messages