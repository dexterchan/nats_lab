from async_work_stream.model import Seq_Workload_Envelope
from dataclasses import dataclass
from typing import Optional
import json
import time
@dataclass
class ConnectionDetails:
    hostname: str
    port: int
    stream_name: str
    subject_name: str
    msg_retention_period_minutes: Optional[int] = 12 * 60

def test_seq_workload_env()->None:
    s1 = Seq_Workload_Envelope(
        job_id="a1",
        id=1,
        total=10,
        payload={"a":1},
    )

    s2 = s1.copy(replicate_txn_code=True)
    assert s1 == s2
    assert s1.expiry_date == s2.expiry_date
    s1.payload["b"] = 3
    assert s1.payload != s2.payload
    assert s1.id == s2.id
    assert len(s2.payload) == 1
    
def test_json_loading(get_model_path) -> None:
    with open(get_model_path, "r") as f:
        conn_details_json: dict = json.load(f)
    print(conn_details_json)
    print (Seq_Workload_Envelope.__dataclass_fields__.keys())
    s = ConnectionDetails(**conn_details_json)
    assert s is not None


def test_json_loading(get_model_path) -> None:
    s = Seq_Workload_Envelope(
        job_id="a",
        id=1,
        payload={},
        total=1
    )
    time.sleep(1)
    s2 = Seq_Workload_Envelope(
        job_id="b",
        id=1,
        payload={},
        total=1
    )
    
    assert s is not None
    assert s2.expiry_date > s.expiry_date
    assert s2.timestamp > s.timestamp