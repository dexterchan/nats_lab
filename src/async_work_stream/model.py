from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Any

WorkStatus_SUCCESS=1
WorkStatus_FAIL=-1
WorkStatus_RUNNING=0

@dataclass
class Seq_Workload_Envelope:
    job_id:str
    id: int
    total: int
    payload:Any
    trial:Optional[int]=0
    last_status:int=0
    