from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional, Any
from datetime import datetime, timedelta
import copy

WorkStatus_SUCCESS=1
WorkStatus_FAIL=-1
WorkStatus_RUNNING=0

default_date_lambda = lambda:((datetime.now() + timedelta(hours=1)).timestamp()*1000)

@dataclass
class Seq_Workload_Envelope:
    job_id:str
    id: int
    total: int
    payload:Any
    expiry_date: int = field(default_factory=default_date_lambda)
    trial:Optional[int]=0
    last_status:int=0

    
    def copy(self) -> Seq_Workload_Envelope:
        # return Seq_Workload_Envelope(
        #     **self.__dict__
        # )
        return copy.deepcopy(self)
    