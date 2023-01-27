from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional, Any
from datetime import datetime, timedelta
import copy
from enum import Enum
import uuid

# WorkStatus_SUCCESS=1
# WorkStatus_FAIL=-1
# WorkStatus_RUNNING=0


class WorkStatus(int, Enum):
    """Work status"""
    SUCCESS = 1
    FAIL = -1
    RUNNING = 0
    FATAL = -2

class BatchStatus(int, Enum):
    LIVE = 1
    TERMINATE = 0

default_date_lambda = lambda:(int((datetime.now() + timedelta(hours=1)).timestamp()*1000))
current_date_lambda = lambda:(int(datetime.now().timestamp()*1000))
gen_tx_code = lambda:(str(uuid.uuid4().hex))

@dataclass
class Seq_Workload_Envelope:
    job_id:str
    id: int
    total: int
    payload:Any
    expiry_date: int = field(default_factory=default_date_lambda)
    trial:Optional[int]=0
    last_status:WorkStatus=WorkStatus.RUNNING
    batch_status:Optional[BatchStatus]=BatchStatus.LIVE
    timestamp:int = field(default_factory=current_date_lambda)
    txn_code:str = field(default_factory=gen_tx_code)
    
    def copy(self, replicate_txn_code:bool=False) -> Seq_Workload_Envelope:
        """copy the object itself

        Args:
            replicate_txn_code (bool, optional): note replicate tx_code. Defaults to False.

        Returns:
            Seq_Workload_Envelope: _description_
        """
        new_obj = copy.deepcopy(self)
        if not replicate_txn_code:
            new_obj.txn_code = gen_tx_code()
        return new_obj
    
    @staticmethod
    def current_time_stamp() -> int:
        return int(datetime.now().timestamp() * 1000)

    @staticmethod
    def calculate_expiry_date_timestamp(seconds:int) -> int:
        return int((datetime.now()+timedelta(seconds=seconds)).timestamp()*1000)
    