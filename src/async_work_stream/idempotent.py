

class Idempotent_Controller:
    def __init__(self) -> None:
        self._store:set= set()
        pass
    def check_and_insert_txn_id(self, txn_id:str) -> bool:
        if txn_id not in self._store:
            self._store.add(txn_id)
            return True
        return False
