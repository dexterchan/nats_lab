class TimeOutException(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)

class RetryException(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)