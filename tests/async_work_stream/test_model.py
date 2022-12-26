from async_work_stream.model import Seq_Workload_Envelope


def test_seq_workload_env()->None:
    s1 = Seq_Workload_Envelope(
        job_id="a1",
        id=1,
        total=10,
        payload={"a":1},
    )

    s2 = s1.copy()
    assert s1 == s2
    assert s1.expiry_date == s2.expiry_date
    s1.payload["b"] = 3
    assert s1.payload != s2.payload
    assert s1.id == s2.id
    assert len(s2.payload) == 1
    