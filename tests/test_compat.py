from healing.compat import task_checkpoint


def test_falls_back_to_a_noop_without_a_state_store():
    cp = task_checkpoint({})
    cp.set("last_batch_offset", 400)
    assert cp.get("last_batch_offset", 0) == 0


def test_uses_the_state_store_when_present():
    class Store(dict):
        def set(self, k, v, **_):
            self[k] = v

    store = Store()
    cp = task_checkpoint({"task_state_store": store})
    cp.set("last_batch_offset", 800)
    assert cp.get("last_batch_offset", 0) == 800
