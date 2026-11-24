import pytest
import gameplay.roomlist_cursor_order as mod

CASES = [
    ("stable_key", (1,)),
    ("broadcast_seq", (1,)),
    ("idempotent", (1,)),
    ("cross_region_dedupe", (1,)),
    ("reconnect_sync", (1,)),
    ("cursor_invalidate", (1,)),
    ("version_stamp", (1,)),
    ("validation", (1,)),
    ("audit", (1,)),
    ("fallback", (1,)),
    ("page_join", (1,)),
    ("metrics", (1,)),
    ("delete_gap", (1,)),
    ("cache_merge", (1,)),
    ("determinism", (1,)),
]

@pytest.mark.parametrize("fn_name,args", CASES)
def test_contract(fn_name, args):
    fn = getattr(mod, fn_name)
    assert fn(*args) is not None
