import threading
import time

from cfquant.normal_bridge import NormalQmtBridge
from cfquant.protocol import ORDER_RECONCILE_VIEW, loads_message, pack_request
from cfquant.xttrader import XtQuantTrader


class RecordingTx(object):
    def __init__(self):
        self.responses = []

    def push(self, kind, payload, client_id):
        self.responses.append((kind, payload, client_id))


def _query_bridge(calls):
    bridge = NormalQmtBridge(
        object(),
        show=False,
        globals_dict={
            "get_trade_detail_data": lambda *args: calls.append(
                (threading.current_thread().name, args)
            ) or [{"m_dAvailable": 123.45}],
        },
        schedule_timer=False,
        dispatch_on_qmt_thread=True,
    )
    bridge.running = True
    bridge.tx = RecordingTx()
    return bridge


def test_qmt_thread_dispatch_pump_executes_query_on_caller_thread():
    calls = []
    bridge = _query_bridge(calls)

    bridge._handle_raw_from_thread(pack_request(
        "xttrader.query_stock_asset",
        params={
            "account": {
                "account_id": "A123",
                "account_type": "STOCK",
            },
        },
        client_id="web-client",
        request_id="request-1",
    ))

    assert bridge.worker_thread is None
    assert bridge.pump() == 1
    assert calls[0][0] == threading.current_thread().name
    assert calls[0][1] == ("A123", "stock", "account")
    response = loads_message(bridge.tx.responses[0][1])
    assert response["ok"] is True
    assert response["id"] == "request-1"


def test_qmt_thread_dispatch_timer_drains_coalesced_query_queue():
    calls = []
    bridge = _query_bridge(calls)

    for request_id in ("request-1", "request-2"):
        bridge._handle_raw_from_thread(pack_request(
            "xttrader.query_stock_positions",
            params={
                "account": {
                    "account_id": "A123",
                    "account_type": "STOCK",
                },
            },
            client_id="web-client",
            request_id=request_id,
        ))

    bridge.on_timer()

    assert len(calls) == 1
    assert len(bridge.tx.responses) == 2
    assert {loads_message(item[1])["id"] for item in bridge.tx.responses} == {
        "request-1",
        "request-2",
    }


def test_expired_order_request_is_rejected_before_qmt_dispatch():
    bridge = _query_bridge([])
    dispatched = []
    bridge._dispatch = lambda action, params, msg: dispatched.append(action)
    msg = loads_message(pack_request(
        "xttrader.order_stock_async",
        params={"stock_code": "512000.SH"},
        client_id="web-client",
        request_id="expired-order",
        timeout=1,
    ))

    bridge._drain_single_request("test", msg, time.time() - 2)

    assert dispatched == []
    response = loads_message(bridge.tx.responses[0][1])
    assert response["id"] == "expired-order"
    assert response["ok"] is False
    assert "expired before QMT dispatch" in response["error"]["message"]


def test_expired_coalesced_query_is_not_dispatched():
    calls = []
    bridge = _query_bridge(calls)
    msg = loads_message(pack_request(
        "xttrader.query_stock_positions",
        params={"account": {"account_id": "A123", "account_type": "STOCK"}},
        client_id="web-client",
        request_id="expired-query",
        timeout=1,
    ))
    key = bridge._coalesce_key(msg)
    bridge.coalesced_requests[key] = {"waiters": [(msg, time.time() - 2)]}

    bridge._drain_coalesced_request("test", msg, time.time() - 2, key)

    assert calls == []
    assert key not in bridge.coalesced_requests


def test_coalesced_query_uses_live_waiter_when_older_waiter_expired():
    calls = []
    bridge = _query_bridge(calls)
    params = {"account": {"account_id": "A123", "account_type": "STOCK"}}
    expired = loads_message(pack_request(
        "xttrader.query_stock_positions",
        params=params,
        client_id="web-client",
        request_id="expired-query",
        timeout=1,
    ))
    active = loads_message(pack_request(
        "xttrader.query_stock_positions",
        params=params,
        client_id="web-client",
        request_id="active-query",
        timeout=10,
    ))
    key = bridge._coalesce_key(expired)
    bridge.coalesced_requests[key] = {
        "waiters": [(expired, time.time() - 2), (active, time.time())],
    }

    bridge._drain_coalesced_request("test", expired, time.time() - 2, key)

    assert len(calls) == 1
    responses = [loads_message(item[1]) for item in bridge.tx.responses]
    assert [response["id"] for response in responses] == ["active-query"]
    assert responses[0]["ok"] is True


def test_compact_order_snapshot_preserves_terminal_rows_and_coalesces_traces():
    calls = []
    raw = {
        "m_strInstrumentID": "159928", "m_strExchangeID": "SZ", "m_nOrderType": 23,
        "m_nVolumeTotalOriginal": 100, "m_nVolumeTraded": 100, "m_dLimitPrice": 0.698,
        "m_nOrderStatus": 56, "m_strRemark": "YCJ1", "m_strOrderSysID": "", "m_nOrderID": 4321,
    }

    def query(*args):
        calls.append(args)
        return [raw]

    bridge = _query_bridge([])
    bridge.globals_dict["get_trade_detail_data"] = query
    logs = []
    bridge._log = logs.append
    account = {"account_id": "A123", "account_type": "STOCK"}
    for client in ("one", "two", "legacy"):
        params = {"account": account, "cancelable_only": False}
        if client != "legacy":
            params.update(snapshot_view=ORDER_RECONCILE_VIEW, snapshot_trace_id="trace-" + client)
        bridge._handle_raw_from_thread(pack_request(
            "xttrader.query_stock_orders", params=params, client_id=client, request_id="req-" + client,
        ))
    bridge.on_timer()

    assert calls == [("A123", "stock", "order")] * 2
    responses = {client: loads_message(raw)["result"] for _, raw, client in bridge.tx.responses}
    assert responses["one"] == responses["two"]
    compact = responses["one"]
    assert compact["snapshot_view"] == ORDER_RECONCILE_VIEW
    row = compact["orders"][0]
    assert row["order_remark"] == "YCJ1" and row["m_strOrderSysID"] == ""
    assert row["order_status"] == 56 and row["traded_volume"] == 100
    assert "instrument_name" not in row and "instrument_name" in responses["legacy"][0]
    arrival = [line for line in logs if "order_snapshot received" in line]
    timing = [line for line in logs if "order_snapshot" in line and "queue_ms=" in line]
    assert len(arrival) == 2
    assert len(timing) == 2
    for client in ("one", "two"):
        assert any("trace-" + client in line and "req-" + client in line for line in arrival)
        line = next(line for line in timing if "trace-" + client in line)
        assert all(token in line for token in ("req-" + client, "queue_ms=", "qmt_ms=", "pack_ms=", "send_ms="))


def test_sdk_compact_snapshot_supports_legacy_and_current_bridges():
    trader = XtQuantTrader.__new__(XtQuantTrader)
    account = {"account_id": "A123", "account_type": "STOCK"}
    row = {"stock_code": "159928.SZ", "order_status": 56, "m_strOrderSysID": ""}
    requests = []
    responses = iter([{"snapshot_view": ORDER_RECONCILE_VIEW, "orders": [row]}, [row]])

    def request(action, params):
        requests.append((action, params))
        return next(responses)

    trader._trade_request = request
    assert trader.query_order_reconcile_snapshot(account, trace_id="test-1") == [row]
    assert trader.query_order_reconcile_snapshot(account, trace_id="test-2")[0].stock_code == "159928.SZ"
    assert all(action == "xttrader.query_stock_orders" for action, _ in requests)
    assert [params["snapshot_trace_id"] for _, params in requests] == ["test-1", "test-2"]
