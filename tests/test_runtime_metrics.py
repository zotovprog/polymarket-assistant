from mm.runtime_metrics import RuntimeMetrics


def test_runtime_metrics_snapshot_includes_counts_and_cpu_pct():
    metrics = RuntimeMetrics()
    metrics.incr("feeds.pm.msg_recv", n=3)
    metrics.observe_ms("mm.run_loop.tick_duration_ms", 12.5)

    snap = metrics.snapshot(top_n=10)

    assert "process_cpu_pct" in snap
    assert snap["process_cpu_pct"] >= 0.0
    assert any(item["name"] == "feeds.pm.msg_recv" for item in snap["counts"])
    assert any(item["name"] == "mm.run_loop.tick_duration_ms" for item in snap["durations"])


def test_runtime_metrics_reset_clears_interval_only():
    metrics = RuntimeMetrics()
    metrics.incr("tg.poll.loop", n=2)

    first = metrics.snapshot(reset=True)
    assert any(item["name"] == "tg.poll.loop" for item in first["counts"])

    second = metrics.snapshot()
    assert all(item["interval_count"] == 0 for item in second["counts"]) or not second["counts"]
