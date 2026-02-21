"""Backtest Report — generate metrics and visualizations."""
from __future__ import annotations
import json
import logging
from pathlib import Path
from typing import Optional

log = logging.getLogger("backtest.report")

try:
    import pandas as pd
    import numpy as np
except ImportError:
    pd = None
    np = None


class BacktestReport:
    """Generate reports from backtest results."""

    def summary(self, result) -> dict:
        """Generate summary metrics dict."""
        return {
            "total_pnl": round(result.total_pnl, 4),
            "realized_pnl": round(result.realized_pnl, 4),
            "unrealized_pnl": round(result.unrealized_pnl, 4),
            "total_fees": round(result.total_fees, 4),
            "total_volume": round(result.total_volume, 2),
            "fill_count": result.fill_count,
            "quote_count": result.quote_count,
            "fill_rate": round(result.fill_rate * 100, 2),
            "avg_spread_bps": round(result.avg_spread_bps, 1),
            "max_drawdown": round(result.max_drawdown, 4),
            "max_inventory": round(result.max_inventory, 2),
            "sharpe_ratio": round(result.sharpe_ratio, 3),
            "duration_sec": round(result.duration_sec, 2),
            "n_ticks": result.n_ticks,
            "config": result.config,
        }

    def to_json(self, result, path: str) -> None:
        """Save summary to JSON file."""
        summary = self.summary(result)
        with open(path, "w") as f:
            json.dump(summary, f, indent=2)
        log.info(f"Report saved to {path}")

    def to_html(self, result, path: str) -> None:
        """Generate HTML report with charts."""
        summary = self.summary(result)

        # PnL series as JSON for chart
        pnl_json = json.dumps(result.pnl_series[-1000:])  # last 1000 points
        inv_json = json.dumps(result.inventory_series[-1000:])
        spread_json = json.dumps(result.spread_series[-1000:])

        html = f"""<!DOCTYPE html>
<html><head>
<meta charset="UTF-8">
<title>MM Backtest Report</title>
<style>
body {{ font-family: -apple-system, sans-serif; background: #0a0e17; color: #e2e8f0; padding: 2rem; }}
h1 {{ color: #3b82f6; }}
.metrics {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin: 1rem 0; }}
.metric {{ background: #1a1f2e; padding: 16px; border-radius: 8px; text-align: center; }}
.metric-label {{ color: #94a3b8; font-size: 0.8rem; }}
.metric-value {{ font-size: 1.4rem; font-weight: 700; margin-top: 4px; }}
.positive {{ color: #22c55e; }}
.negative {{ color: #ef4444; }}
canvas {{ width: 100%; height: 200px; margin: 1rem 0; }}
.chart-container {{ background: #1a1f2e; padding: 16px; border-radius: 8px; margin: 1rem 0; }}
.config-table {{ width: 100%; border-collapse: collapse; margin: 1rem 0; }}
.config-table td {{ padding: 8px; border-bottom: 1px solid #1e293b; }}
.config-table td:first-child {{ color: #94a3b8; }}
</style>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
</head><body>
<h1>MM Backtest Report</h1>

<div class="metrics">
  <div class="metric">
    <div class="metric-label">Total PnL</div>
    <div class="metric-value {'positive' if summary['total_pnl'] >= 0 else 'negative'}">${{summary['total_pnl']:.4f}}</div>
  </div>
  <div class="metric">
    <div class="metric-label">Sharpe Ratio</div>
    <div class="metric-value {'positive' if summary['sharpe_ratio'] >= 1 else ''}">{summary['sharpe_ratio']:.3f}</div>
  </div>
  <div class="metric">
    <div class="metric-label">Max Drawdown</div>
    <div class="metric-value negative">${{summary['max_drawdown']:.4f}}</div>
  </div>
  <div class="metric">
    <div class="metric-label">Fill Rate</div>
    <div class="metric-value">{summary['fill_rate']:.1f}%</div>
  </div>
  <div class="metric">
    <div class="metric-label">Total Volume</div>
    <div class="metric-value">${{summary['total_volume']:.2f}}</div>
  </div>
  <div class="metric">
    <div class="metric-label">Fill Count</div>
    <div class="metric-value">{summary['fill_count']}</div>
  </div>
  <div class="metric">
    <div class="metric-label">Avg Spread</div>
    <div class="metric-value">{summary['avg_spread_bps']:.0f} bps</div>
  </div>
  <div class="metric">
    <div class="metric-label">Max Inventory</div>
    <div class="metric-value">{summary['max_inventory']:.1f}</div>
  </div>
</div>

<div class="chart-container">
  <h3>PnL Curve</h3>
  <canvas id="pnlChart"></canvas>
</div>

<div class="chart-container">
  <h3>Inventory Over Time</h3>
  <canvas id="invChart"></canvas>
</div>

<div class="chart-container">
  <h3>Spread Over Time (bps)</h3>
  <canvas id="spreadChart"></canvas>
</div>

<h3>Configuration</h3>
<table class="config-table">
{''.join(f'<tr><td>{k}</td><td>{v}</td></tr>' for k, v in summary['config'].items())}
</table>

<script>
const pnlData = {pnl_json};
const invData = {inv_json};
const spreadDataArr = {spread_json};
const labels = Array.from({{length: pnlData.length}}, (_, i) => i);

new Chart(document.getElementById('pnlChart'), {{
  type: 'line',
  data: {{ labels, datasets: [{{ data: pnlData, borderColor: '#22c55e', borderWidth: 1, pointRadius: 0, fill: true, backgroundColor: 'rgba(34,197,94,0.1)' }}] }},
  options: {{ plugins: {{ legend: {{ display: false }} }}, scales: {{ x: {{ display: false }}, y: {{ grid: {{ color: '#1e293b' }} }} }} }}
}});

new Chart(document.getElementById('invChart'), {{
  type: 'line',
  data: {{ labels, datasets: [{{ data: invData, borderColor: '#3b82f6', borderWidth: 1, pointRadius: 0 }}] }},
  options: {{ plugins: {{ legend: {{ display: false }} }}, scales: {{ x: {{ display: false }}, y: {{ grid: {{ color: '#1e293b' }} }} }} }}
}});

new Chart(document.getElementById('spreadChart'), {{
  type: 'line',
  data: {{ labels, datasets: [{{ data: spreadDataArr, borderColor: '#a855f7', borderWidth: 1, pointRadius: 0 }}] }},
  options: {{ plugins: {{ legend: {{ display: false }} }}, scales: {{ x: {{ display: false }}, y: {{ grid: {{ color: '#1e293b' }} }} }} }}
}});
</script>
</body></html>"""

        with open(path, "w") as f:
            f.write(html)
        log.info(f"HTML report saved to {path}")

    def print_summary(self, result) -> None:
        """Print summary to console."""
        s = self.summary(result)
        print("\n" + "=" * 50)
        print("  MM BACKTEST RESULTS")
        print("=" * 50)
        print(f"  Total PnL:     ${s['total_pnl']:>10.4f}")
        print(f"  Realized:      ${s['realized_pnl']:>10.4f}")
        print(f"  Unrealized:    ${s['unrealized_pnl']:>10.4f}")
        print(f"  Max Drawdown:  ${s['max_drawdown']:>10.4f}")
        print(f"  Sharpe Ratio:  {s['sharpe_ratio']:>10.3f}")
        print(f"  Fill Count:    {s['fill_count']:>10d}")
        print(f"  Fill Rate:     {s['fill_rate']:>9.1f}%")
        print(f"  Avg Spread:    {s['avg_spread_bps']:>8.0f} bps")
        print(f"  Total Volume:  ${s['total_volume']:>10.2f}")
        print(f"  Max Inventory: {s['max_inventory']:>10.1f}")
        print(f"  Duration:      {s['duration_sec']:>9.2f}s")
        print("=" * 50 + "\n")
