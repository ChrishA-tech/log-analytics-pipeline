"""Dashboard-ready CSV extracts (load into Tableau Public / QuickSight) + a plotly HTML dashboard."""
import duckdb, os, plotly.graph_objects as go
from plotly.subplots import make_subplots
os.makedirs("extracts", exist_ok=True)
c = duckdb.connect(); R = "read_parquet('data/curated/requests/*/*.parquet')"
c.execute(f"COPY (SELECT date_trunc('day',ts) AS day, endpoint, count(*) AS requests, sum((status>=500)::int) AS errors, round(avg(latency_ms),1) AS avg_ms, round(quantile_cont(latency_ms,0.95),1) AS p95_ms FROM {R} GROUP BY 1,2 ORDER BY 1,2) TO 'extracts/daily_endpoint.csv' (HEADER)")
c.execute(f"COPY (SELECT region, device, source, count(*) AS requests, round(avg(latency_ms),1) avg_ms FROM {R} GROUP BY 1,2,3) TO 'extracts/region_device.csv' (HEADER)")
d = c.execute("SELECT * FROM 'extracts/daily_endpoint.csv'").df()
dd = d.groupby("day").agg(requests=("requests","sum"), errors=("errors","sum")).reset_index(); dd["err_pct"] = 100*dd.errors/dd.requests
ep = d.groupby("endpoint").agg(p95=("p95_ms","mean"), req=("requests","sum")).reset_index().sort_values("p95")
f = make_subplots(rows=1, cols=2, subplot_titles=("Daily requests and 5xx rate (%)", "Mean daily p95 latency by endpoint (ms)"), specs=[[{"secondary_y": True}, {}]])
f.add_bar(x=dd.day, y=dd.requests, name="requests", marker_color="#1f3a5f", row=1, col=1)
f.add_scatter(x=dd.day, y=dd.err_pct, name="5xx %", line=dict(color="#c0392b"), secondary_y=True, row=1, col=1)
f.add_bar(x=ep.p95, y=ep.endpoint, orientation="h", name="p95 ms", marker_color="#1f3a5f", row=1, col=2)
f.update_layout(title="Clickstream log analytics (synthetic data, 3.98M clean events)", height=480, template="simple_white")
f.write_html("extracts/dashboard.html", include_plotlyjs="cdn"); print(len(d), "rows; dashboard written")
