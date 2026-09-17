# Grafana dashboard

`solobch-forge-dashboard.json` is a ready-made [Grafana](https://grafana.com/)
dashboard for SoloBCH Forge, driven by the `/metrics` Prometheus endpoint.

## 1. Scrape the metrics

SoloBCH Forge exposes Prometheus metrics at `http://<host>:3335/metrics`. Add a
scrape job to your `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: solobch-forge
    metrics_path: /metrics
    static_configs:
      - targets: ["<solobch-host>:3335"]
```

On Umbrel the status port (3335) sits behind the app-proxy, so point Prometheus
at the container over the internal Docker network (or an exposure you set up)
rather than the app-proxy URL.

## 2. Import the dashboard

In Grafana: **Dashboards → New → Import**, upload
`solobch-forge-dashboard.json`, and pick your Prometheus data source when
prompted.

## Panels

- Hashrate (5m), Workers, Best difficulty, Blocks found
- Node sync state, Network difficulty, Block reward (BCH), Stratum connections
- Pool hashrate over time (1m / 5m / 1h)
- Accepted vs rejected share rate
- Per-miner hashrate

All series come from the `solobch_*` metrics; see the app's `/metrics` output
for the full list.
