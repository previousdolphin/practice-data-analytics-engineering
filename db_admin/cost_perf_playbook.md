# Cost & Performance Playbook

- Replace COUNT DISTINCT with HLL where appropriate.
- Use approx percentiles (t-digest/KLL) for p95/p99.
- Pre-aggregate hot dashboards; cache or materialized views.
- Partition on date; cluster/sort on selective columns; z-order in lakehouses.
- Compact small files; avoid SELECT *; broadcast small dims; skew handling.
- Monitor credits/slots and set guardrails in CI for cost deltas.
