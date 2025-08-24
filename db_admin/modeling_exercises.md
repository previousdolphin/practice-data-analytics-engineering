# Modeling Exercises (Domain-Driven + Warehouse-Native)

1) Trust & Safety core facts: model `fct_report`, `fct_policy_decision`, `fct_moderation_action`, `fct_appeal` with shared dims.
2) Ads facts: `fct_ad_impression`, `fct_click`, `fct_conversion`, `fct_spend`.
3) Consumer facts: `fct_session`, `fct_event`, `fct_retention_cohort`.
4) Late-arriving data policy: watermark N days; MERGE semantics.
5) SCD2 dims: `dim_policy_rule`, `dim_community` with effective ranges.
6) File layout: partition by dt; cluster on selective keys; target 256MB parquet files.
