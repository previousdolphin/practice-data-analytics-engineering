-- SQL CHALLENGES (at least 12)
-- Use standard SQL; adapt to Snowflake/BigQuery/Databricks as needed.

-- 1) Window Fundamentals: For each user, compute first_session_at, last_session_at, sessions_7d.
-- Tables: events(user_id, event_type, ts)
-- Hints: MIN() OVER (PARTITION BY), MAX(), COUNT FILTER / SUM CASE in 7d window.

--// assuming that there are not multiple rows per session
SELECT DISTINCT
  user_id,
  MIN(ts) OVER (PARTITION BY user_id) AS first_session_at,
  MAX(ts) OVER (PARTITION BY user_id) AS last_session_at,
  COUNT(*) FILTER (WHERE ts >= CURRENT_DATE - INTERVAL '7 day')
    OVER (PARTITION BY user_id) AS sessions_7d
FROM events;

-- 2) Retention Cohorts: Build D1/D7 retention by signup cohort (signup_date).
-- Tables: users(user_id, signup_date), events(user_id, ts)
-- Output: cohort_date, d1_rate, d7_rate.

WITH cohorts AS (
  SELECT signup_date AS cohort_date,
         COUNT(*) AS cohort_size
  FROM users
  GROUP BY signup_date
),
user_day_events AS (
  -- one row per (user, event_date) after signup
  SELECT
    u.user_id,
    u.signup_date AS cohort_date,
    DATE(e.ts) AS event_date,
    /* BigQuery: DATE_DIFF(DATE(e.ts), u.signup_date, DAY)
       Snowflake: DATEDIFF('day', u.signup_date, DATE(e.ts))
       Postgres: DATE(e.ts) - u.signup_date */
    DATE_DIFF(DATE(e.ts), u.signup_date, DAY) AS days_since_signup
  FROM users u
  JOIN events e
    ON e.user_id = u.user_id
  WHERE DATE(e.ts) > u.signup_date
),
retention_flags AS (
  SELECT
    cohort_date,
    COUNT(DISTINCT CASE WHEN days_since_signup = 1 THEN user_id END) AS d1_users,
    COUNT(DISTINCT CASE WHEN days_since_signup = 7 THEN user_id END) AS d7_users
  FROM user_day_events
  GROUP BY cohort_date
)
SELECT
  c.cohort_date,
  COALESCE(ROUND(CAST(f.d1_users AS FLOAT) / c.cohort_size, 3), 0) AS d1_rate,
  COALESCE(ROUND(CAST(f.d7_users AS FLOAT) / c.cohort_size, 3), 0) AS d7_rate
FROM cohorts c
LEFT JOIN retention_flags f
  ON f.cohort_date = c.cohort_date
ORDER BY c.cohort_date;
-- 3) Funnel Conversion: View -> Click -> Purchase within 24h per user.
-- Tables: events(user_id, ts, name IN ('view','click','purchase'))
-- Output: overall conv rates, and by device.

-- 4) Skew-Aware Join: Join big fact to small dim efficiently (broadcast/semi-join). Provide both naive and optimized versions.

-- 5) COUNT DISTINCT -> HLL: Swap exact distinct of daily active users for HLL sketch merge across days.
-- BigQuery: HLL_COUNT.INIT(user_id), HLL_COUNT.MERGE(sketch)
-- Snowflake: HLL_ACCUMULATE/HLL_COMBINE/HLL_ESTIMATE

-- 6) Approx Percentiles: Compute p50/p95 latency via t-digest/KLL functions.
-- Output: by api_endpoint, day.

-- 7) Top-K Referrers: Approx Top-K without full sort (Count-Min Sketch or APPROX_TOP_K()).

-- 8) Smart Accumulation: Build cumulative first_seen_date efficiently (yesterday + today_delta) vs naive full scan.

-- 9) Partition + Clustering: Rewrite a slow query to prune by dt partition and cluster key (e.g., subreddit_id).
-- Show EXPLAIN/estimated bytes scanned differences in comments.

-- 10) Late-arriving Data: Implement watermarking; reprocess last N days only.
-- Output: upsert into curated table with MERGE semantics.

-- 11) Policy Decisions (T&S): Create fct_policy_decision from raw decisions, reports, and appeals with SCD2 policy rule dim join.

-- 12) Experiment Framework: Build exposure + outcomes tables; compute lift with CUPED or simple diff-in-means.

-- 13) Brand Safety Join (Ads x T&S): Percent of ad impressions in elevated-risk communities last 7d.
-- Tables: fct_ad_impression, dim_community, safety_risk_daily.

-- 14) Pre-aggregations: Create a materialized daily_usage rollup feeding BI; compare latency vs. raw scan.
