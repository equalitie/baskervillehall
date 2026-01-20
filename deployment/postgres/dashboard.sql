WITH hours AS (
  SELECT generate_series(
    date_trunc('hour', now() AT TIME ZONE 'UTC') - interval '23 hours',
    date_trunc('hour', now() AT TIME ZONE 'UTC'),
    interval '1 hour'
  ) AS bucket_hour
)
SELECT
  h.bucket_hour AS "time",              -- UTC
  COALESCE(c.challenged_ips, 0) AS challenged,
  COALESCE(p.passed_ips, 0)     AS passed
FROM hours h
LEFT JOIN public.dashboard_challenged_1h c
  ON c.bucket_hour = h.bucket_hour
LEFT JOIN public.dashboard_passed_1h p
  ON p.bucket_hour = h.bucket_hour
ORDER BY h.bucket_hour;

-- precision timeseries 24h
WITH hours AS (
  SELECT generate_series(
    date_trunc('hour', now() AT TIME ZONE 'UTC') - interval '23 hours',
    date_trunc('hour', now() AT TIME ZONE 'UTC'),
    interval '1 hour'
  ) AS bucket_hour
)
SELECT
  h.bucket_hour AS "time",               -- UTC
  COALESCE(p.precision_pct, 0)::DOUBLE PRECISION AS precision_pct
FROM hours h
LEFT JOIN public.dashboard_precision_1h p
  ON p.bucket_hour = h.bucket_hour
ORDER BY h.bucket_hour;



-- average precision 24h
SELECT
  ROUND(
    SUM(precision_pct * total_ips) / NULLIF(SUM(total_ips), 0),
    1
  )::DOUBLE PRECISION AS weighted_avg_precision_24h
FROM public.dashboard_precision_1h
WHERE
  bucket_hour >= date_trunc('hour', now() AT TIME ZONE 'UTC') - interval '23 hours';



--  human vs automated last 24h
WITH hours AS (
  SELECT generate_series(
    date_trunc('hour', now() AT TIME ZONE 'UTC') - interval '23 hours',
    date_trunc('hour', now() AT TIME ZONE 'UTC'),
    interval '1 hour'
  ) AS bucket_hour
),
agg AS (
  SELECT
    bucket_hour,
    COALESCE(SUM(cnt) FILTER (WHERE human_label = 'human'), 0) AS human,
    COALESCE(SUM(cnt) FILTER (WHERE human_label = 'bot'),   0) AS bot
  FROM public.dashboard_human_bot_1h
  WHERE
    bucket_hour >= date_trunc('hour', now() AT TIME ZONE 'UTC') - interval '23 hours'
  GROUP BY bucket_hour
)
SELECT
  h.bucket_hour AS "time",               -- UTC
  COALESCE(a.human, 0) AS human,
  COALESCE(a.bot, 0)   AS bot,
  ROUND(
    100.0 * COALESCE(a.human, 0)
    / NULLIF(COALESCE(a.human, 0) + COALESCE(a.bot, 0), 0),
    1
  )::DOUBLE PRECISION AS human_percenatage,
  ROUND(
    100.0 * COALESCE(a.bot, 0)
    / NULLIF(COALESCE(a.human, 0) + COALESCE(a.bot, 0), 0),
    1
  )::DOUBLE PRECISION AS bot_percentage
FROM hours h
LEFT JOIN agg a
  ON a.bucket_hour = h.bucket_hour
ORDER BY h.bucket_hour;
