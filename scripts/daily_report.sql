-- Baskerville daily report: data for the last 24 hours (step 1 of BASKERVILLE_AGENT_DESIGN.md).
-- Read-only. Actions are reached through incidents (indexed on started_at), because
-- first_responder_actions has no index on created_at.
\set QUIET on
\pset footer off
SET statement_timeout = '60s';

CREATE TEMP TABLE inc AS
SELECT * FROM incidents WHERE started_at >= now() - interval '8 days';
CREATE TEMP TABLE act AS
SELECT a.*, i.spike_ratio, i.started_at AS inc_started
FROM first_responder_actions a JOIN inc i ON i.id = a.incident_id;

\echo '### Q1. Totals, last 24 h vs previous 7 days (per day)'
SELECT (SELECT count(*) FROM inc WHERE started_at >= now() - interval '1 day') AS incidents_24h,
       (SELECT round(count(*) / 7.0) FROM inc WHERE started_at < now() - interval '1 day') AS incidents_per_day_prev7,
       (SELECT count(DISTINCT host) FROM inc WHERE started_at >= now() - interval '1 day') AS hosts_24h,
       (SELECT count(*) FROM inc WHERE ended_at IS NULL) AS open_now,
       (SELECT count(*) FROM act WHERE created_at >= now() - interval '1 day' AND action NOT IN ('monitor_only','raise_threshold')) AS fr_actions_24h;

\echo '### Q2. New: hosts with incidents in 24 h and none in the previous 14 days'
SELECT i.host, count(*) AS incidents, round(max(i.spike_ratio)::numeric, 1) AS max_spike,
       max(i.traffic_peak_count) AS max_peak_req_min,
       string_agg(DISTINCT a.action, ',') AS fr_actions
FROM incidents i LEFT JOIN act a ON a.incident_id = i.id
WHERE i.started_at >= now() - interval '1 day'
  AND NOT EXISTS (SELECT 1 FROM incidents p WHERE p.host = i.host
                  AND p.started_at >= now() - interval '15 days' AND p.started_at < now() - interval '1 day')
GROUP BY i.host ORDER BY max(i.spike_ratio) DESC LIMIT 25;

\echo '### Q3. Ongoing: hosts with incidents on 3+ of the last 7 days'
SELECT host, count(DISTINCT started_at::date) AS days, count(*) AS incidents,
       count(*) FILTER (WHERE started_at >= now() - interval '1 day') AS last_24h,
       round(max(spike_ratio)::numeric, 1) AS max_spike,
       round((avg(spike_ratio) FILTER (WHERE started_at >= now() - interval '1 day'))::numeric, 1) AS avg_spike_24h,
       round((avg(spike_ratio) FILTER (WHERE started_at < now() - interval '1 day'))::numeric, 1) AS avg_spike_before
FROM inc WHERE started_at >= now() - interval '7 days'
GROUP BY host HAVING count(DISTINCT started_at::date) >= 3
ORDER BY incidents DESC LIMIT 25;

\echo '### Q4. First Responder actions in 24 h by type'
SELECT action, count(*), count(DISTINCT host) AS hosts
FROM act WHERE created_at >= now() - interval '1 day' GROUP BY 1 ORDER BY 2 DESC;

\echo '### Q5. Repeated actions: more than 3 of one non-trivial action on one host in 24 h'
SELECT host, action, count(*) FROM act
WHERE created_at >= now() - interval '1 day' AND action NOT IN ('monitor_only','raise_threshold')
GROUP BY 1, 2 HAVING count(*) > 3 ORDER BY 3 DESC LIMIT 20;

\echo '### Q6. monitor_only at spike >= 6x (possible missed attacks)'
SELECT host, count(*) AS n, round(max(spike_ratio)::numeric, 1) AS max_spike,
       left(max(reasoning), 160) AS sample_reasoning
FROM act WHERE created_at >= now() - interval '1 day' AND action = 'monitor_only' AND spike_ratio >= 6
GROUP BY host ORDER BY n DESC LIMIT 15;

\echo '### Q7. Actions with empty targets'
SELECT host, action, count(*) FROM act
WHERE created_at >= now() - interval '1 day' AND action NOT IN ('monitor_only','raise_threshold','challenge_all')
  AND coalesce(target, '') = '' GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 15;

\echo '### Q8. Collateral suspects: targets that look like good bots, monitoring, CMS/CDN tools, or the host itself'
SELECT host, action, left(target, 90) AS target, count(*)
FROM act
WHERE created_at >= now() - interval '1 day' AND action NOT IN ('monitor_only','raise_threshold','challenge_all')
  AND (target ~* '(googlebot|bingbot|applebot|duckduck|yandexbot|facebookexternalhit|meta-externalagent|twitterbot|linkedinbot|slackbot|telegrambot|whatsapp|uptimerobot|pingdom|statuscake|deflecthealthcheck|jetpack|wordpress|wp rocket|cloudflarebrowserrendering|feedly|inoreader|newsblur|rss)'
       OR position(split_part(host, '.', greatest(1, array_length(string_to_array(host, '.'), 1) - 1)) IN lower(target)) > 0)
GROUP BY 1, 2, 3 ORDER BY 4 DESC LIMIT 25;

\echo '### Q9. Commands sent in 24 h by type (challenge_command_history)'
SELECT command_type_name, count(*), count(DISTINCT host_name) AS hosts
FROM challenge_command_history WHERE session_end >= now() - interval '1 day'
GROUP BY 1 ORDER BY 2 DESC;

\echo '### Q10. Top hosts by commands in 24 h'
SELECT host_name, count(*), string_agg(DISTINCT command_type_name, ',') AS types
FROM challenge_command_history WHERE session_end >= now() - interval '1 day'
GROUP BY 1 ORDER BY 2 DESC LIMIT 15;

\echo '### Q11. LLM cost, 24 h'
SELECT source, count(*) AS calls, sum(input_tokens) AS input_tokens,
       round(sum(cost_usd)::numeric, 2) AS cost_usd
FROM llm_usage_log WHERE ts >= now() - interval '1 day' GROUP BY 1 ORDER BY 4 DESC NULLS LAST;
