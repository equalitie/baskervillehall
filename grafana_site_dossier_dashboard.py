"""Generates grafana_site_dossier_dashboard.json: python3 grafana_site_dossier_dashboard.py <out.json>"""
import json, sys

DS = {"type": "grafana-postgresql-datasource", "uid": "PCC52D03280B7034C"}
H = "${host:sqlstring}"  # quoted and escaped by Grafana
NORM = r"regexp_replace(lower(host), '^www\.', '')"
panels = []
_id = [0]


def panel(kind, title, sql, x, y, w, h, fmt="table", **extra):
    _id[0] += 1
    p = {"id": _id[0], "type": kind, "title": title, "datasource": DS,
         "gridPos": {"x": x, "y": y, "w": w, "h": h},
         "targets": [{"datasource": DS, "editorMode": "code", "format": fmt, "rawQuery": True,
                      "rawSql": sql.strip(), "refId": "A"}]}
    p.update(extra)
    panels.append(p)
    return p


def row(title, y):
    _id[0] += 1
    panels.append({"id": _id[0], "type": "row", "title": title, "collapsed": False,
                   "gridPos": {"x": 0, "y": y, "w": 24, "h": 1}, "panels": []})


def stat(title, sql, x, y, w, unit="short", decimals=None, thresholds=None):
    steps = thresholds or [{"color": "blue", "value": None}]
    fc = {"defaults": {"unit": unit, "color": {"mode": "thresholds"},
                       "thresholds": {"mode": "absolute", "steps": steps}}, "overrides": []}
    if decimals is not None:
        fc["defaults"]["decimals"] = decimals
    panel("stat", title, sql, x, y, w, 4, fieldConfig=fc,
          options={"colorMode": "value", "graphMode": "none", "justifyMode": "center",
                   "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                   "textMode": "value"})


def table(title, sql, x, y, w, h, overrides=None, sort=None):
    opts = {"showHeader": True, "cellHeight": "sm"}
    if sort:
        opts["sortBy"] = [{"displayName": sort, "desc": True}]
    panel("table", title, sql, x, y, w, h,
          fieldConfig={"defaults": {"custom": {"inspect": True, "cellOptions": {"type": "auto"}}},
                       "overrides": overrides or []},
          options=opts)


def width(name, px):
    return {"matcher": {"id": "byName", "options": name}, "properties": [{"id": "custom.width", "value": px}]}


def link_host(name):
    return {"matcher": {"id": "byName", "options": name}, "properties": [{"id": "links", "value": [
        {"title": "Dossier", "url": "/d/site-dossier-v1/site-dossier?var-host=${__value.raw}&${__url_time_range}"}]}]}


P = f"(SELECT profile FROM site_dossier WHERE host = {H})"
red_after = lambda v: [{"color": "green", "value": None}, {"color": "orange", "value": v}, {"color": "red", "value": v * 5}]

y = 0
stat("Requests / hour (p50)", f"SELECT ({P}->'traffic'->>'req_per_hour_p50')::numeric", 0, y, 4)
stat("Requests / hour (p95)", f"SELECT ({P}->'traffic'->>'req_per_hour_p95')::numeric", 4, y, 4)
stat("Incidents 7 d", f"SELECT coalesce(({P}->'incidents'->>'n_7d')::int, 0)", 8, y, 3, thresholds=red_after(1))
stat("Incidents 30 d", f"SELECT coalesce(({P}->'incidents'->>'n_30d')::int, 0)", 11, y, 3, thresholds=red_after(5))
stat("Days with incidents (30 d)", f"SELECT coalesce(({P}->'incidents'->>'days_with_incidents_30d')::int, 0)", 14, y, 3,
     thresholds=red_after(3))
stat("Max spike 30 d", f"SELECT ({P}->'incidents'->>'max_spike_30d')::numeric", 17, y, 3, unit="x", decimals=0)
stat("Manual baseline", f"SELECT ({P}->>'manual_baseline')::numeric", 20, y, 2,
     thresholds=[{"color": "orange", "value": None}])
panel("stat", "Status", f"""
SELECT CASE WHEN ({P}->>'opted_out_or_whitelisted')::boolean THEN 'opted out / whitelisted' ELSE 'protected' END AS status""",
      22, y, 2, 4, options={"colorMode": "value", "graphMode": "none", "justifyMode": "center", "textMode": "value",
                            "reduceOptions": {"calcs": ["lastNotNull"], "fields": "/.*/", "values": False}},
      fieldConfig={"defaults": {"mappings": [
          {"type": "value", "options": {"protected": {"color": "green"}, "opted out / whitelisted": {"color": "orange"}}}]},
          "overrides": []})
y += 4

panel("timeseries", "Requests per hour (red: incidents)", f"""
SELECT to_timestamp(epoch_hour * 3600) AS time, sum(count) AS requests
FROM host_hourly_traffic
WHERE {NORM} = {H} AND epoch_hour * 3600 BETWEEN $__unixEpochFrom() AND $__unixEpochTo()
GROUP BY 1 ORDER BY 1""", 0, y, 24, 8, fmt="time_series",
      fieldConfig={"defaults": {"custom": {"drawStyle": "line", "fillOpacity": 15, "lineWidth": 1}}, "overrides": []},
      options={"legend": {"showLegend": False}, "tooltip": {"mode": "single"}})
y += 8

row("Profile", y); y += 1
for i, (name, label, title) in enumerate((("countries", "Country", "Top countries"), ("asns", "ASN", "Top ASNs"),
                                          ("urls", "URL", "Top URLs"))):
    table(f"{title} (current snapshot)", f"""
SELECT e->>'key' AS "{label}", (e->>'pct')::numeric AS "%"
FROM site_dossier, jsonb_array_elements(profile->'top_{name}') e WHERE host = {H}""", i * 6, y, 6, 6,
          overrides=[width("%", 70)])
table("Dossier facts", f"""
SELECT k AS fact, v AS value FROM (VALUES
  ('group', coalesce((SELECT group_id FROM site_dossier WHERE host = {H}), '—')),
  ('dnet', coalesce((SELECT string_agg(d, ', ') FROM jsonb_array_elements_text({P}->'dnet') d), '—')),
  ('peak hour UTC', {P}->'traffic'->>'peak_hour_utc'),
  ('quiet hour UTC', {P}->'traffic'->>'quiet_hour_utc'),
  ('traffic seen since', {P}->'traffic'->>'since'),
  ('manual baseline', coalesce(({P}->>'manual_baseline') || ' (deflect_alerts INCIDENT_BASELINE_OVERRIDES)', 'none')),
  ('baseline req/min (p50)', {P}->'incidents'->>'baseline_req_min_p50'),
  ('last incident', {P}->'incidents'->>'last'),
  ('exposed files 7 d', coalesce({P}->>'exposures_7d', '0')),
  ('SQLi attempts 7 d', coalesce({P}->>'sqli_7d', '0')),
  ('dossier version', (SELECT version || ' (' || to_char(updated_at, 'YYYY-MM-DD HH24:MI') || ' UTC, ' || updated_by || ')'
                       FROM site_dossier WHERE host = {H}))
) t(k, v)""", 18, y, 6, 6, overrides=[width("fact", 160)])
y += 6
table("Top user agents (current snapshot)", f"""
SELECT e->>'key' AS "User agent", (e->>'pct')::numeric AS "%"
FROM site_dossier, jsonb_array_elements(profile->'top_uas') e WHERE host = {H}""", 0, y, 16, 6,
      overrides=[width("%", 70)])
table("Same group (registrable domain)", f"""
SELECT d.host AS site, coalesce((d.profile->'incidents'->>'n_30d')::int, 0) AS "incidents 30 d",
       (d.profile->'traffic'->>'req_per_hour_p50')::numeric AS "req/h p50"
FROM site_dossier d
WHERE d.group_id = (SELECT group_id FROM site_dossier WHERE host = {H}) AND d.host <> {H}
ORDER BY 2 DESC, 1""", 16, y, 8, 6, overrides=[link_host("site")])
y += 6

table("All manual baselines (deflect_alerts INCIDENT_BASELINE_OVERRIDES)", """
SELECT host AS site, (profile->>'manual_baseline')::numeric AS baseline,
       (profile->'incidents'->>'baseline_req_min_p50')::numeric AS "incident baseline p50",
       (profile->'incidents'->>'n_30d')::int AS "incidents 30 d"
FROM site_dossier WHERE profile ? 'manual_baseline' ORDER BY host""", 0, y, 24, 4, overrides=[link_host("site")])
y += 4

row("Incidents and First Responder", y); y += 1
table("Incidents (time range)", f"""
SELECT started_at AS started, ended_at AS ended,
       round(extract(epoch FROM coalesce(ended_at, now()) - started_at) / 60) AS "minutes",
       round(spike_ratio::numeric, 1) AS spike, traffic_peak_count AS "peak req/min",
       round(baseline_avg::numeric) AS baseline, challenge_count AS "ML flagged",
       round(immature_ratio::numeric, 2) AS immature, id
FROM incidents WHERE host = {H} AND $__timeFilter(started_at)
ORDER BY started_at DESC LIMIT 500""", 0, y, 14, 9)
panel("barchart", "First Responder actions (time range)", f"""
SELECT action, count(*) AS n FROM first_responder_actions
WHERE host = {H} AND $__timeFilter(created_at) GROUP BY 1 ORDER BY 2 DESC""", 14, y, 10, 9,
      options={"orientation": "horizontal", "legend": {"showLegend": False}, "showValue": "auto",
               "xTickLabelRotation": 0}, fieldConfig={"defaults": {}, "overrides": []})
y += 9
table("First Responder actions with reasoning (time range)", f"""
SELECT created_at AS time, action, left(target, 120) AS target, confidence, ttl_minutes AS ttl,
       reasoning, incident_id
FROM first_responder_actions WHERE host = {H} AND $__timeFilter(created_at)
ORDER BY created_at DESC LIMIT 200""", 0, y, 24, 10,
      overrides=[width("time", 150), width("action", 130), width("target", 220), width("confidence", 90),
                 width("ttl", 60), width("incident_id", 90)])
y += 10

row("Support: exposed files and SQL injection (7 days)", y); y += 1
table("Exposed files", f"""
SELECT max(day) AS "last day", url AS path, content_type, sum(hits) AS hits, max(max_bytes) AS bytes
FROM exposures_daily WHERE {NORM} = {H} AND day > current_date - 7
GROUP BY url, content_type ORDER BY 1 DESC, 4 DESC""", 0, y, 10, 7)
table("SQL injection attempts", f"""
SELECT max(last_seen) AS "last seen", url AS path, sum(attempts) AS attempts, sum(code_500) AS "500s",
       max(max_time) AS "max s", max(signature) AS signature, sum(challenged_ips) AS challenged
FROM sqli_daily WHERE {NORM} = {H} AND day > current_date - 7
GROUP BY url ORDER BY 3 DESC""", 10, y, 14, 7)
y += 7

row("Knowledge (filled by the LLM step and by people)", y); y += 1
table("Campaigns", f"SELECT jsonb_array_elements(campaigns) AS campaign FROM site_dossier WHERE host = {H}", 0, y, 12, 6)
table("Known clients", f"SELECT jsonb_array_elements(known_clients) AS client FROM site_dossier WHERE host = {H}", 12, y, 12, 6)
y += 6
table("Open questions", f"SELECT jsonb_array_elements(open_questions) AS question FROM site_dossier WHERE host = {H}", 0, y, 8, 6)
table("Narrative", f"SELECT narrative, array_to_string(human_fields, ', ') AS \"set by people\" FROM site_dossier WHERE host = {H}",
      8, y, 16, 6)

dash = {
    "uid": "site-dossier-v1", "title": "Site dossier", "tags": ["baskerville", "dossier"],
    "editable": True, "graphTooltip": 1, "schemaVersion": 41, "version": 1, "refresh": "",
    "time": {"from": "now-30d", "to": "now"}, "timezone": "utc", "links": [],
    "annotations": {"list": [{
        "name": "Incidents", "enable": True, "iconColor": "red", "datasource": DS,
        "target": {"editorMode": "code", "format": "table", "rawQuery": True, "refId": "Anno", "rawSql":
                   f"SELECT started_at AS time, coalesce(ended_at, now()) AS timeend, "
                   f"'spike ' || round(spike_ratio::numeric) || 'x, peak ' || traffic_peak_count || ' req/min' AS text "
                   f"FROM incidents WHERE host = {H} AND $__timeFilter(started_at)"}}]},
    "templating": {"list": [{
        "name": "host", "label": "Site", "type": "query", "datasource": DS, "refresh": 1, "sort": 0,
        "includeAll": False, "multi": False,
        "query": "SELECT host FROM site_dossier "
                 "ORDER BY coalesce((profile->'incidents'->>'n_30d')::int, 0) DESC, host",
        "definition": "site_dossier hosts, most incidents first"}]},
    "panels": panels,
}
json.dump(dash, open(sys.argv[1], "w"), indent=2, ensure_ascii=False)
print(len(panels), "panels")
