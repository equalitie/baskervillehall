
ALTER TABLE public.sessions ADD COLUMN IF NOT EXISTS survey_country text;
  ALTER TABLE incidents ADD COLUMN IF NOT EXISTS first_responder_processed BOOLEAN DEFAULT FALSE;

ALTER TABLE incidents ADD COLUMN IF NOT EXISTS first_responder_processed BOOLEAN DEFAULT FALSE;

  CREATE TABLE IF NOT EXISTS first_responder_actions (
      id           BIGSERIAL PRIMARY KEY,
      incident_id  BIGINT NOT NULL REFERENCES incidents(id),
      host         TEXT NOT NULL,
      action       TEXT NOT NULL,
      target       TEXT NOT NULL DEFAULT '',
      confidence   TEXT NOT NULL,
      reasoning    TEXT,
      ttl_minutes  INTEGER NOT NULL DEFAULT 30,
      created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      expires_at   TIMESTAMPTZ NOT NULL,
      applied      BOOLEAN NOT NULL DEFAULT FALSE
  );
  CREATE INDEX IF NOT EXISTS first_responder_actions_host_expires
      ON first_responder_actions (host, expires_at DESC);
  

  docker buildx build --platform linux/amd64 -f ./Dockerfile_latest . \
    -t equalitie/baskervillehall:latest --push
  


  Шаг 3 — Деплой Ollama

  kubectl apply -f ollama_service.yaml
  kubectl apply -f ollama_deployment.yaml

  Ждём пока initContainer скачает модель (~2-5 мин):
  kubectl rollout status deployment/ollama
  kubectl exec deployment/ollama -- ollama list  # должно показать qwen2.5:7b
  
kubectl exec ollama-688fb9fbd9-4l5pc -c pull-model -- ollama pull qwen2.5:7b

deplying FirstResponder

kubectl apply -f config_baskervillehall.yaml
kubectl apply -f incident_first_responder_deployment.yaml
kubectl logs -f deployment/incident-first-responder


 Шаг 6 — Тест

 INSERT INTO incidents (host, command, window_seconds, challenge_count, baseline_avg, spike_ratio, started_at, survey_country)
  VALUES ('sudanile.com', 'challenge_ip', 60, 120, 15, 8.0, NOW(), 'SD');

  Через 30 секунд смотрим логи — должен появиться LLM вызов. Но для полного теста нужны данные в incident_country_stats и incident_asn_stats. Добавим их тоже:

  -- получи id только что созданного инцидента
  SELECT id FROM incidents ORDER BY id DESC LIMIT 1;

  -- вставь тестовое распределение (замени 123 на реальный id)
  INSERT INTO incident_country_stats (incident_id, country, cmd_count) VALUES
    (123, 'NL', 98),
    (123, 'DE', 15),
    (123, 'FR', 7);

  INSERT INTO incident_asn_stats (incident_id, asn_name, datacenter, cmd_count) VALUES
    (123, 'Hetzner Online GmbH', true, 85),
    (123, 'OVH SAS', true, 28),
    (123, 'Deutsche Telekom', false, 7);
