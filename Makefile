DC = docker compose
AF = $(DC) exec -T airflow airflow

.PHONY: check-env up down logs traditional llm hybrid outage-on outage-off reset test lint approver-up approver-check

check-env:
	@test -f .env || { echo "missing .env: cp .env.example .env and fill it in"; exit 1; }
	@grep -q '^AIRFLOW_CONN_PYDANTICAI_DEFAULT=.' .env || echo "warning: AIRFLOW_CONN_PYDANTICAI_DEFAULT not set in .env; LLM policies will fall back"
	@grep -q '^SLACK_WEBHOOK_URL=.' .env || echo "warning: SLACK_WEBHOOK_URL not set in .env; Slack alerts will be skipped"

up: check-env
	$(DC) up -d
	@echo "Airflow: http://localhost:8080  (user admin, password is in: make logs)"

down:
	$(DC) down -v

logs:
	$(DC) logs -f airflow

traditional:
	$(AF) dags trigger traditional_healing

llm:
	$(AF) dags trigger llm_healing

hybrid:
	$(AF) dags trigger hybrid_healing

outage-on:
	$(AF) variables set warehouse_outage true

outage-off:
	$(AF) variables set warehouse_outage false

reset:
	-$(AF) variables delete breaker_warehouse
	-$(AF) variables set warehouse_outage false

test:
	python -m pytest -q

lint:
	ruff check . && ruff format --check .

approver-up: check-env
	@grep -q '^SLACK_SIGNING_SECRET=.' .env || { echo "set SLACK_SIGNING_SECRET in .env"; exit 1; }
	$(DC) --profile slack-approval up -d approver

approver-check:
	@curl -s -X POST localhost:8080/auth/token -H 'Content-Type: application/json' \
	  -d "{\"username\":\"$$AIRFLOW_API_USER\",\"password\":\"$$AIRFLOW_API_PASSWORD\"}" \
	  | python3 -c 'import sys,json;print(json.load(sys.stdin)["access_token"])' > .approver_token
	@curl -s localhost:8080/openapi.json -H "Authorization: Bearer $$(cat .approver_token)" \
	  | python3 -c 'import sys,json;[print(m.upper(),p) for p,v in json.load(sys.stdin)["paths"].items() if "hitl" in p.lower() for m in v]'
	@rm -f .approver_token
