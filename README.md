# Self-Healing Airflow Demo

Demo DAGs for the talk "Self-Healing Airflow: Pipelines That Fix Themselves". It runs on Airflow 3.3 in Docker and
sends alerts to Slack.

Three DAGs run the same payments pipeline with the same injected failures (rate limit, revoked API key, schema
drift, warehouse outage):

- `traditional_healing`: rule-based retries, fallbacks and a circuit breaker.
- `llm_healing`: a model decides retries and repairs failures.
- `hybrid_healing`: rules handle known errors, and a model handles the rest.

## Setup

1. Create an`.env` and fill in the values:
   - `SLACK_WEBHOOK_URL`: where alerts are posted. Optional.
   - `AIRFLOW_CONN_PYDANTICAI_DEFAULT`: the LLM connection. Without it, the LLM retry policies fall back to plain
     retries.
   - `SLACK_SIGNING_SECRET`, `AIRFLOW_API_USER` and `AIRFLOW_API_PASSWORD`: only needed for the Slack approver.
2. Start Airflow:

   ```bash
   make up
   ```

3. Open http://localhost:8080 and log in.

## Commands

| Command | What it does |
|---|---|
| `make traditional` / `make llm` / `make hybrid` | Trigger a DAG |
| `make outage-on` / `make outage-off` | Take the warehouse down or bring it back |
| `make reset` | Clear circuit breaker state |
| `make logs` | Follow the Airflow logs |
| `make test` | Run the unit tests |
| `make lint` | Run the linter |
| `make down` | Stop and remove the containers |

## Layout

```
dags/            the three DAGs
dags/healing/    retry policies, callbacks, alerts, circuit breaker
approver/        small server that handles Slack approve/deny buttons
tests/           unit tests
```
