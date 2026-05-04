export GOOGLE_APPLICATION_CREDENTIALS := $(PWD)/gcp.json

.PHONY: init-env tf-init tf-plan tf-apply docker-up docker-down run-producer run-flink dbt-run

init-env:
	uv venv --clear --python 3.11
	uv pip install "setuptools<70" wheel
	uv pip install pandas pyarrow confluent-kafka==2.3.0 apache-flink==1.18.1 google-cloud-storage google-cloud-bigquery dbt-bigquery --no-build-isolation
	@echo "Run 'source .venv/bin/activate' to activate the environment."

tf-init:
	cd terraform && terraform init

tf-plan:
	cd terraform && terraform plan

tf-apply:
	cd terraform && terraform apply -auto-approve

docker-up:
	docker-compose up -d

docker-down:
	docker-compose down

run-producer:
	uv run python src/kafka/producer.py

run-flink:
	uv run python src/flink/processor.py

sync-gcs:
	uv run python src/utils/sync_to_gcs.py

airflow-init:
	docker-compose up airflow-init

airflow-up:
	docker-compose up -d airflow-webserver airflow-scheduler

dbt-run:
	cd dbt_project && dbt run --profiles-dir .
