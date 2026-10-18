FROM apache/airflow:3.1.0

# The orchestrator runs producer collection passes in-process, so the
# ingestion package is installed into the Airflow image. Build context is
# the repo root.
COPY services/producers /opt/ly-ingest
RUN pip install --no-cache-dir /opt/ly-ingest
