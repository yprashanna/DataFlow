.PHONY: install run-pipeline run-api-pipeline schedule dashboard test docker-up docker-down clean help

# ── Setup ─────────────────────────────────────────────────────────────────────
install:
	pip install -r requirements.txt
	python generate_sample_data.py
	@echo "✅ DataFlow installed. Run 'make run-pipeline' to test."

# ── Run pipelines ─────────────────────────────────────────────────────────────
run-pipeline:
	@echo "▶ Running CSV sales pipeline..."
	python -c "
from orchestrator.config_parser import load_pipeline_config
from orchestrator.runner import PipelineRunner
config = load_pipeline_config('configs/sample_csv_pipeline.yml')
result = PipelineRunner(config).run()
print(f'Status: {result[\"status\"]} | Rows loaded: {result.get(\"rows_loaded\", 0)} | Latency: {result.get(\"total_latency_ms\", 0):.0f}ms')
"

run-api-pipeline:
	@echo "▶ Running Open-Meteo weather API pipeline..."
	python -c "
from orchestrator.config_parser import load_pipeline_config
from orchestrator.runner import PipelineRunner
config = load_pipeline_config('configs/sample_api_pipeline.yml')
result = PipelineRunner(config).run()
print(f'Status: {result[\"status\"]} | Rows loaded: {result.get(\"rows_loaded\", 0)} | Latency: {result.get(\"total_latency_ms\", 0):.0f}ms')
"

# ── Scheduler ─────────────────────────────────────────────────────────────────
schedule:
	@echo "▶ Starting APScheduler (Ctrl+C to stop)..."
	python -m orchestrator.scheduler

# ── Dashboard ─────────────────────────────────────────────────────────────────
dashboard:
	@echo "▶ Starting Streamlit dashboard on http://localhost:8501"
	streamlit run ui/app.py

# ── API ───────────────────────────────────────────────────────────────────────
api:
	@echo "▶ Starting FastAPI on http://localhost:8000 | Docs: http://localhost:8000/docs"
	uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload

# ── Tests ─────────────────────────────────────────────────────────────────────
test:
	pytest tests/ -v --tb=short

test-coverage:
	pytest tests/ -v --cov=. --cov-report=html --cov-report=term-missing

# ── Docker ────────────────────────────────────────────────────────────────────
docker-up:
	docker-compose up --build -d
	@echo "✅ DataFlow running:"
	@echo "   API:       http://localhost:8000"
	@echo "   Dashboard: http://localhost:8501"
	@echo "   API Docs:  http://localhost:8000/docs"

docker-down:
	docker-compose down

docker-logs:
	docker-compose logs -f

# ── Cleanup ───────────────────────────────────────────────────────────────────
clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null; true
	find . -name "*.pyc" -delete 2>/dev/null; true
	rm -f data/warehouse.db data/metadata.db
	@echo "🧹 Cleaned up."

# ── Help ─────────────────────────────────────────────────────────────────────
help:
	@echo ""
	@echo "DataFlow — Data Pipeline Orchestration Platform"
	@echo "================================================"
	@echo "  make install          Install dependencies & generate sample data"
	@echo "  make run-pipeline     Run the CSV sales pipeline"
	@echo "  make run-api-pipeline Run the Open-Meteo weather API pipeline"
	@echo "  make schedule         Start the cron scheduler"
	@echo "  make dashboard        Start Streamlit monitoring dashboard"
	@echo "  make api              Start FastAPI server"
	@echo "  make test             Run pytest test suite"
	@echo "  make docker-up        Start all services in Docker"
	@echo "  make docker-down      Stop Docker services"
	@echo "  make clean            Remove cache files and SQLite DBs"
	@echo ""
