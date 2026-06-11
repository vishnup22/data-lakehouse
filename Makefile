.PHONY: setup demo-env up down logs init-bucket produce stream stream-exec compact compact-once compact-daemon compact-dry-run benchmark benchmark-before benchmark-after benchmark-compare wait-eligible status test lint typecheck ci clean

setup:
	@if [ ! -f .env ]; then cp .env.example .env && echo "Created .env from .env.example"; else echo ".env already exists"; fi

demo-env:
	cp .env.demo .env
	@echo "Copied .env.demo — lower thresholds for faster local demos"

up: setup
	docker compose up -d --build redpanda minio minio-init spark compactor
	@echo ""
	@echo "Stack is up."
	@echo "  MinIO console : http://localhost:9001  (minioadmin / minioadmin)"
	@echo "  Spark UI      : http://localhost:8080"
	@echo "  Kafka         : localhost:19092"
	@echo ""
	@echo "Next steps:"
	@echo "  make produce   # terminal 2 — event producer"
	@echo "  make stream    # terminal 3 — Spark streaming job"

down:
	docker compose --profile producer --profile streaming down -v

logs:
	docker compose logs -f compactor spark streaming redpanda minio

init-bucket:
	docker compose run --rm minio-init

produce:
	docker compose --profile producer run --rm producer

stream:
	docker compose --profile streaming up streaming

stream-exec:
	docker compose exec spark bash /workspace/scripts/spark-streaming-submit.sh

compact:
	docker compose exec compactor python -m src.compactor.compaction_daemon --daemon

compact-once:
	docker compose exec compactor python -m src.compactor.compaction_daemon --once

compact-daemon:
	docker compose exec compactor python -m src.compactor.compaction_daemon --daemon

compact-dry-run:
	docker compose exec compactor python -m src.compactor.compaction_daemon --dry-run

benchmark:
	docker compose exec compactor python -m src.benchmark.benchmark_queries --phase auto

benchmark-before:
	docker compose exec compactor python -m src.benchmark.benchmark_queries --phase before

benchmark-after:
	docker compose exec compactor python -m src.benchmark.benchmark_queries --phase after

benchmark-compare:
	docker compose exec compactor python -m src.benchmark.benchmark_queries --phase auto

wait-eligible:
	@echo "Waiting 3 minutes for partition age guard (use .env.demo for 2 min skip)..."
	@sleep 180

status:
	docker compose ps

test:
	python -m pytest tests/ -v

lint:
	python -m ruff check .
	python -m ruff format --check .

typecheck:
	python -m mypy src

ci: lint typecheck test
	@echo "CI checks passed locally"

clean:
	-docker compose --profile producer --profile streaming down --remove-orphans 2>/dev/null
	rm -rf logs/*.jsonl logs/benchmark_report.md locks/compaction/*.lock .pytest_cache 2>/dev/null || true
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
