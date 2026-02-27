.PHONY: run run-telegram run-both test test-e2e deploy tunnels doctor install-dev
E2E_JOBS ?= 1
DOCTOR_ARGS ?=

# Run router + web UI (default)
run:
	uv run python -m src

# Run router + Telegram frontend (loads TELEGRAM_BOT_TOKEN from .env when present)
run-telegram:
	@set -a; [ -f .env ] && . ./.env; set +a; uv run python -m src --frontend telegram

# Run router + Web + Telegram together (loads TELEGRAM_BOT_TOKEN from .env when present)
run-both:
	@set -a; [ -f .env ] && . ./.env; set +a; uv run python -m src --frontend both

# Run unit/integration tests (no real Claude calls)
test:
	uv run pytest tests/ -v --ignore=tests/test_e2e.py

# Run end-to-end tests (calls real Claude)
test-e2e:
ifeq ($(E2E_JOBS),1)
	uv run pytest tests/test_e2e.py -v
else
	@uv run python -c "import xdist" >/dev/null 2>&1 || (echo "pytest-xdist is required for E2E_JOBS>1. Run: uv sync --extra dev" && exit 1)
	uv run pytest tests/test_e2e.py -v -n $(E2E_JOBS)
endif

# Deploy broker to a remote server: make deploy HOST=user@server NAME=my-server
deploy:
	@test -n "$(HOST)" || (echo "Usage: make deploy HOST=user@server NAME=my-server" && exit 1)
	bash tools/deploy.sh $(HOST) $(NAME)

# Start SSH tunnels to remote brokers
tunnels:
	bash tools/start_tunnels.sh

# Diagnose local<->remote daemon communication and registration
doctor:
	uv run python tools/doctor.py $(DOCTOR_ARGS)

# Install development dependencies
install-dev:
	uv sync --extra dev
