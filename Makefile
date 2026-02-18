.PHONY: run run-web run-cli run-telegram test test-e2e deploy tunnels install-dev

# Run orchestrator with web chat frontend (default)
run:
	uv run python -m src --config config.yaml

# Alias for `make run`
run-web:
	uv run python -m src --config config.yaml

# Run orchestrator with CLI frontend
run-cli:
	uv run python -m src --config config.yaml --cli

# Run orchestrator with Telegram bot frontend
run-telegram:
	uv run python -m src --config config.yaml --telegram

# Run unit/integration tests (no real Claude calls)
test:
	uv run pytest tests/ -v --ignore=tests/test_e2e.py

# Run end-to-end tests (calls real Claude, costs money)
test-e2e:
	uv run pytest tests/test_e2e.py -v

# Deploy broker to a remote server: make deploy HOST=user@server NAME=my-server
deploy:
	@test -n "$(HOST)" || (echo "Usage: make deploy HOST=user@server NAME=my-server" && exit 1)
	bash tools/deploy.sh $(HOST) $(NAME)

# Start SSH tunnels to remote brokers
tunnels:
	bash tools/start_tunnels.sh

# Install development dependencies
install-dev:
	uv sync --extra dev --extra telegram
