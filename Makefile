.PHONY: run test test-e2e deploy tunnels install-dev

# Run router + web UI (default)
run:
	uv run python -m src

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
	uv sync --extra dev
