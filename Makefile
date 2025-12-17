.PHONY: help setup start stop restart clean test test-unit test-contract test-integration \
        deploy-connectors monitor health check-replication test-replication \
        cleanup cleanup-full cleanup-connectors logs shell-scylla shell-postgres \
        install-deps fmt lint

.DEFAULT_GOAL := help

# Colors for output
BLUE := \033[0;34m
GREEN := \033[0;32m
YELLOW := \033[1;33m
RED := \033[0;31m
NC := \033[0m # No Color

##@ Quick Start

setup: ## Setup complete local environment (Docker + services + connectors)
	@echo "$(BLUE)Setting up complete CDC environment...$(NC)"
	@./scripts/setup-local.sh
	@echo "$(GREEN)✓ Environment ready!$(NC)"
	@echo ""
	@echo "Next steps:"
	@echo "  make deploy-connectors  # Deploy CDC connectors"
	@echo "  make health             # Check all services"
	@echo "  make test-replication   # Test CDC pipeline"

start: ## Start all Docker services
	@echo "$(BLUE)Starting Docker services...$(NC)"
	@docker compose -f docker/docker-compose.yml up -d
	@echo "$(GREEN)✓ Services started$(NC)"
	@echo "Waiting for services to be healthy..."
	@sleep 10
	@make health

stop: ## Stop all Docker services
	@echo "$(YELLOW)Stopping Docker services...$(NC)"
	@docker compose -f docker/docker-compose.yml stop
	@echo "$(GREEN)✓ Services stopped$(NC)"

restart: stop start ## Restart all Docker services

clean: ## Stop services and remove volumes (data will be lost)
	@echo "$(RED)WARNING: This will delete all data!$(NC)"
	@./scripts/teardown-local.sh

clean-keep-volumes: ## Stop services but keep data volumes
	@echo "$(YELLOW)Stopping services (keeping data)...$(NC)"
	@./scripts/teardown-local.sh --keep-volumes

##@ Connectors

deploy-connectors: ## Deploy CDC connectors (source + sink)
	@echo "$(BLUE)Deploying CDC connectors...$(NC)"
	@./scripts/deploy-connectors.sh
	@echo "$(GREEN)✓ Connectors deployed$(NC)"
	@make monitor

monitor: ## Monitor connector status
	@./scripts/monitor-connectors.sh

restart-connectors: ## Restart CDC connectors (quick fix for stuck connectors)
	@echo "$(BLUE)Restarting connectors only...$(NC)"
	@./scripts/cleanup-test-env.sh --keep-data

##@ Testing & Validation

test: install-deps cleanup-full ## Run all tests with fresh environment
	@echo "$(BLUE)Running all tests...$(NC)"
	@.venv/bin/pytest -v

test-unit: install-deps ## Run unit tests only
	@echo "$(BLUE)Running unit tests...$(NC)"
	@.venv/bin/pytest tests/unit/ -v

test-contract: install-deps cleanup-full ## Run contract tests with fresh environment
	@echo "$(BLUE)Running contract tests...$(NC)"
	@.venv/bin/pytest tests/contract/ -v

test-integration: install-deps cleanup-full ## Run integration tests with fresh environment
	@echo "$(BLUE)Running integration tests...$(NC)"
	@.venv/bin/pytest tests/integration/ -v

test-replication: ## Quick test CDC replication (INSERT/UPDATE/DELETE)
	@echo "$(BLUE)Testing CDC replication...$(NC)"
	@./scripts/test-replication.sh --table users --wait 30

check-replication: ## Verify CDC replication status and data completeness
	@echo "$(BLUE)Checking replication status...$(NC)"
	@docker exec postgres psql -U postgres -d warehouse -c "\
		SELECT category, metric, expected_value, actual_value, validation_status \
		FROM cdc_data.validation_summary \
		ORDER BY category, validation_status;"
	@echo ""
	@echo "Completeness:"
	@docker exec postgres psql -U postgres -d warehouse -c "\
		SELECT cdc_data.get_replication_completeness() || '%' AS completeness, \
		       cdc_data.is_replication_complete() AS all_passed;"

##@ Environment Cleanup

cleanup: ## Standard cleanup - truncate data + restart connectors
	@echo "$(BLUE)Running standard cleanup...$(NC)"
	@./scripts/cleanup-test-env.sh

cleanup-full: ## Full fresh-start - clear data + reset Kafka offsets + recreate connectors
	@echo "$(BLUE)Running FULL cleanup (fresh-start)...$(NC)"
	@./scripts/cleanup-test-env.sh --full
	@echo "$(GREEN)✓ Environment reset to fresh state$(NC)"

cleanup-connectors: ## Only restart connectors (preserve data)
	@./scripts/cleanup-test-env.sh --keep-data

##@ Health & Monitoring

health: ## Check health of all services
	@./scripts/health-check.sh

health-verbose: ## Detailed health check with verbose output
	@./scripts/health-check.sh --verbose

logs: ## Show logs from all services (follow mode)
	@docker compose -f docker/docker-compose.yml logs -f

logs-kafka-connect: ## Show Kafka Connect logs
	@docker logs kafka-connect -f --tail 100

logs-scylla: ## Show ScyllaDB logs
	@docker logs scylla -f --tail 100

logs-postgres: ## Show PostgreSQL logs
	@docker logs postgres -f --tail 100

##@ Database Access

shell-scylla: ## Open CQL shell to ScyllaDB
	@docker exec -it scylla cqlsh

shell-postgres: ## Open psql shell to PostgreSQL
	@docker exec -it postgres psql -U postgres -d warehouse

query-scylla-users: ## Query ScyllaDB users table
	@docker exec scylla cqlsh -e "SELECT * FROM app_data.users LIMIT 10;"

query-postgres-users: ## Query PostgreSQL users table
	@docker exec postgres psql -U postgres -d warehouse -c "\
		SELECT username, email, status, first_name, last_name \
		FROM cdc_data.users \
		ORDER BY cdc_timestamp DESC \
		LIMIT 10;"

count-scylla: ## Show row counts in ScyllaDB
	@echo "$(BLUE)ScyllaDB row counts:$(NC)"
	@docker exec scylla cqlsh -e "\
		SELECT COUNT(*) as users FROM app_data.users; \
		SELECT COUNT(*) as products FROM app_data.products; \
		SELECT COUNT(*) as orders FROM app_data.orders;"

count-postgres: ## Show row counts in PostgreSQL
	@echo "$(BLUE)PostgreSQL row counts:$(NC)"
	@docker exec postgres psql -U postgres -d warehouse -c "\
		SELECT 'users' as table, COUNT(*) as count FROM cdc_data.users \
		UNION ALL SELECT 'products', COUNT(*) FROM cdc_data.products \
		UNION ALL SELECT 'orders', COUNT(*) FROM cdc_data.orders \
		UNION ALL SELECT 'order_items', COUNT(*) FROM cdc_data.order_items \
		UNION ALL SELECT 'inventory_transactions', COUNT(*) FROM cdc_data.inventory_transactions \
		ORDER BY table;"

##@ Development

install-deps: ## Install Python dependencies
	@if [ ! -d ".venv" ]; then \
		echo "$(BLUE)Creating Python virtual environment...$(NC)"; \
		python3 -m venv .venv; \
	fi
	@echo "$(BLUE)Installing dependencies...$(NC)"
	@.venv/bin/pip install -q --upgrade pip
	@.venv/bin/pip install -q -r requirements.txt
	@echo "$(GREEN)✓ Dependencies installed$(NC)"

fmt: install-deps ## Format Python code with black
	@echo "$(BLUE)Formatting Python code...$(NC)"
	@.venv/bin/black src/ tests/
	@echo "$(GREEN)✓ Code formatted$(NC)"

lint: install-deps ## Lint Python code with ruff
	@echo "$(BLUE)Linting Python code...$(NC)"
	@.venv/bin/ruff check src/ tests/

##@ Quick Commands (for CI/CD)

ci-setup: ## CI: Setup environment for testing
	@echo "$(BLUE)Setting up CI environment...$(NC)"
	@make setup
	@make deploy-connectors
	@make cleanup-full

ci-test: ## CI: Run all tests
	@make test-unit
	@make test-contract
	@make test-integration

ci-teardown: ## CI: Teardown environment
	@make clean

##@ Information

ps: ## Show status of all Docker containers
	@docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

services: ## Show service URLs
	@echo "$(BLUE)Service URLs:$(NC)"
	@echo "  Kafka Connect API:  http://localhost:8083"
	@echo "  Schema Registry:    http://localhost:8081"
	@echo "  Vault UI:           http://localhost:8200/ui (token: dev-token)"
	@echo "  Prometheus:         http://localhost:9090"
	@echo "  Grafana:            http://localhost:3000 (admin/admin)"
	@echo "  Jaeger UI:          http://localhost:16686"

status: ps services ## Show complete environment status

help: ## Display this help message
	@echo "$(BLUE)ScyllaDB to Postgres CDC Pipeline - Make Commands$(NC)"
	@echo ""
	@awk 'BEGIN {FS = ":.*##"; printf "Usage: make $(YELLOW)<target>$(NC)\n"} \
		/^[a-zA-Z_-]+:.*?##/ { printf "  $(GREEN)%-25s$(NC) %s\n", $$1, $$2 } \
		/^##@/ { printf "\n$(BLUE)%s$(NC)\n", substr($$0, 5) }' $(MAKEFILE_LIST)
	@echo ""
	@echo "$(BLUE)Quick Start Examples:$(NC)"
	@echo "  make setup                    # Initial environment setup"
	@echo "  make deploy-connectors        # Deploy CDC connectors"
	@echo "  make test-replication         # Test CDC pipeline"
	@echo "  make cleanup-full             # Fresh-start cleanup"
	@echo "  make test-contract            # Run contract tests (with fresh-start)"
	@echo ""
	@echo "$(BLUE)Common Workflows:$(NC)"
	@echo "  1. First time setup:         make setup && make deploy-connectors"
	@echo "  2. Run tests:                make test"
	@echo "  3. Test CDC manually:        make test-replication"
	@echo "  4. Fresh-start for testing:  make cleanup-full && make test-contract"
	@echo "  5. Monitor pipeline:         make monitor && make check-replication"
	@echo ""
