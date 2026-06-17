.PHONY: up down test

up:
	@docker compose -f docker-compose.test.yaml up -d
	@echo "Waiting for services..."
	@sleep 10

down:
	@docker compose -f docker-compose.test.yaml down -v

test: up
	@echo "Running migrations..."
	goose -dir ../migrations postgres "user=postgres password=qwerty dbname=test host=localhost port=5432 sslmode=disable" up

	@echo "Running tests..."
	cd .. && go test -v ./tests/...
	@$(MAKE) down