up:
	docker compose up --build -d

up-prod-example:
	docker compose -f deployments/compose/docker-compose.prod.example.yml config

down:
	docker compose down

logs:
	docker compose logs -f order-service

test:
	go test ./...

fmt:
	gofmt -w cmd internal

check:
	test -z "$$(gofmt -l cmd internal)"
	go vet ./...
	go test ./...
	docker compose config --quiet
