.PHONY: build run

build:
	docker build -t gitsync-container-volume:latest .

run:
	docker run -v $(shell pwd)/data:/data \
    -e REPOSITORY_URL="https://github.com/USACE/airflow-config" \
    -e REMOTE_BRANCH="develop" \
    -e PULL_INTERVAL="10s" \
	gitsync-container-volume:latest
