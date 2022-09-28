# author : sofiane

.DEFAULT_GOAL := help

ENV?=development
REGION?=us-central1
REPOSITORY?=boom
GOOGLE_CLOUD_PROJECT?=
REPOSITORY_URL?=${REGION}-docker.pkg.dev
DOCKER_IMAGE?=microservice-template:v1

# ----------------------------------------------------------------------------------------------------------------------

all: ## All
	make tests build deploy

require_project: ## Check if $GOOGLE_CLOUD_PROJECT exist
	ifndef GOOGLE_CLOUD_PROJECT
	$(error GOOGLE_CLOUD_PROJECT not defined. Required for task)
	endif

venv: ## Setup venv
	py -m venv venv && source ./venv/bin/activate

require_venv: ## Setup requirements
	make venv && \
	pip install -r requirements.txt -q && \
	pip install -r requirements-test.txt -q;

build: ## Build the service into a container image
	make require_project && \
	gcloud builds submit --pack image=${REPOSITORY_URL}/${GOOGLE_CLOUD_PROJECT}/${REPOSITORY}/${DOCKER_IMAGE}

deploy: ## Deploy the container into Cloud Run (fully managed)
	make require_project && \
	gcloud run deploy microservice-template "--image ${REPOSITORY_URL}/${GOOGLE_CLOUD_PROJECT}/${REPOSITORY}/${DOCKER_IMAGE} --platform managed --region ${REGION}"

tests: ## Run unit tests
	make require_venv && \
	pytest test/test_app.py

system_tests: ## Run system tests
	make require_venv && \
	pytest test/test_system.py

start: ## Start the web service
	make require_venv && \
	py app.py

dev: ## Start the web service in a development environment, with fast reload
	make require_venv && \
	FLASK_ENV=${ENV} py app.py

# ----------------------------------------------------------------------------------------------------------------------

help: ## Print the help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
