.PHONY: configure config prepare up up-dev up-minimal down down-v down-dev deploy deploy-airflow deploy-jobs init-buckets bootstrap fb-login verify e2e build sync

COMPOSE_DEV := docker compose -f docker-compose.yml
COMPOSE_MINIO_DAGS := docker compose $(shell bash scripts/compose_args.sh 2>/dev/null || echo "-f docker-compose.yml -f docker-compose.minio-dags.yml")

configure:
	bash scripts/configure.sh

config:
	bash scripts/generate_config.sh

prepare:
	bash scripts/prepare_workspace.sh

sync:
	bash scripts/sync_from_upstream.sh

build:
	$(COMPOSE_MINIO_DAGS) build
	$(COMPOSE_MINIO_DAGS) --profile job build fen-job

up:
	bash scripts/up.sh

# Dev: bind-mount ./dags directly, no MinIO sidecar / Dev: mount ./dags, không sidecar
up-dev:
	$(COMPOSE_DEV) up -d

up-minimal:
	docker compose -f docker-compose.minimal.yml up -d

down:
	$(COMPOSE_MINIO_DAGS) down

# Remove containers + named volumes (MinIO data, Postgres, Selenium profile, DAG cache)
# Xóa container + volume (data MinIO, Postgres, profile Selenium, cache DAG)
down-v:
	$(COMPOSE_MINIO_DAGS) down -v

down-dev:
	$(COMPOSE_DEV) down

init-buckets:
	$(COMPOSE_MINIO_DAGS) run --rm minio-init

deploy-airflow:
	bash scripts/deploy_airflow.sh

deploy-jobs:
	$(COMPOSE_MINIO_DAGS) --profile job build fen-job

deploy: init-buckets deploy-airflow deploy-jobs

bootstrap:
	bash scripts/bootstrap.sh

fb-login:
	docker compose --profile job run --rm \
		-e FEN_JOB=fen_bootstrap_login \
		-e FEN_GROUP_ID=$${FEN_GROUP_ID:-322453387859386} \
		fen-job

# Manual FB login via noVNC then save cookies /
# Login FB thủ công qua noVNC rồi lưu cookies
# If .env has FB_USERNAME + FB_PASSWORD → auto-fill; you only complete 2FA in noVNC /
# Có FB_USERNAME + FB_PASSWORD trong .env → tự điền; bạn chỉ cần 2FA trên noVNC
fb-login-manual:
	docker compose -f docker-compose.yml -f docker-compose.minio-dags.yml up -d selenium-chrome
	@echo ""
	@echo "============================================================"
	@echo "  noVNC (click):  http://localhost:7900"
	@echo "  VNC password:   secret"
	@echo "  Tip: set FB_USERNAME + FB_PASSWORD in .env to auto-fill;"
	@echo "       then only finish 2FA in the browser above."
	@echo "============================================================"
	@echo ""
	docker compose --profile job run --rm \
		-e FEN_JOB=fen_bootstrap_login \
		-e FEN_MANUAL_LOGIN=true \
		-e FEN_HEADLESS=false \
		-e FEN_GROUP_ID=$${FEN_GROUP_ID:-322453387859386} \
		fen-job

verify:
	bash scripts/verify_e2e.sh

e2e: deploy fb-login
	@echo "Trigger DAG fen_e2e_pipeline in Airflow UI http://localhost:8080"
