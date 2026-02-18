SHELL := env bash

.PHONY: build stress-test clean-test reliability-test clean-reliability

build:
	cargo build
	cargo build --example tcp_client
	cargo build --example tcp_server
	cargo build --example game_check
	cargo build --example mesh_check

long-stress-test: build
	TOPIC="test_topic_$$RANDOM"; \
	GAME_TEST_DURATION=530; \
	echo "Using TOPIC=$$TOPIC"; \
	# sudo -E TOPIC=$$TOPIC docker compose -f docker_test/compose-stress.yaml up --build --abort-on-container-exit --remove-orphans
	TOPIC=$$TOPIC docker compose -f docker_test/compose-stress.yaml up --build --abort-on-container-exit --remove-orphans

stress-test: build
	TOPIC="test_topic_$$RANDOM"; \
	echo "Using TOPIC=$$TOPIC"; \
	WIFI_SIM_DELAY=100 TOPIC=$$TOPIC docker compose -f docker_test/compose-stress.yaml up --build --abort-on-container-exit --remove-orphans; \
	echo ""; \
	./docker_test/check_logs.sh

clean-test:
	sudo docker compose -f docker_test/compose-stress.yaml down -v

reliability-test: build
	TOPIC="reliability_$$RANDOM"; \
	echo "Using TOPIC=$$TOPIC"; \
	sudo -E TOPIC=$$TOPIC RECONNECT_MAX_SEC=60 docker compose -f docker_test/compose-reliability.yaml up --build --abort-on-container-exit --remove-orphans; \
	echo ""; \
	chmod +x ./docker_test/check_reliability_logs.sh; \
	./docker_test/check_reliability_logs.sh

clean-reliability:
	sudo docker compose -f docker_test/compose-reliability.yaml down -v
