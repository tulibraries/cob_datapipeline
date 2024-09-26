INTO-SUBMODULE := cd ./airflow-docker-dev-setup

up:
	git submodule init
	git submodule update
	$(INTO-SUBMODULE) && $(MAKE) up

stop:
	$(INTO-SUBMODULE) && $(MAKE) stop

reload:
	$(INTO-SUBMODULE) && $(MAKE) reload

down:
	$(INTO-SUBMODULE) && $(MAKE) down

tty-webserver:
	$(INTO-SUBMODULE) && $(MAKE) tty-root-webserver

tty-schedular:
	$(INTO-SUBMODULE) && $(MAKE) tty-schedular

tty-root-worker:
	$(INTO-SUBMODULE) && $(MAKE) tty-root-worker

tty-root-webserver:
	$(INTO-SUBMODULE) && $(MAKE) tty-root-webserver

tty-root-schedular:
	$(INTO-SUBMODULE) && $(MAKE) tty-root-schedular

ps:
	$(INTO-SUBMODULE) && docker compose -p infra ps


lint:
	pipenv run pylint cob_datapipeline -E

test:
	PYTHONPATH=. pipenv run pytest

compare-dependencies:
	.circleci/scripts/compare_dependencies.sh

build-requirements:
	.circleci/scripts/build-requirements.sh

rebuild-pipfile: build-requirements
	pipenv --rm
	rm Pipfile.lock
	pipenv install --dev --requirements requirements.txt
