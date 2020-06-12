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

