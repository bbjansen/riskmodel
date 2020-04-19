
help:
	@echo "Tasks in \033[1;32mriskmodel\033[0m:"
	@cat Makefile

install:
	export PIPENV_VENV_IN_PROJECT=1
	pipenv install -d

lint:
	mypy src --ignore-missing-imports
	flake8 src --ignore=$(shell cat flake_ignored)

dev:
	python setup.py develop

mongo_install:
	@echo "Install mongodb locally"
	@sudo apt update
	@sudo apt install -y mongodb

mongo_start: mongo_install
	@echo "Start mongo db"
	@sudo service mongodb start
	@sudo service mongodb status
