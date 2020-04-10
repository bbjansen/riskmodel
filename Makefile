
help:
	@echo "Tasks in \033[1;32mriskmodel\033[0m:"
	@cat Makefile

install:
	export PIPENV_VENV_IN_PROJECT=1
	pipenv install -d

lint:
	mypy src --ignore-missing-imports
	flake8 src --ignore=$(shell cat flake_ignored)
