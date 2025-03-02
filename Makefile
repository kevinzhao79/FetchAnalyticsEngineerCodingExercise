.PHONY: all init load queries test

all: init load queries test

init:
	python3 ./database/db_init.py

load:
	python3 ./database/db_loading.py

queries:
	python3 ./queries/q1.py
	python3 ./queries/q3.py
	python3 ./queries/q5.py

test:
	python3 ./database/db_testing.py