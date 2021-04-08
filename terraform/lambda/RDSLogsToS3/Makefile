testenv : virtualenv localstack

virtualenv:
	python3 -m venv env
	source env/bin/activate && python3 -m pip install -r requirements.txt

localstack:
	brew install localstack
	localstack start

test:
	source env/bin/activate && python3 -m unittest main_test.py
