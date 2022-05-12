PYTHON=python
EXAMPLE=main.py

lint:
	flake8 --exclude="env/" --ignore=E701,E252 .
example:
	$(PYTHON) $(EXAMPLE)
