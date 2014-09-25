all:
	@echo "This is a convenience Makefile for cleaning and testing, no building here."

clean:
	find . -type d -name __pycache__ -print0 | xargs -0 -L1 rm -rf
	find . -name '*.pyc' -delete
	find . -name '*.pyo' -delete
	find . -name '*.pyd' -delete


check: test
test:
	#py.test --cov=ceilometer_publisher_vaultaire test_publisher.py --cov-report=html
	py.test test_publisher.py


# You should run this before switching between the two kinds of test above.
# This is necessary because you can get cross-contamination between them.
testclean:
	-rm -f .coverage
	-rm -rf htmlcov/
