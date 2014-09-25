all:
	@echo "This is a convenience Makefile for cleaning and testing, no building here."

clean:
	find . -type d -name __pycache__ -print0 | xargs -0 -L1 rm -rf
	find . -name '*.pyc' -delete
	find . -name '*.pyo' -delete
	find . -name '*.pyd' -delete
	-rm -rf *.egg-info


check: test
test:
	py.test --cov=ceilometer_publisher_vaultaire test_ceilometer.py --cov-report=html

# So we can verify that test_pymarquise.py satisfactorily covers 100% of the
# pymarquise code, but how do we know that all of test_pymarquise.py is getting
# run? With more tests! This target produces a coverage report about
# test_pymarquise.py, dumping it in `htmlcov/`
test-coverage-of-main-in-testsuite:
	coverage run test_ceilometer.py ; coverage html test_ceilometer.py


# You should run this before switching between the two kinds of test above.
# This is necessary because you can get cross-contamination between them.
testclean:
	-rm -f .coverage
	-rm -rf htmlcov/
