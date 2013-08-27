.PHONY: test upload wc pep8 pyflakes clean
XARGS := xargs -0 $(shell test $$(uname) = Linux && echo -r)

test:
	trial txrdq

upload:
	python setup.py sdist upload

wc:
	find . -name '*.py' -print0 | $(XARGS) wc -l

pep8:
	find . -name '*.py' -print0 | $(XARGS) -n 1 pep8 --repeat

pyflakes:
	find . -name '*.py' -print0 | $(XARGS) pyflakes

clean:
	find . \( -name '*.pyc' -o -name '*~' \) -print0 | $(xargs) rm
	find . -type d -name _trial_temp -print0 | $(XARGS) rm -r
	rm -fr MANIFEST dist
