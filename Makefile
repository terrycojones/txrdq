.PHONY: test upload wc pep8 pyflakes clean

test:
	trial txrdq

upload:
	python setup.py sdist upload

wc:
	find . -name '*.py' -print0 | xargs -r -0 wc -l

pep8:
	find . -name '*.py' -print0 | xargs -r -0 -n 1 pep8 --repeat

pyflakes:
	find . -name '*.py' -print0 | xargs -r -0 pyflakes

clean:
	find . \( -name '*.pyc' -o -name '*~' \) -print0 | xargs -r -0 rm 
	find . -type d -name _trial_temp -print0 | xargs -r -0 rm -r
	rm -fr MANIFEST dist
