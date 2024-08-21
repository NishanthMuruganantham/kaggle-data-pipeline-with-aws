#!/bin/bash

source venv/bin/activate
if [ "$1" == 'pylint_check' ]; then
    venv/bin/python -m pylint $(git ls-files 'src/*.py')
elif [ "$1" == 'pyright_check' ]; then
    venv/bin/python -m pyright $(git ls-files 'src/*.py')
else
    venv/bin/python -m isort --check $(git ls-files 'src/*.py')
fi
