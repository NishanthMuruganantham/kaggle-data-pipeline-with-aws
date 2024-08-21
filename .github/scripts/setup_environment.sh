#!/bin/bash

bash create_venv.sh
source venv/bin/activate
if [ "$1" == 'pylint_check' ]; then
    pip install pylint
elif [ "$1" == 'pyright_check' ]; then
    pip install pyright
else
    pip install isort
fi
