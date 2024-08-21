#!/bin/bash

VENV=${1:-venv}

python3.11 -m venv $VENV && \
    source $VENV/bin/activate && \
    python3.11 -m pip install pip==23.3.1 && \
    pip install -e '.[build]' && \
    pip install -r requirements-test.txt
