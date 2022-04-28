#!/bin/sh
# Run Python tests on local environment.

export PREFIX=""
if [ -d 'venv' ] ; then
    export PREFIX="venv/bin/"
fi

set -ex

PYTHONDONTWRITEBYTECODE=1 ${PREFIX}pytest --cov-config=setup.cfg -n auto "$@"