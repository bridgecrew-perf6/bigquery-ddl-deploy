#!/bin/sh -e
# Run linting tests on local environment.

export PREFIX=""
if [ -d 'venv' ] ; then
    export PREFIX="venv/bin/"
fi
export SOURCE_FILES="src tests"
export PYTHONDONTWRITEBYTECODE=1
set -x

${PREFIX}black -l 120 --check --diff $SOURCE_FILES
${PREFIX}flake8 $SOURCE_FILES
${PREFIX}mypy $SOURCE_FILES
${PREFIX}isort --profile black --check --diff --project=src $SOURCE_FILES