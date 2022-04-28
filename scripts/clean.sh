#!/bin/sh -e
# Clean up all the extra Python files we don't need.

export PREFIX=""
if [ -d 'venv' ] ; then
    export PREFIX="venv/bin/"
fi

set -x

rm -rf .mypy_cache
rm -rf .pytest_cache
rm -rf .coverage coverage.xml

${PREFIX}pyclean -v .