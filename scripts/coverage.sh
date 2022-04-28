#!/bin/sh -e
# Run a coverage report after testing.

export PREFIX=""
if [ -d 'venv' ] ; then
    export PREFIX="venv/bin/"
fi

set -x

${PREFIX}coverage report --show-missing --skip-covered