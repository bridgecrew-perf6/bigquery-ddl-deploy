#!/bin/sh -e
# Autoformat code in the local environment.

export PREFIX=""
if [ -d 'venv' ] ; then
    export PREFIX="venv/bin/"
fi
export SOURCE_FILES="src tests"

set -x

${PREFIX}autoflake --in-place --recursive $SOURCE_FILES
${PREFIX}isort --profile black --project=src $SOURCE_FILES
${PREFIX}black -l 120 $SOURCE_FILES