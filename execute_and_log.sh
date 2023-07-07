#!/bin/bash

RESULT_FOLDER="./${1}/${2}"

TO_RUN="servers"

if [[ $# -ge 3 ]]; then
    TO_RUN="$3"
fi

ulimit -n 100000

mkdir -p "${RESULT_FOLDER}" || exit 0

cp env "${RESULT_FOLDER}/env" && ./run "$TO_RUN" | tee "${RESULT_FOLDER}/log.txt"