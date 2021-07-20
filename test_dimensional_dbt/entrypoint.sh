#!/bin/bash

set -e 

if [ $1 == 'cleanup' ]; then
    dbt run-operation cleanup
    exit 0
fi

dbt clean && \
dbt deps && \
dbt seed && \
dbt run && \
dbt test
