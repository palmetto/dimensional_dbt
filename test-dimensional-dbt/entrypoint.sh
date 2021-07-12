#!/bin/bash

set -e 

dbt clean && \
dbt deps && \
dbt seed && \
dbt run && \
dbt test
