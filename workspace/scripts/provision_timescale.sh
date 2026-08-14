#!/bin/bash

cd "$(dirname "$0")"
sops exec-env ../sops.env 'envsubst < provision_timescale.sql | psql -h $TIMESCALE_DB_ENDPOINT -U postgres'
