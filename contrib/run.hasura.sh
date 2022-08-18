#!/usr/bin/env bash

# start container (remove everything on shutdown)
docker-compose -f docker-compose.hasura.yml up && docker-compose -f docker-compose.hasura.yml rm -fsv

