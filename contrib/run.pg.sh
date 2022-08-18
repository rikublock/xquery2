#!/usr/bin/env bash

# start container (remove everything on shutdown)
docker-compose -f docker-compose.pg.yml up && docker-compose -f docker-compose.pg.yml rm -fsv

