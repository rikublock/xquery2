#!/usr/bin/env bash

# start container (remove everything on shutdown)
docker-compose -f docker-compose.redis.yml up && docker-compose -f docker-compose.redis.yml rm -fsv

