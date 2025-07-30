#!/bin/bash
# scripts/run-redis.sh - UPDATED TO RUN VALKEY

# Stop and remove any existing redis or valkey benchmark containers
docker stop caching-benchmark-redis > /dev/null 2>&1
docker rm caching-benchmark-redis > /dev/null 2>&1
docker stop caching-benchmark-valkey > /dev/null 2>&1
docker rm caching-benchmark-valkey > /dev/null 2>&1

# Run a new Valkey container
# The name makes it easy to stop/start/remove.
docker run \
  --name caching-benchmark-valkey \
  -p 6379:6379 \
  -d \
  valkey/valkey:7.2-alpine

echo "Valkey container 'caching-benchmark-valkey' started." 