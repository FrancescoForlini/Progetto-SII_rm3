#!/bin/bash

echo stopping docker_container elasticasearch

docker compose down -v

sleep 5 

docker ps

echo docker_container elasticasearch stopped