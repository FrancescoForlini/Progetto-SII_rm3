#!/bin/bash

echo starting docker_container elasticasearch

docker compose up -d --build

sleep 5 

docker ps

echo docker_container elasticasearch started

#sleep 5 
# python search_engine_elasticsearch.py
