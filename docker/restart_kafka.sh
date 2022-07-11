#!/bin/bash

docker-compose -f docker-compose.yml stop
yes | docker-compose -f docker-compose.yml rm
docker-compose -f docker-compose.yml create
docker-compose -f docker-compose.yml start
