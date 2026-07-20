#!/bin/bash
set -e

echo "Building spark-master..."
docker compose build spark-master

docker compose build 

echo "Done."