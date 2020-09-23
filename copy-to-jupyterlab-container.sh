#!/usr/bin/env bash

container_id=$(docker ps | grep dapla-jupyterlab:dev | awk '{ print $1 }')
echo "found jupyterlab container: $container_id"
echo "copy build/lib/* $container_id:/opt/conda/lib/python3.8/site-packages/"
docker cp build/lib/* $container_id:/opt/conda/lib/python3.8/site-packages/
echo "restart kernel in jupyter to use copied file"
