#!/bin/bash

echo "INJETANDO FALHA..."

docker compose stop node-2
sleep 1

curl -s -X POST localhost:8001/cart/add \
  -d '{"product_id":"left","quantity":1}'
echo ""

curl -s -X POST localhost:8004/cart/add \
  -d '{"product_id":"right","quantity":1}'
echo ""

sleep 3
for k in 1 3 4; do
  echo -n "node-$k: "
  curl -s localhost:800$k/cart
  echo
done
