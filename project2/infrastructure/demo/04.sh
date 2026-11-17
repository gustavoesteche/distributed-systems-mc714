#!/bin/bash

echo "DISPARANDO ADDS AO MESMO TEMPO..."

curl -s -X POST localhost:8001/cart/add \
  -d '{"product_id":"apple","quantity":2}' &
echo ""
curl -s -X POST localhost:8004/cart/add \
  -d '{"product_id":"apple","quantity":4}' &
echo ""
wait
sleep 3

for k in 1 2 3 4; do
  echo -n "node-$k: "
  curl -s localhost:800$k/cart
  echo
done

