#!/bin/bash
echo "INICIANDO NODE 2..."

docker compose start node-2
sleep 5

for k in 1 2 3 4; do
    echo -n "node-$k: "
    curl -s localhost:800$k/cart
    echo ""
done

echo "HEALTH:"
curl localhost:8002/health
