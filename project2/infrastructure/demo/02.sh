#!/bin/bash
echo "TESTANDO CENÁRIO NORMAL..."
curl -s -X POST localhost:8001/cart/add -d '{"product_id": "apple", "quantity": 3}'
echo ""

sleep 3

curl -s localhost:8001/cart
echo ""
