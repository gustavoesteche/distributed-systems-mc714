#!/bin/bash
echo "CONSULTANDO MÉTRICAS..."

curl -s localhost:8001/debug/metrics | python -c '
import json, sys
d = json.load(sys.stdin)
print({k: d[k] for k in ("clock", "quantities", "pending_ops")})
'
echo ""
