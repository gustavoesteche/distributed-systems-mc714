#!/bin/bash
docker compose logs --no-color | grep concurrent_ops_detected | tail
