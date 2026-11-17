#!/bin/bash
docker compose logs node-1 | grep gossip_peer_unreachable | tail -1
